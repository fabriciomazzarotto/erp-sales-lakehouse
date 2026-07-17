/*
    ERP Sales Lakehouse
    05_simulate_daily_activity.sql

    Simula um dia de operação do ERP: novas notas fiscais (com itens),
    ocasionalmente uma devolução, e algumas atualizações em cadastros
    existentes (cliente/produto) — tudo com UpdatedAt "agora", para que a
    extração incremental da Bronze (watermark) tenha algo de verdade para
    pegar quando o pipeline agendado rodar depois deste script.

    Pensado para rodar via Task Scheduler todo dia, ANTES do pipeline
    (notebooks/01_ingest_bronze.py em diante) — ver scripts/run_daily_erp_simulation.ps1
    e docs/automation.md.

    Reaproveita a técnica de aleatoriedade correlacionada (CHECKSUM(NEWID(), coluna))
    já usada em 02_insert_sample_data.sql — sem a coluna correlacionada, o
    SQL Server pode avaliar o NEWID() uma única vez para a query inteira e
    repetir o mesmo resultado em todas as linhas (bug real já documentado
    nesse outro script).
*/

USE ERP_Sales;
GO

-- =========================================================
-- 1. Watermarks ANTES de inserir (para isolar só as linhas novas depois)
-- =========================================================
DECLARE @MaxInvoiceIDBefore INT = (SELECT ISNULL(MAX(InvoiceID), 0) FROM erp.SalesInvoiceHeader);
DECLARE @StartInvoiceNumber INT = (SELECT ISNULL(MAX(CAST(InvoiceNumber AS INT)), 100000) FROM erp.SalesInvoiceHeader) + 1;
DECLARE @InvoiceCount INT = 5 + ABS(CHECKSUM(NEWID())) % 11; -- 5 a 15 notas "hoje"

PRINT CONCAT('Simulando ', @InvoiceCount, ' novas notas fiscais...');

-- =========================================================
-- 2. Novas notas fiscais (últimas 24h, espalhadas)
-- =========================================================
IF OBJECT_ID('tempdb..#NewInvoiceSeq') IS NOT NULL DROP TABLE #NewInvoiceSeq;

SELECT TOP (@InvoiceCount) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
INTO #NewInvoiceSeq
FROM master.dbo.spt_values;

INSERT INTO erp.SalesInvoiceHeader
    (InvoiceNumber, InvoiceSeries, CustomerID, SalespersonID, PaymentMethodID, IssueDate, InvoiceStatus, DiscountValue)
SELECT
    RIGHT('000000' + CAST(@StartInvoiceNumber - 1 + iv.rn AS VARCHAR(10)), 6)                          AS InvoiceNumber,
    '1'                                                                                                 AS InvoiceSeries,
    (SELECT TOP 1 CustomerID FROM erp.Customers WHERE IsActive = 1 ORDER BY CHECKSUM(NEWID(), iv.rn))    AS CustomerID,
    (SELECT TOP 1 SalespersonID FROM erp.Salespersons WHERE IsActive = 1 ORDER BY CHECKSUM(NEWID(), iv.rn)) AS SalespersonID,
    (SELECT TOP 1 PaymentMethodID FROM erp.PaymentMethods ORDER BY CHECKSUM(NEWID(), iv.rn))             AS PaymentMethodID,
    DATEADD(MINUTE, -1 * (ABS(CHECKSUM(NEWID(), iv.rn)) % (24 * 60)), SYSUTCDATETIME())                  AS IssueDate,
    CASE WHEN ABS(CHECKSUM(NEWID(), iv.rn)) % 100 < 5 THEN 'Cancelada' ELSE 'Emitida' END                AS InvoiceStatus,
    CASE WHEN ABS(CHECKSUM(NEWID(), iv.rn)) % 100 < 20
         THEN CAST(ROUND((ABS(CHECKSUM(NEWID(), iv.rn)) % 5000) / 100.0, 2) AS DECIMAL(12,2))
         ELSE 0 END                                                                                       AS DiscountValue
FROM #NewInvoiceSeq AS iv;

-- =========================================================
-- 3. Itens para as notas recém-criadas (1 a 4 por nota)
-- =========================================================
INSERT INTO erp.SalesInvoiceItems (InvoiceID, ItemSequence, ProductID, Quantity, UnitPrice, DiscountValue)
SELECT
    h.InvoiceID,
    s.ItemSeq,
    p.ProductID,
    p.Quantity,
    p.UnitPrice,
    p.DiscountValue
FROM erp.SalesInvoiceHeader h
CROSS APPLY (
    SELECT TOP (1 + ABS(CHECKSUM(NEWID(), h.InvoiceID)) % 4) n
    FROM (VALUES (1), (2), (3), (4)) AS x(n)
) AS s(ItemSeq)
CROSS APPLY (
    SELECT TOP 1
        prod.ProductID,
        CAST(1 + ABS(CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq)) % 10 AS DECIMAL(12,2))                              AS Quantity,
        CAST(ROUND(prod.UnitPrice * (0.9 + (ABS(CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq, prod.ProductID)) % 2000) / 10000.0), 2) AS DECIMAL(12,2)) AS UnitPrice,
        CASE WHEN ABS(CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq)) % 100 < 10
             THEN CAST(ROUND((ABS(CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq)) % 2000) / 100.0, 2) AS DECIMAL(12,2))
             ELSE 0 END                                                                                              AS DiscountValue
    FROM erp.Products prod
    WHERE prod.IsActive = 1
    ORDER BY CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq, prod.ProductID)
) AS p(ProductID, Quantity, UnitPrice, DiscountValue)
WHERE h.InvoiceID > @MaxInvoiceIDBefore;

-- =========================================================
-- 4. Devolução ocasional (~15% de chance por dia) de um item recém-criado
-- =========================================================
INSERT INTO erp.SalesReturns
    (ReturnNumber, InvoiceID, InvoiceItemID, ProductID, CustomerID, ReturnDate, Quantity, UnitValue, ReturnReason)
SELECT TOP (CASE WHEN ABS(CHECKSUM(NEWID())) % 100 < 15 THEN 1 ELSE 0 END)
    'DEV' + RIGHT('000000' + CAST((SELECT ISNULL(MAX(ReturnID), 0) FROM erp.SalesReturns) + 1 AS VARCHAR(10)), 6) AS ReturnNumber,
    ii.InvoiceID,
    ii.InvoiceItemID,
    ii.ProductID,
    h.CustomerID,
    SYSUTCDATETIME()                                                                                 AS ReturnDate,
    CASE WHEN ii.Quantity > 1
         THEN CAST(1 + ABS(CHECKSUM(NEWID(), ii.InvoiceItemID)) % CAST(ii.Quantity AS INT) AS DECIMAL(12,2))
         ELSE ii.Quantity END                                                                        AS Quantity,
    ii.UnitPrice,
    (SELECT TOP 1 reason FROM (VALUES
        ('Produto com defeito'),
        ('Cliente desistiu da compra'),
        ('Produto trocado por outro modelo'),
        ('Divergência no pedido')) r(reason)
     ORDER BY CHECKSUM(NEWID(), ii.InvoiceItemID))                                                    AS ReturnReason
FROM erp.SalesInvoiceItems ii
JOIN erp.SalesInvoiceHeader h ON h.InvoiceID = ii.InvoiceID
WHERE h.InvoiceID > @MaxInvoiceIDBefore
  AND h.InvoiceStatus = 'Emitida'
ORDER BY CHECKSUM(NEWID(), ii.InvoiceItemID);

-- =========================================================
-- 5. Atualizações em cadastros existentes (exercita o MERGE de update,
--    não só insert, na extração incremental da Bronze)
--    Usa CTE + ORDER BY NEWID() para escolher linhas de fato aleatórias —
--    "UPDATE TOP (N)" sozinho, sem ORDER BY, tende a pegar sempre as
--    mesmas linhas (ordem física/índice), não uma amostra nova por dia.
-- =========================================================

-- ~2 clientes por dia "atualizam" telefone
;WITH RandomCustomers AS (
    SELECT TOP (2) CustomerID FROM erp.Customers WHERE IsActive = 1 ORDER BY NEWID()
)
UPDATE c
SET Phone = CONCAT('(11) 9', RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID(), c.CustomerID)) % 10000 AS VARCHAR(4)), 4), '-', RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID(), c.CustomerID)) % 10000 AS VARCHAR(4)), 4)),
    UpdatedAt = SYSUTCDATETIME()
FROM erp.Customers c
JOIN RandomCustomers rc ON rc.CustomerID = c.CustomerID;

-- ~1 produto por dia tem reajuste de preço (+/- 5%)
;WITH RandomProducts AS (
    SELECT TOP (1) ProductID FROM erp.Products WHERE IsActive = 1 ORDER BY NEWID()
)
UPDATE p
SET UnitPrice = CAST(ROUND(p.UnitPrice * (0.95 + (ABS(CHECKSUM(NEWID(), p.ProductID)) % 1000) / 10000.0), 2) AS DECIMAL(12,2)),
    UpdatedAt = SYSUTCDATETIME()
FROM erp.Products p
JOIN RandomProducts rp ON rp.ProductID = p.ProductID;

PRINT 'Simulacao de atividade diaria concluida.';
GO
