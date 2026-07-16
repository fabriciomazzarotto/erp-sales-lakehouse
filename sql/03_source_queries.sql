/*
    ERP Sales Lakehouse
    03_source_queries.sql

    Queries de apoio sobre o ERP de origem (SQL Server).

    Neste momento (Etapa 1), este arquivo contém apenas queries de VALIDAÇÃO,
    usadas para conferir se a carga de dados sintéticos (02_insert_sample_data.sql)
    ficou consistente antes de iniciar a extração.

    As queries de EXTRAÇÃO incremental (as que a extração Python/PySpark via JDBC
    vai efetivamente usar, com filtro por watermark em UpdatedAt) serão adicionadas
    aqui na etapa de ingestão (notebooks/01_ingest_bronze.py e src/extract.py),
    para manter a lógica de "o que ler da origem" centralizada e versionada.
*/

USE ERP_Sales;
GO

-- =========================================================
-- 1. Contagem de registros por tabela
-- =========================================================
SELECT 'Regions' AS TableName, COUNT(*) AS RecordCount FROM erp.Regions
UNION ALL SELECT 'PaymentMethods', COUNT(*) FROM erp.PaymentMethods
UNION ALL SELECT 'Products', COUNT(*) FROM erp.Products
UNION ALL SELECT 'Customers', COUNT(*) FROM erp.Customers
UNION ALL SELECT 'Salespersons', COUNT(*) FROM erp.Salespersons
UNION ALL SELECT 'SalesInvoiceHeader', COUNT(*) FROM erp.SalesInvoiceHeader
UNION ALL SELECT 'SalesInvoiceItems', COUNT(*) FROM erp.SalesInvoiceItems
UNION ALL SELECT 'SalesReturns', COUNT(*) FROM erp.SalesReturns
UNION ALL SELECT 'SalesTargets', COUNT(*) FROM erp.SalesTargets;
GO

-- =========================================================
-- 2. Distribuição de notas fiscais por mês (checagem de volume/período)
-- =========================================================
SELECT
    YEAR(IssueDate)  AS InvoiceYear,
    MONTH(IssueDate) AS InvoiceMonth,
    COUNT(*)         AS InvoiceCount,
    SUM(CASE WHEN InvoiceStatus = 'Cancelada' THEN 1 ELSE 0 END) AS CancelledCount
FROM erp.SalesInvoiceHeader
GROUP BY YEAR(IssueDate), MONTH(IssueDate)
ORDER BY InvoiceYear, InvoiceMonth;
GO

-- =========================================================
-- 3. Integridade cabeçalho x itens (toda nota deve ter ao menos 1 item)
-- =========================================================
SELECT h.InvoiceID, h.InvoiceNumber
FROM erp.SalesInvoiceHeader h
LEFT JOIN erp.SalesInvoiceItems i ON i.InvoiceID = h.InvoiceID
WHERE i.InvoiceItemID IS NULL;
GO

-- =========================================================
-- 4. Registros "sujos" injetados propositalmente (conferência)
--    Devem aparecer aqui: 1 quantidade negativa, 1 valor unitário negativo,
--    1 nota com data de emissão futura.
-- =========================================================
SELECT InvoiceItemID, InvoiceID, ProductID, Quantity, UnitPrice
FROM erp.SalesInvoiceItems
WHERE Quantity <= 0 OR UnitPrice < 0;

SELECT InvoiceID, InvoiceNumber, IssueDate
FROM erp.SalesInvoiceHeader
WHERE IssueDate > SYSUTCDATETIME();
GO

-- =========================================================
-- 5. Visão geral de devoluções por produto (checagem rápida de negócio)
-- =========================================================
SELECT
    p.ProductName,
    COUNT(*)            AS ReturnCount,
    SUM(r.Quantity)      AS TotalQuantityReturned,
    SUM(r.Quantity * r.UnitValue) AS TotalValueReturned
FROM erp.SalesReturns r
JOIN erp.Products p ON p.ProductID = r.ProductID
GROUP BY p.ProductName
ORDER BY TotalValueReturned DESC;
GO
