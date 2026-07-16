/*
    ERP Sales Lakehouse
    02_insert_sample_data.sql

    Popula o ERP_Sales com dados sintéticos para simular ~18 meses de operação.

    Estratégia:
    - Tabelas mestras pequenas (Regions, PaymentMethods, Products, Customers,
      Salespersons) são inseridas com listas literais, priorizando nomes
      realistas (importante para prints de dashboard/portfólio).
    - Tabelas de volume (SalesInvoiceHeader, SalesInvoiceItems, SalesReturns,
      SalesTargets) são geradas de forma set-based, usando master.dbo.spt_values
      como tabela de números e CHECKSUM(NEWID(), <coluna_da_linha_externa>) para
      aleatoriedade por linha (técnica padrão em T-SQL para evitar loops linha a
      linha).
      IMPORTANTE: toda chamada a NEWID() usada para gerar aleatoriedade por linha
      inclui, de propósito, uma coluna da tabela externa (ex.: CHECKSUM(NEWID(), h.InvoiceID)).
      Sem essa correlação explícita, o otimizador do SQL Server pode identificar
      a subquery/CROSS APPLY como independente da linha externa e avaliá-la
      UMA ÚNICA VEZ para a query inteira, reaproveitando o mesmo resultado em
      todas as linhas — mesmo com NEWID() sendo não-determinístico. Isso já
      aconteceu numa versão anterior deste script (todas as notas caíram com o
      mesmo cliente/vendedor/produto). Incluir uma coluna real da linha externa
      no CHECKSUM/ORDER BY é o que impede esse cache indevido.
    - Ao final, um pequeno conjunto de registros é propositalmente "sujo"
      (quantidade negativa, valor unitário negativo, data futura) para dar
      trabalho real à camada Silver (ver docs/business_rules.md).
      Cenários de nulo em chave e duplicidade de ingestão não são simulados
      aqui, pois o schema de origem impõe integridade referencial como um
      ERP real faria — esses casos serão simulados na extração/Bronze.
*/

USE ERP_Sales;
GO

-- =========================================================
-- Regions
-- =========================================================
INSERT INTO erp.Regions (RegionCode, RegionName, State, Country) VALUES
('SP01', 'São Paulo - Capital',    'SP', 'Brasil'),
('SP02', 'São Paulo - Interior',   'SP', 'Brasil'),
('RJ01', 'Rio de Janeiro',         'RJ', 'Brasil'),
('MG01', 'Minas Gerais',           'MG', 'Brasil'),
('SUL01','Região Sul',             'RS', 'Brasil'),
('NE01', 'Nordeste',               'BA', 'Brasil'),
('CO01', 'Centro-Oeste',           'GO', 'Brasil'),
('NO01', 'Norte',                  'AM', 'Brasil');
GO

-- =========================================================
-- Payment methods
-- =========================================================
INSERT INTO erp.PaymentMethods (PaymentMethodCode, PaymentMethodName, PaymentType) VALUES
('PIX', 'Pix',                 'Pix'),
('CC',  'Cartão de Crédito',   'Card'),
('CD',  'Cartão de Débito',    'Card'),
('BOL', 'Boleto Bancário',     'Boleto'),
('DIN', 'Dinheiro',            'Cash');
GO

-- =========================================================
-- Products (24) — custo ~55-70% do preço de tabela
-- =========================================================
INSERT INTO erp.Products (ProductCode, ProductName, CategoryName, UnitOfMeasure, UnitCost, UnitPrice) VALUES
('PRD0001', 'Smartphone Galaxy A15',        'Eletrônicos',        'UN', 850.00,  1399.00),
('PRD0002', 'Fone de Ouvido Bluetooth',     'Eletrônicos',        'UN', 45.00,   99.90),
('PRD0003', 'Smart TV 50" 4K',              'Eletrônicos',        'UN', 1350.00, 2199.00),
('PRD0004', 'Caixa de Som Portátil',        'Eletrônicos',        'UN', 90.00,   189.90),
('PRD0005', 'Notebook 15" i5 8GB',          'Informática',        'UN', 2100.00, 3299.00),
('PRD0006', 'Mouse sem Fio',                'Informática',        'UN', 25.00,   59.90),
('PRD0007', 'Teclado Mecânico',             'Informática',        'UN', 110.00,  249.90),
('PRD0008', 'Monitor 24" Full HD',          'Informática',        'UN', 480.00,  799.00),
('PRD0009', 'SSD 480GB',                    'Informática',        'UN', 130.00,  229.90),
('PRD0010', 'Sofá Retrátil 3 Lugares',      'Casa e Decoração',   'UN', 980.00,  1799.00),
('PRD0011', 'Jogo de Panelas Antiaderente', 'Casa e Decoração',   'UN', 95.00,   189.90),
('PRD0012', 'Luminária de Mesa LED',        'Casa e Decoração',   'UN', 35.00,   79.90),
('PRD0013', 'Cortina Blackout',             'Casa e Decoração',   'UN', 55.00,   119.90),
('PRD0014', 'Bicicleta Aro 29',             'Esporte e Lazer',    'UN', 650.00,  1199.00),
('PRD0015', 'Tênis de Corrida',             'Esporte e Lazer',    'UN', 120.00,  259.90),
('PRD0016', 'Kit Halteres 10kg',            'Esporte e Lazer',    'UN', 140.00,  249.90),
('PRD0017', 'Bola de Futebol Oficial',      'Esporte e Lazer',    'UN', 60.00,   129.90),
('PRD0018', 'Café Torrado e Moído 1kg',     'Alimentos e Bebidas','UN', 14.00,   24.90),
('PRD0019', 'Azeite Extra Virgem 500ml',    'Alimentos e Bebidas','UN', 18.00,   34.90),
('PRD0020', 'Vinho Tinto Reserva',          'Alimentos e Bebidas','UN', 32.00,   69.90),
('PRD0021', 'Barra de Cereal (cx 12un)',    'Alimentos e Bebidas','UN', 16.00,   29.90),
('PRD0022', 'Camiseta Básica Algodão',      'Vestuário',          'UN', 22.00,   49.90),
('PRD0023', 'Jaqueta Corta-Vento',          'Vestuário',          'UN', 68.00,   149.90),
('PRD0024', 'Mochila Executiva',            'Vestuário',          'UN', 75.00,   169.90);
GO

-- =========================================================
-- Customers (20) — segmentos: Varejo, Atacado, E-commerce
-- =========================================================
INSERT INTO erp.Customers (CustomerCode, CustomerName, Document, Email, Phone, CustomerSegment, RegionID) VALUES
('CUST0001', 'Mercado Boa Compra Ltda',        '12345678000101', 'contato@boacompra.com.br',   '(11) 3000-1001', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP01')),
('CUST0002', 'Distribuidora Alfa Comércio',    '12345678000102', 'vendas@alfacomercio.com.br', '(11) 3000-1002', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP01')),
('CUST0003', 'Loja Estilo Urbano',             '12345678000103', 'contato@estilourbano.com.br','(19) 3000-1003', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP02')),
('CUST0004', 'Shop Fácil E-commerce',          '12345678000104', 'sac@shopfacil.com.br',       '(19) 3000-1004', 'E-commerce', (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP02')),
('CUST0005', 'Casa & Cia Móveis e Decoração',  '12345678000105', 'contato@casaecia.com.br',    '(21) 3000-1005', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='RJ01')),
('CUST0006', 'Atacadão Rio Comercial',         '12345678000106', 'vendas@atacadaorio.com.br',  '(21) 3000-1006', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='RJ01')),
('CUST0007', 'Supermercados Minas Gerais',     '12345678000107', 'compras@supermg.com.br',     '(31) 3000-1007', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='MG01')),
('CUST0008', 'BH Distribuidora',               '12345678000108', 'contato@bhdistrib.com.br',   '(31) 3000-1008', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='MG01')),
('CUST0009', 'Sul Modas Ltda',                 '12345678000109', 'contato@sulmodas.com.br',    '(51) 3000-1009', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='SUL01')),
('CUST0010', 'Gaúcha Distribuição',            '12345678000110', 'vendas@gauchadist.com.br',   '(51) 3000-1010', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='SUL01')),
('CUST0011', 'Nordeste Shop Online',           '12345678000111', 'sac@nordesteshop.com.br',    '(71) 3000-1011', 'E-commerce', (SELECT RegionID FROM erp.Regions WHERE RegionCode='NE01')),
('CUST0012', 'Bahia Atacadista',               '12345678000112', 'compras@bahiaatacado.com.br','(71) 3000-1012', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='NE01')),
('CUST0013', 'Central Goiás Comércio',         '12345678000113', 'contato@centralgo.com.br',   '(62) 3000-1013', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='CO01')),
('CUST0014', 'Cerrado Distribuidora',          '12345678000114', 'vendas@cerradodist.com.br',  '(62) 3000-1014', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='CO01')),
('CUST0015', 'Amazônia Comércio Digital',      '12345678000115', 'sac@amazoniadigital.com.br', '(92) 3000-1015', 'E-commerce', (SELECT RegionID FROM erp.Regions WHERE RegionCode='NO01')),
('CUST0016', 'Norte Distribuição Ltda',        '12345678000116', 'contato@nortedist.com.br',   '(92) 3000-1016', 'Atacado',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='NO01')),
('CUST0017', 'Loja Tech Express',              '12345678000117', 'contato@techexpress.com.br', '(11) 3000-1017', 'E-commerce', (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP01')),
('CUST0018', 'Mundo Esportivo Comércio',       '12345678000118', 'vendas@mundoesportivo.com.br','(19) 3000-1018', 'Varejo',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP02')),
('CUST0019', 'Empório Gourmet Rio',            '12345678000119', 'contato@emporiorio.com.br',  '(21) 3000-1019', 'Varejo',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='RJ01')),
('CUST0020', 'MG Casa Store',                  '12345678000120', 'sac@mgcasastore.com.br',     '(31) 3000-1020', 'E-commerce', (SELECT RegionID FROM erp.Regions WHERE RegionCode='MG01'));
GO

-- =========================================================
-- Salespersons (10)
-- =========================================================
INSERT INTO erp.Salespersons (SalespersonCode, SalespersonName, RegionID, HireDate) VALUES
('VEND001', 'Ana Beatriz Souza',    (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP01'), '2021-03-15'),
('VEND002', 'Carlos Eduardo Lima',  (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP01'), '2020-07-01'),
('VEND003', 'Fernanda Alves',       (SELECT RegionID FROM erp.Regions WHERE RegionCode='SP02'), '2022-01-10'),
('VEND004', 'Ricardo Nogueira',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='RJ01'), '2019-11-20'),
('VEND005', 'Juliana Ferreira',     (SELECT RegionID FROM erp.Regions WHERE RegionCode='RJ01'), '2023-02-01'),
('VEND006', 'Marcos Vinícius Rocha',(SELECT RegionID FROM erp.Regions WHERE RegionCode='MG01'), '2021-09-05'),
('VEND007', 'Patrícia Gomes',       (SELECT RegionID FROM erp.Regions WHERE RegionCode='SUL01'), '2020-05-18'),
('VEND008', 'Bruno Cardoso',        (SELECT RegionID FROM erp.Regions WHERE RegionCode='NE01'), '2022-06-30'),
('VEND009', 'Camila Ribeiro',       (SELECT RegionID FROM erp.Regions WHERE RegionCode='CO01'), '2021-12-01'),
('VEND010', 'Diego Martins',        (SELECT RegionID FROM erp.Regions WHERE RegionCode='NO01'), '2023-08-14');
GO

-- =========================================================
-- Sales invoices (header) — ~500 notas distribuídas nos últimos 18 meses
-- =========================================================
IF OBJECT_ID('tempdb..#InvoiceSeq') IS NOT NULL DROP TABLE #InvoiceSeq;

SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
INTO #InvoiceSeq
FROM master.dbo.spt_values;

INSERT INTO erp.SalesInvoiceHeader
    (InvoiceNumber, InvoiceSeries, CustomerID, SalespersonID, PaymentMethodID, IssueDate, InvoiceStatus, DiscountValue)
SELECT
    RIGHT('000000' + CAST(100000 + iv.rn AS VARCHAR(10)), 6)                                          AS InvoiceNumber,
    '1'                                                                                                 AS InvoiceSeries,
    (SELECT TOP 1 CustomerID FROM erp.Customers ORDER BY CHECKSUM(NEWID(), iv.rn))                      AS CustomerID,
    (SELECT TOP 1 SalespersonID FROM erp.Salespersons ORDER BY CHECKSUM(NEWID(), iv.rn))                AS SalespersonID,
    (SELECT TOP 1 PaymentMethodID FROM erp.PaymentMethods ORDER BY CHECKSUM(NEWID(), iv.rn))            AS PaymentMethodID,
    DATEADD(MINUTE, ABS(CHECKSUM(NEWID(), iv.rn)) % (60 * 24),
        DATEADD(DAY, -1 * (ABS(CHECKSUM(NEWID(), iv.rn)) % 540), SYSUTCDATETIME()))                     AS IssueDate,
    CASE WHEN ABS(CHECKSUM(NEWID(), iv.rn)) % 100 < 5 THEN 'Cancelada' ELSE 'Emitida' END                AS InvoiceStatus,
    CASE WHEN ABS(CHECKSUM(NEWID(), iv.rn)) % 100 < 20
         THEN CAST(ROUND((ABS(CHECKSUM(NEWID(), iv.rn)) % 5000) / 100.0, 2) AS DECIMAL(12,2))
         ELSE 0 END                                                                                      AS DiscountValue
FROM #InvoiceSeq AS iv;
GO

-- =========================================================
-- Sales invoice items — 1 a 4 itens por nota
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
    ORDER BY CHECKSUM(NEWID(), h.InvoiceID, s.ItemSeq, prod.ProductID)
) AS p(ProductID, Quantity, UnitPrice, DiscountValue);
GO

-- =========================================================
-- Sales returns — ~8% dos itens de notas "Emitida" geram devolução
-- =========================================================
INSERT INTO erp.SalesReturns
    (ReturnNumber, InvoiceID, InvoiceItemID, ProductID, CustomerID, ReturnDate, Quantity, UnitValue, ReturnReason)
SELECT
    'DEV' + RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY ii.InvoiceItemID) AS VARCHAR(10)), 6) AS ReturnNumber,
    ii.InvoiceID,
    ii.InvoiceItemID,
    ii.ProductID,
    h.CustomerID,
    DATEADD(DAY, 1 + ABS(CHECKSUM(NEWID(), ii.InvoiceItemID)) % 15, h.IssueDate)                     AS ReturnDate,
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
WHERE h.InvoiceStatus = 'Emitida'
  AND ABS(CHECKSUM(NEWID(), ii.InvoiceItemID)) % 100 < 8;
GO

-- =========================================================
-- Sales targets — meta mensal por vendedor nos últimos 12 meses
-- =========================================================
IF OBJECT_ID('tempdb..#Months') IS NOT NULL DROP TABLE #Months;

SELECT TOP (12)
    DATEADD(MONTH, -1 * (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1), CAST(GETDATE() AS DATE)) AS MonthDate
INTO #Months
FROM master.dbo.spt_values;

INSERT INTO erp.SalesTargets (SalespersonID, RegionID, TargetYear, TargetMonth, TargetValue)
SELECT
    sp.SalespersonID,
    sp.RegionID,
    YEAR(m.MonthDate),
    MONTH(m.MonthDate),
    CAST(15000 + (ABS(CHECKSUM(NEWID(), sp.SalespersonID, m.MonthDate)) % 10000) AS DECIMAL(14,2))
FROM erp.Salespersons sp
CROSS JOIN #Months m;
GO

-- =========================================================
-- Registros propositalmente inconsistentes (para exercitar a camada Silver)
-- =========================================================

-- Item com quantidade negativa (erro de digitação no ERP)
UPDATE TOP (1) erp.SalesInvoiceItems
SET Quantity = -3
WHERE InvoiceItemID = (SELECT MIN(InvoiceItemID) FROM erp.SalesInvoiceItems);

-- Item com valor unitário negativo (estorno mal lançado)
UPDATE TOP (1) erp.SalesInvoiceItems
SET UnitPrice = -50.00
WHERE InvoiceItemID = (SELECT MAX(InvoiceItemID) FROM erp.SalesInvoiceItems);

-- Nota fiscal com data de emissão futura (erro de sistema/fuso horário)
UPDATE TOP (1) erp.SalesInvoiceHeader
SET IssueDate = DATEADD(DAY, 10, SYSUTCDATETIME())
WHERE InvoiceID = (SELECT MAX(InvoiceID) FROM erp.SalesInvoiceHeader);
GO
