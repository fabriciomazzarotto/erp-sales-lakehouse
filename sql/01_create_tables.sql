/*
    ERP Sales Lakehouse
    01_create_tables.sql

    Modelagem transacional (OLTP) do ERP de vendas de origem.

    Decisões de design:
    - CreatedAt / UpdatedAt em todas as tabelas: UpdatedAt é a coluna de watermark
      usada pela extração incremental (Python/PySpark via JDBC). É a estratégia
      "controle por coluna de data de alteração" definida no escopo do projeto.
    - Chave técnica (*ID, INT IDENTITY) separada da chave de negócio (*Code).
      Isso reflete ERPs reais e cria, de propósito, o cenário onde a camada Gold
      precisa gerar suas próprias chaves substitutas (surrogate keys).
    - Propositalmente NÃO aplicamos CHECK CONSTRAINTS rígidas para regras de negócio
      (ex.: quantidade > 0, valor_unitario >= 0) neste nível. Isso é intencional:
      ERPs reais raramente têm 100% de integridade garantida na origem, e é
      exatamente essa "sujeira" que a camada Silver existe para tratar e validar.
      As regras de qualidade estão documentadas em docs/business_rules.md e serão
      aplicadas via PySpark na camada Silver.
*/

USE ERP_Sales;
GO

-- =========================================================
-- Dimensões de apoio (tabelas pequenas — candidatas a carga FULL)
-- =========================================================

CREATE TABLE erp.Regions (
    RegionID        INT IDENTITY(1,1) PRIMARY KEY,
    RegionCode      VARCHAR(10)     NOT NULL UNIQUE,
    RegionName      VARCHAR(100)    NOT NULL,
    State           VARCHAR(50)     NOT NULL,
    Country         VARCHAR(50)     NOT NULL DEFAULT 'Brasil',
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE erp.PaymentMethods (
    PaymentMethodID     INT IDENTITY(1,1) PRIMARY KEY,
    PaymentMethodCode   VARCHAR(10)     NOT NULL UNIQUE,
    PaymentMethodName   VARCHAR(50)     NOT NULL,
    PaymentType         VARCHAR(20)     NOT NULL, -- Cash, Card, Boleto, Pix, Transfer
    CreatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- =========================================================
-- Entidades principais (mestres)
-- =========================================================

CREATE TABLE erp.Customers (
    CustomerID      INT IDENTITY(1,1) PRIMARY KEY,
    CustomerCode    VARCHAR(20)     NOT NULL UNIQUE,
    CustomerName    VARCHAR(150)    NOT NULL,
    Document        VARCHAR(20)     NULL,       -- CNPJ/CPF
    Email           VARCHAR(150)    NULL,
    Phone           VARCHAR(30)     NULL,
    CustomerSegment VARCHAR(30)     NULL,        -- Varejo, Atacado, E-commerce
    RegionID        INT             NOT NULL,
    IsActive        BIT             NOT NULL DEFAULT 1,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Customers_Region FOREIGN KEY (RegionID) REFERENCES erp.Regions(RegionID)
);
GO

CREATE TABLE erp.Products (
    ProductID       INT IDENTITY(1,1) PRIMARY KEY,
    ProductCode     VARCHAR(20)     NOT NULL UNIQUE,
    ProductName     VARCHAR(150)    NOT NULL,
    CategoryName    VARCHAR(80)     NOT NULL,
    UnitOfMeasure   VARCHAR(10)     NOT NULL DEFAULT 'UN',
    UnitCost        DECIMAL(12,2)   NOT NULL,   -- custo, usado no cálculo de margem
    UnitPrice       DECIMAL(12,2)   NOT NULL,   -- preço de tabela
    IsActive        BIT             NOT NULL DEFAULT 1,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE erp.Salespersons (
    SalespersonID   INT IDENTITY(1,1) PRIMARY KEY,
    SalespersonCode VARCHAR(20)     NOT NULL UNIQUE,
    SalespersonName VARCHAR(150)    NOT NULL,
    RegionID        INT             NOT NULL,
    HireDate        DATE            NOT NULL,
    IsActive        BIT             NOT NULL DEFAULT 1,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Salespersons_Region FOREIGN KEY (RegionID) REFERENCES erp.Regions(RegionID)
);
GO

-- =========================================================
-- Notas fiscais (cabeçalho + itens) — tabelas grandes, carga incremental
-- =========================================================

CREATE TABLE erp.SalesInvoiceHeader (
    InvoiceID           INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNumber       VARCHAR(20)     NOT NULL,
    InvoiceSeries       VARCHAR(5)      NOT NULL DEFAULT '1',
    CustomerID          INT             NOT NULL,
    SalespersonID       INT             NOT NULL,
    PaymentMethodID     INT             NOT NULL,
    IssueDate           DATETIME2       NOT NULL,
    InvoiceStatus       VARCHAR(20)     NOT NULL DEFAULT 'Emitida', -- Emitida, Cancelada
    DiscountValue       DECIMAL(12,2)   NOT NULL DEFAULT 0,
    CreatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Invoice_Number_Series UNIQUE (InvoiceNumber, InvoiceSeries),
    CONSTRAINT FK_Invoice_Customer FOREIGN KEY (CustomerID) REFERENCES erp.Customers(CustomerID),
    CONSTRAINT FK_Invoice_Salesperson FOREIGN KEY (SalespersonID) REFERENCES erp.Salespersons(SalespersonID),
    CONSTRAINT FK_Invoice_PaymentMethod FOREIGN KEY (PaymentMethodID) REFERENCES erp.PaymentMethods(PaymentMethodID)
);
GO

CREATE TABLE erp.SalesInvoiceItems (
    InvoiceItemID   INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceID       INT             NOT NULL,
    ItemSequence    INT             NOT NULL,
    ProductID       INT             NOT NULL,
    Quantity        DECIMAL(12,2)   NOT NULL,
    UnitPrice       DECIMAL(12,2)   NOT NULL,
    DiscountValue   DECIMAL(12,2)   NOT NULL DEFAULT 0,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_InvoiceItem_Sequence UNIQUE (InvoiceID, ItemSequence),
    CONSTRAINT FK_InvoiceItem_Invoice FOREIGN KEY (InvoiceID) REFERENCES erp.SalesInvoiceHeader(InvoiceID),
    CONSTRAINT FK_InvoiceItem_Product FOREIGN KEY (ProductID) REFERENCES erp.Products(ProductID)
);
GO

-- =========================================================
-- Devoluções
-- =========================================================

CREATE TABLE erp.SalesReturns (
    ReturnID        INT IDENTITY(1,1) PRIMARY KEY,
    ReturnNumber    VARCHAR(20)     NOT NULL UNIQUE,
    InvoiceID       INT             NOT NULL,      -- nota fiscal original
    InvoiceItemID   INT             NOT NULL,      -- item original devolvido
    ProductID       INT             NOT NULL,
    CustomerID      INT             NOT NULL,
    ReturnDate      DATETIME2       NOT NULL,
    Quantity        DECIMAL(12,2)   NOT NULL,
    UnitValue        DECIMAL(12,2)  NOT NULL,
    ReturnReason    VARCHAR(200)    NULL,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Return_Invoice FOREIGN KEY (InvoiceID) REFERENCES erp.SalesInvoiceHeader(InvoiceID),
    CONSTRAINT FK_Return_InvoiceItem FOREIGN KEY (InvoiceItemID) REFERENCES erp.SalesInvoiceItems(InvoiceItemID),
    CONSTRAINT FK_Return_Product FOREIGN KEY (ProductID) REFERENCES erp.Products(ProductID),
    CONSTRAINT FK_Return_Customer FOREIGN KEY (CustomerID) REFERENCES erp.Customers(CustomerID)
);
GO

-- =========================================================
-- Metas comerciais
-- =========================================================

CREATE TABLE erp.SalesTargets (
    TargetID        INT IDENTITY(1,1) PRIMARY KEY,
    SalespersonID   INT             NOT NULL,
    RegionID        INT             NOT NULL,
    TargetYear      INT             NOT NULL,
    TargetMonth     INT             NOT NULL,      -- 1 a 12
    TargetValue     DECIMAL(14,2)   NOT NULL,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Target_Salesperson_Period UNIQUE (SalespersonID, TargetYear, TargetMonth),
    CONSTRAINT FK_Target_Salesperson FOREIGN KEY (SalespersonID) REFERENCES erp.Salespersons(SalespersonID),
    CONSTRAINT FK_Target_Region FOREIGN KEY (RegionID) REFERENCES erp.Regions(RegionID)
);
GO

-- =========================================================
-- Índices de apoio à extração incremental (watermark em UpdatedAt)
-- =========================================================

CREATE INDEX IX_Customers_UpdatedAt ON erp.Customers(UpdatedAt);
CREATE INDEX IX_Products_UpdatedAt ON erp.Products(UpdatedAt);
CREATE INDEX IX_Salespersons_UpdatedAt ON erp.Salespersons(UpdatedAt);
CREATE INDEX IX_InvoiceHeader_UpdatedAt ON erp.SalesInvoiceHeader(UpdatedAt);
CREATE INDEX IX_InvoiceItems_UpdatedAt ON erp.SalesInvoiceItems(UpdatedAt);
CREATE INDEX IX_Returns_UpdatedAt ON erp.SalesReturns(UpdatedAt);
CREATE INDEX IX_Targets_UpdatedAt ON erp.SalesTargets(UpdatedAt);
GO
