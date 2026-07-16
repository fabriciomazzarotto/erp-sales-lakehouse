/*
    ERP Sales Lakehouse
    00_create_database.sql

    Cria o banco de dados que simula o ERP transacional de vendas.
    Este banco representa a ORIGEM do pipeline (SQL Server on-premises/simulado).
*/

IF DB_ID('ERP_Sales') IS NULL
BEGIN
    CREATE DATABASE ERP_Sales;
END
GO

USE ERP_Sales;
GO

-- Schema dedicado para deixar explícito que estas tabelas representam o ERP de origem,
-- evitando qualquer ambiguidade com os nomes usados depois no Lakehouse (bronze/silver/gold).
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'erp')
BEGIN
    EXEC('CREATE SCHEMA erp');
END
GO
