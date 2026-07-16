/*
    ERP Sales Lakehouse
    04_create_pipeline_login.sql

    Cria um login SQL Server dedicado ao pipeline de extração (ERP Sales Lakehouse),
    com permissão apenas de leitura (SELECT) no schema erp — princípio de least
    privilege: o job de extração nunca deve usar uma conta pessoal/administrativa.

    Motivo de existir: a extração vai rodar fora desta máquina (Databricks, cloud),
    onde Windows Integrated Security não está disponível. Por isso o pipeline usa
    autenticação SQL Server (login + senha), não autenticação do Windows.

    A senha NUNCA é hardcoded aqui — é passada via variável sqlcmd:

        sqlcmd -S <server> -E -C -v LoginPassword="SuaSenhaForte" -i sql/04_create_pipeline_login.sql

    A senha real do ambiente de dev fica apenas no .env local (fora do Git).
*/

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'erp_extractor')
BEGIN
    CREATE LOGIN erp_extractor WITH PASSWORD = '$(LoginPassword)', CHECK_POLICY = ON;
END
ELSE
BEGIN
    ALTER LOGIN erp_extractor WITH PASSWORD = '$(LoginPassword)';
END
GO

USE ERP_Sales;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'erp_extractor')
BEGIN
    CREATE USER erp_extractor FOR LOGIN erp_extractor;
END
GO

-- Leitura apenas do schema erp (nunca write, nunca outros schemas/bancos)
GRANT SELECT ON SCHEMA :: erp TO erp_extractor;
GO
