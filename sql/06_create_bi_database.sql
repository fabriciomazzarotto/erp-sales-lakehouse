/*
    ERP Sales Lakehouse
    06_create_bi_database.sql

    Cria o banco de dados "ERP_Sales_BI" — a camada de serving que recebe as
    tabelas agregadas da Diamond (via powerbi/publish_to_sql.py), para o
    Power BI consumir com o conector nativo SQL Server em vez de arquivos
    Parquet locais.

    Por que um banco separado (não um schema dentro de ERP_Sales):
    ERP_Sales é o banco transacional de ORIGEM — simula o ERP real. Escrever
    dado agregado ali misturaria "sistema fonte" com "sistema de consumo
    analítico", que numa migração futura para AWS seriam duas coisas
    fisicamente separadas (SQL Server on-prem vs. Athena/Glue). Separar já
    localmente deixa a fronteira entre os dois papéis explícita, e permite
    dar ao login de escrita (erp_bi_writer) permissão SOMENTE neste banco —
    sem qualquer acesso ao schema erp.* de origem (mesmo princípio de least
    privilege de sql/04_create_pipeline_login.sql, aplicado no sentido
    inverso: erp_extractor só LÊ a origem; erp_bi_writer só ESCREVE o
    destino, nenhum dos dois enxerga o papel do outro).

    erp_bi_writer recebe db_owner DENTRO de ERP_Sales_BI (não no server) —
    aceitável aqui porque esse banco só contém tabelas geradas pelo próprio
    pipeline (recriadas a cada rodada via overwrite), sem dado de origem para
    proteger. É diferente de dar db_owner em ERP_Sales, que exporia o dado
    transacional real.

        sqlcmd -S <server> -E -C -v LoginPassword="SuaSenhaForte" -i sql/06_create_bi_database.sql
*/

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ERP_Sales_BI')
BEGIN
    CREATE DATABASE ERP_Sales_BI;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'erp_bi_writer')
BEGIN
    CREATE LOGIN erp_bi_writer WITH PASSWORD = '$(LoginPassword)', CHECK_POLICY = ON;
END
ELSE
BEGIN
    ALTER LOGIN erp_bi_writer WITH PASSWORD = '$(LoginPassword)';
END
GO

USE ERP_Sales_BI;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'erp_bi_writer')
BEGIN
    CREATE USER erp_bi_writer FOR LOGIN erp_bi_writer;
END
GO

-- db_owner só dentro de ERP_Sales_BI: precisa criar/derrubar tabelas a cada
-- publicação (Spark JDBC write com mode "overwrite" faz DROP + CREATE TABLE),
-- e este banco não guarda nada além do que o pipeline regenera.
ALTER ROLE db_owner ADD MEMBER erp_bi_writer;
GO
