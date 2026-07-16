# Databricks notebook source
"""
01_ingest_bronze.py

Extração do SQL Server (schema erp.*) via JDBC e escrita em Delta Lake na
camada Bronze, com colunas técnicas de controle (ingestion_timestamp,
source_system, source_table, batch_id, load_type).

Estratégia por tabela:
- full: Regions, PaymentMethods — pequenas, baixa mutação.
- incremental (watermark em UpdatedAt, MERGE por chave primária):
  Customers, Products, Salespersons, SalesInvoiceHeader, SalesInvoiceItems,
  SalesReturns, SalesTargets.

Roda tanto localmente (python notebooks/01_ingest_bronze.py, com RUN_MODE=local
no .env) quanto como notebook no Databricks — a lógica não muda, só o destino
das camadas (ver src/config.get_layer_path).
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_table_path
from src.extract import read_full_table, read_incremental_table
from src.load import add_technical_columns, get_last_watermark, write_bronze_full, write_bronze_incremental
from src.utils import generate_batch_id, get_logger, get_spark_session

logger = get_logger("ingest_bronze")

SOURCE_SYSTEM = "erp_sqlserver"

TABLES = [
    {"source_table": "erp.Regions", "bronze_table": "erp_regions", "load_type": "full"},
    {"source_table": "erp.PaymentMethods", "bronze_table": "erp_payment_methods", "load_type": "full"},
    {
        "source_table": "erp.Customers",
        "bronze_table": "erp_customers",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "CustomerID",
    },
    {
        "source_table": "erp.Products",
        "bronze_table": "erp_products",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "ProductID",
    },
    {
        "source_table": "erp.Salespersons",
        "bronze_table": "erp_salespersons",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "SalespersonID",
    },
    {
        "source_table": "erp.SalesInvoiceHeader",
        "bronze_table": "erp_sales_invoice_header",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "InvoiceID",
    },
    {
        "source_table": "erp.SalesInvoiceItems",
        "bronze_table": "erp_sales_invoice_items",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "InvoiceItemID",
    },
    {
        "source_table": "erp.SalesReturns",
        "bronze_table": "erp_sales_returns",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "ReturnID",
    },
    {
        "source_table": "erp.SalesTargets",
        "bronze_table": "erp_sales_targets",
        "load_type": "incremental",
        "watermark_column": "UpdatedAt",
        "primary_key": "TargetID",
    },
]

# COMMAND ----------


def ingest_table(spark, table_config: dict, batch_id: str) -> None:
    source_table = table_config["source_table"]
    bronze_table = table_config["bronze_table"]
    load_type = table_config["load_type"]
    table_path = get_table_path("bronze", bronze_table)

    if load_type == "full":
        df = read_full_table(spark, source_table)
        df = add_technical_columns(df, SOURCE_SYSTEM, source_table, batch_id, load_type)
        write_bronze_full(df, table_path)
        logger.info(f"[{bronze_table}] carga FULL concluída — {df.count()} linhas")
        return

    watermark_column = table_config["watermark_column"]
    primary_key = table_config["primary_key"]

    last_watermark = get_last_watermark(spark, table_path, watermark_column)
    df = read_incremental_table(spark, source_table, watermark_column, last_watermark)
    row_count = df.count()

    if row_count == 0:
        logger.info(f"[{bronze_table}] nenhum registro novo desde {last_watermark}")
        return

    df = add_technical_columns(df, SOURCE_SYSTEM, source_table, batch_id, load_type)
    write_bronze_incremental(spark, df, table_path, primary_key)
    logger.info(
        f"[{bronze_table}] carga INCREMENTAL concluída — {row_count} linhas novas/alteradas "
        f"(watermark anterior: {last_watermark})"
    )


# COMMAND ----------


def main():
    batch_id = generate_batch_id()
    logger.info(f"Iniciando batch {batch_id}")

    spark = get_spark_session("erp-ingest-bronze")
    try:
        for table_config in TABLES:
            ingest_table(spark, table_config, batch_id)
    finally:
        spark.stop()

    logger.info(f"Batch {batch_id} finalizado")


if __name__ == "__main__":
    main()
