# Databricks notebook source
"""
02_transform_silver.py

Limpeza, padronização e validação de qualidade a partir da Bronze, gerando as
tabelas Silver. Registros que falham qualquer validação vão para uma tabela de
quarentena (<tabela>_quarantine, mesmo schema + rejection_reason) em vez de
serem descartados silenciosamente ou aceitos sem checagem.

Ordem de processamento respeita as dependências de FK: dimensões sem
dependência primeiro, depois cabeçalho de nota, depois itens (dependem do
cabeçalho), depois devoluções e metas.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_table_path
from src.quality import (
    check_foreign_key_exists,
    check_has_related_records,
    check_not_future_date,
    check_not_null,
    check_positive,
    deduplicate_by_key,
    split_valid_rejected,
)
from src.transformations import drop_bronze_technical_columns, standardize_column_names
from src.utils import get_logger, get_spark_session

logger = get_logger("transform_silver")

SOURCE_SYSTEM = "erp_sqlserver"

# COMMAND ----------


def read_bronze(spark, table):
    return spark.read.format("delta").load(get_table_path("bronze", table))


def finalize_and_write(spark, valid_df, rejected_df, silver_table, dedup_key=None):
    """Padroniza nomes, remove colunas técnicas da Bronze, grava Silver + quarentena."""
    if dedup_key is not None:
        valid_df = deduplicate_by_key(valid_df, dedup_key, "UpdatedAt")

    valid_df = drop_bronze_technical_columns(standardize_column_names(valid_df))
    rejected_df = drop_bronze_technical_columns(standardize_column_names(rejected_df))

    valid_path = get_table_path("silver", silver_table)
    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(valid_path)
    valid_count = valid_df.count()

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        quarantine_path = get_table_path("silver", f"{silver_table}_quarantine")
        rejected_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quarantine_path)

    logger.info(f"[silver.{silver_table}] válidos={valid_count} rejeitados={rejected_count}")
    return valid_df, rejected_count


# COMMAND ----------


def process_regions(spark):
    df = read_bronze(spark, "erp_regions")
    df = check_not_null(df, ["RegionID", "RegionCode"])
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "regions", dedup_key=["RegionID"])
    return valid_df


def process_payment_methods(spark):
    df = read_bronze(spark, "erp_payment_methods")
    df = check_not_null(df, ["PaymentMethodID", "PaymentMethodCode"])
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "payment_methods", dedup_key=["PaymentMethodID"])
    return valid_df


def process_customers(spark, regions_bronze):
    df = read_bronze(spark, "erp_customers")
    df = check_not_null(df, ["CustomerID", "CustomerCode", "CustomerName"])
    df = check_foreign_key_exists(df, "RegionID", regions_bronze, "RegionID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "customers", dedup_key=["CustomerID"])
    return valid_df


def process_products(spark):
    df = read_bronze(spark, "erp_products")
    df = check_not_null(df, ["ProductID", "ProductCode", "ProductName"])
    df = check_positive(df, "UnitPrice", allow_zero=False)
    df = check_positive(df, "UnitCost", allow_zero=True)
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "products", dedup_key=["ProductID"])
    return valid_df


def process_salespersons(spark, regions_bronze):
    df = read_bronze(spark, "erp_salespersons")
    df = check_not_null(df, ["SalespersonID", "SalespersonCode", "SalespersonName"])
    df = check_foreign_key_exists(df, "RegionID", regions_bronze, "RegionID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "salespersons", dedup_key=["SalespersonID"])
    return valid_df


def process_sales_invoice_header(spark, customers_bronze, salespersons_bronze, payment_methods_bronze, items_bronze_raw):
    df = read_bronze(spark, "erp_sales_invoice_header")
    df = check_not_null(df, ["InvoiceID", "InvoiceNumber", "CustomerID", "IssueDate"])
    df = check_not_future_date(df, "IssueDate")
    df = check_foreign_key_exists(df, "CustomerID", customers_bronze, "CustomerID")
    df = check_foreign_key_exists(df, "SalespersonID", salespersons_bronze, "SalespersonID")
    df = check_foreign_key_exists(df, "PaymentMethodID", payment_methods_bronze, "PaymentMethodID")
    # Toda nota deve ter ao menos 1 item — checa contra a Bronze crua de itens (existência
    # estrutural, independente de o item em si passar nas próprias regras de qualidade).
    df = check_has_related_records(df, "InvoiceID", items_bronze_raw, "InvoiceID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "sales_invoice_header", dedup_key=["InvoiceID"])
    return valid_df


def process_sales_invoice_items(spark, header_bronze, products_bronze):
    df = read_bronze(spark, "erp_sales_invoice_items")
    df = check_not_null(df, ["InvoiceItemID", "InvoiceID", "ProductID"])
    df = check_positive(df, "Quantity", allow_zero=False)
    df = check_positive(df, "UnitPrice", allow_zero=True)
    df = check_foreign_key_exists(df, "InvoiceID", header_bronze, "InvoiceID")
    df = check_foreign_key_exists(df, "ProductID", products_bronze, "ProductID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "sales_invoice_items", dedup_key=["InvoiceItemID"])
    return valid_df


def process_sales_returns(spark, header_bronze, items_bronze, products_bronze, customers_bronze):
    df = read_bronze(spark, "erp_sales_returns")
    df = check_not_null(df, ["ReturnID", "InvoiceID", "InvoiceItemID", "ProductID", "CustomerID"])
    df = check_positive(df, "Quantity", allow_zero=False)
    df = check_foreign_key_exists(df, "InvoiceID", header_bronze, "InvoiceID")
    df = check_foreign_key_exists(df, "InvoiceItemID", items_bronze, "InvoiceItemID")
    df = check_foreign_key_exists(df, "ProductID", products_bronze, "ProductID")
    df = check_foreign_key_exists(df, "CustomerID", customers_bronze, "CustomerID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "sales_returns", dedup_key=["ReturnID"])
    return valid_df


def process_sales_targets(spark, salespersons_bronze, regions_bronze):
    df = read_bronze(spark, "erp_sales_targets")
    df = check_not_null(df, ["TargetID", "SalespersonID", "RegionID"])
    df = check_positive(df, "TargetValue", allow_zero=False)
    df = check_foreign_key_exists(df, "SalespersonID", salespersons_bronze, "SalespersonID")
    df = check_foreign_key_exists(df, "RegionID", regions_bronze, "RegionID")
    valid_df, rejected_df = split_valid_rejected(df)
    valid_df, _ = finalize_and_write(spark, valid_df, rejected_df, "sales_targets", dedup_key=["TargetID"])
    return valid_df


# COMMAND ----------


def main():
    spark = get_spark_session("erp-transform-silver")
    try:
        # Bronze cru, usado só como referência de FK (não precisa estar limpo para checar existência)
        regions_bronze = read_bronze(spark, "erp_regions")
        customers_bronze = read_bronze(spark, "erp_customers")
        salespersons_bronze = read_bronze(spark, "erp_salespersons")
        payment_methods_bronze = read_bronze(spark, "erp_payment_methods")
        products_bronze = read_bronze(spark, "erp_products")
        header_bronze = read_bronze(spark, "erp_sales_invoice_header")
        items_bronze = read_bronze(spark, "erp_sales_invoice_items")

        process_regions(spark)
        process_payment_methods(spark)
        process_customers(spark, regions_bronze)
        process_products(spark)
        process_salespersons(spark, regions_bronze)
        process_sales_invoice_header(spark, customers_bronze, salespersons_bronze, payment_methods_bronze, items_bronze)
        process_sales_invoice_items(spark, header_bronze, products_bronze)
        process_sales_returns(spark, header_bronze, items_bronze, products_bronze, customers_bronze)
        process_sales_targets(spark, salespersons_bronze, regions_bronze)
    finally:
        spark.stop()

    logger.info("Transformação Silver finalizada")


if __name__ == "__main__":
    main()
