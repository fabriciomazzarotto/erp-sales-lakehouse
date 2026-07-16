"""
test_transformations.py

Testes das funções de padronização de schema em src/transformations.py:
- standardize_column_names: PascalCase (origem SQL Server) -> snake_case
- drop_bronze_technical_columns: remoção das colunas de controle da Bronze
- cast_columns: cast explícito de tipos, ignorando colunas fora do df
"""
from __future__ import annotations

from src.transformations import (
    BRONZE_TECHNICAL_COLUMNS,
    cast_columns,
    drop_bronze_technical_columns,
    standardize_column_names,
)


# ---------------------------------------------------------------------------
# standardize_column_names
# ---------------------------------------------------------------------------


def test_standardize_column_names_simple_pascal_case(spark):
    df = spark.createDataFrame([(1, "2024-01-01")], ["OrderID", "OrderDate"])
    result = standardize_column_names(df)
    assert result.columns == ["order_id", "order_date"]


def test_standardize_column_names_acronym_in_middle_not_split_per_letter(spark):
    """InvoiceID deve virar invoice_id, e não invoice_i_d - a sigla ID no
    final do nome tem que ser tratada como uma unidade, não letra a letra."""
    df = spark.createDataFrame([(1, "C1")], ["InvoiceID", "CustomerID"])
    result = standardize_column_names(df)
    assert result.columns == ["invoice_id", "customer_id"]


def test_standardize_column_names_already_snake_case_stays_unchanged(spark):
    df = spark.createDataFrame([(1, "A")], ["customer_id", "product_name"])
    result = standardize_column_names(df)
    assert result.columns == ["customer_id", "product_name"]


def test_standardize_column_names_preserves_row_data(spark):
    """Renomear coluna não pode embaralhar/perder os dados da linha."""
    df = spark.createDataFrame([(1, "Alice")], ["CustomerID", "CustomerName"])
    result = standardize_column_names(df)
    row = result.collect()[0]
    assert row["customer_id"] == 1
    assert row["customer_name"] == "Alice"


# ---------------------------------------------------------------------------
# drop_bronze_technical_columns
# ---------------------------------------------------------------------------


def test_drop_bronze_technical_columns_removes_all_present(spark):
    columns = ["id", "value"] + BRONZE_TECHNICAL_COLUMNS
    data = [(1, "A", "2024-01-01", "SQLSERVER", "erp.Notes", "batch-1", "full")]
    df = spark.createDataFrame(data, columns)
    result = drop_bronze_technical_columns(df)
    assert result.columns == ["id", "value"]
    for tech_col in BRONZE_TECHNICAL_COLUMNS:
        assert tech_col not in result.columns


def test_drop_bronze_technical_columns_noop_when_absent(spark):
    """Silver que já não tem colunas técnicas (ex.: segunda chamada) não pode
    quebrar por coluna ausente."""
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    result = drop_bronze_technical_columns(df)
    assert result.columns == ["id", "value"]
    assert result.count() == 1


def test_drop_bronze_technical_columns_removes_only_present_subset(spark):
    """Caso parcial: só algumas das 5 colunas técnicas presentes - remove só
    essas, preserva as demais colunas de negócio intactas."""
    df = spark.createDataFrame(
        [(1, "A", "batch-1")], ["id", "value", "batch_id"]
    )
    result = drop_bronze_technical_columns(df)
    assert result.columns == ["id", "value"]


# ---------------------------------------------------------------------------
# cast_columns
# ---------------------------------------------------------------------------


def test_cast_columns_applies_explicit_cast(spark):
    df = spark.createDataFrame([("10", "19.90")], ["quantity", "unit_price"])
    result = cast_columns(df, {"quantity": "int", "unit_price": "double"})
    dtypes = dict(result.dtypes)
    assert dtypes["quantity"] == "int"
    assert dtypes["unit_price"] == "double"
    row = result.collect()[0]
    assert row["quantity"] == 10
    assert row["unit_price"] == 19.90


def test_cast_columns_ignores_columns_not_in_dataframe(spark):
    """Chave do type_map que não existe no df (ex.: typo, coluna renomeada)
    não pode quebrar o pipeline - deve ser silenciosamente ignorada."""
    df = spark.createDataFrame([("10",)], ["quantity"])
    result = cast_columns(df, {"quantity": "int", "nonexistent_column": "double"})
    assert result.columns == ["quantity"]
    assert dict(result.dtypes)["quantity"] == "int"
