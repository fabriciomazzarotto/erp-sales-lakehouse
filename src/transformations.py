"""
transformations.py

Funções reutilizáveis de padronização de schema para a camada Silver:
nomes de coluna em snake_case e remoção das colunas técnicas da Bronze
(que não fazem sentido expostas na Silver).
"""
from __future__ import annotations

import re
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_TECHNICAL_COLUMNS = [
    "ingestion_timestamp",
    "source_system",
    "source_table",
    "batch_id",
    "load_type",
]


def _to_snake_case(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Converte nomes de coluna PascalCase (origem SQL Server) para snake_case."""
    for column in df.columns:
        new_name = _to_snake_case(column)
        if new_name != column:
            df = df.withColumnRenamed(column, new_name)
    return df


def drop_bronze_technical_columns(df: DataFrame) -> DataFrame:
    """Remove as colunas de controle da Bronze — não fazem sentido na Silver."""
    existing = [c for c in BRONZE_TECHNICAL_COLUMNS if c in df.columns]
    return df.drop(*existing) if existing else df


def cast_columns(df: DataFrame, type_map: Dict[str, str]) -> DataFrame:
    """Aplica cast explícito de tipos onde necessário (coluna -> tipo Spark)."""
    for column, target_type in type_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(target_type))
    return df
