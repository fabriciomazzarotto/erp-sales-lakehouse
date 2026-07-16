"""
load.py

Escrita em Delta Lake para a camada Bronze: colunas técnicas de controle,
carga full (overwrite) e carga incremental (MERGE por chave primária —
idempotente, reprocessar o mesmo batch não duplica nem perde dados).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def add_technical_columns(
    df: DataFrame,
    source_system: str,
    source_table: str,
    batch_id: str,
    load_type: str,
) -> DataFrame:
    """Adiciona as colunas técnicas de controle da Bronze."""
    return (
        df.withColumn("ingestion_timestamp", F.lit(datetime.now(timezone.utc)))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_type", F.lit(load_type))
    )


def write_bronze_full(df: DataFrame, table_path: str) -> None:
    """Carga full: sobrescreve a tabela Bronze inteira a cada execução."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)


def write_bronze_incremental(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    primary_key: str,
) -> None:
    """
    Carga incremental: MERGE por chave primária (upsert). Se a tabela ainda
    não existe, faz a escrita inicial (backfill) direto.
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        df.write.format("delta").save(table_path)
        return

    delta_table = DeltaTable.forPath(spark, table_path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), f"target.{primary_key} = source.{primary_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def get_last_watermark(spark: SparkSession, table_path: str, watermark_column: str) -> Optional[str]:
    """
    Maior valor da coluna de watermark já presente na Bronze — de onde a
    próxima extração incremental deve continuar. None se a tabela ainda não
    existe (primeira carga).

    Truncado para o segundo inteiro (sem microssegundos) para casar com o
    truncamento aplicado no predicado de read_incremental_table — ver o
    comentário lá sobre a divergência de precisão DATETIME2(7) vs Spark(6).
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        return None

    row = spark.read.format("delta").load(table_path).agg(F.max(watermark_column)).collect()[0]
    max_value = row[0]
    return max_value.strftime("%Y-%m-%d %H:%M:%S") if max_value is not None else None
