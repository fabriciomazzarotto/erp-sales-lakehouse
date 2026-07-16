"""
quality.py

Funções reutilizáveis de validação de qualidade de dados para a camada Silver.

Padrão: cada check_* função acumula motivos de rejeição numa coluna interna
(_rejection_reasons, array<string>), sem descartar nenhuma linha ainda. Ao final
da cadeia de checks, split_valid_rejected() separa o que passou de tudo (linhas
com zero motivos) do que foi rejeitado (linhas com >=1 motivo), preservando o(s)
motivo(s) para quarentena — nada é descartado silenciosamente.
"""
from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

_REASONS_COL = "_rejection_reasons"


def _ensure_reasons_col(df: DataFrame) -> DataFrame:
    if _REASONS_COL not in df.columns:
        df = df.withColumn(_REASONS_COL, F.array().cast("array<string>"))
    return df


def _add_reason(df: DataFrame, condition, reason: str) -> DataFrame:
    df = _ensure_reasons_col(df)
    return df.withColumn(
        _REASONS_COL,
        F.when(condition, F.array_union(F.col(_REASONS_COL), F.array(F.lit(reason))))
        .otherwise(F.col(_REASONS_COL)),
    )


def check_not_null(df: DataFrame, columns: List[str]) -> DataFrame:
    """Rejeita linhas com nulo em qualquer uma das colunas informadas (ex.: chaves principais)."""
    for column in columns:
        df = _add_reason(df, F.col(column).isNull(), f"{column}_is_null")
    return df


def check_positive(df: DataFrame, column: str, allow_zero: bool = False) -> DataFrame:
    """Rejeita valores negativos (allow_zero=True) ou zero/negativos (allow_zero=False)."""
    condition = F.col(column) < 0 if allow_zero else F.col(column) <= 0
    reason = f"{column}_negative" if allow_zero else f"{column}_zero_or_negative"
    return _add_reason(df, condition, reason)


def check_not_future_date(df: DataFrame, column: str) -> DataFrame:
    """Rejeita datas no futuro (ex.: data de emissão de nota fiscal)."""
    return _add_reason(df, F.col(column) > F.current_timestamp(), f"{column}_in_future")


def check_foreign_key_exists(df: DataFrame, fk_column: str, ref_df: DataFrame, ref_pk_column: str) -> DataFrame:
    """Rejeita linhas cuja chave estrangeira não existe na tabela de referência."""
    df = _ensure_reasons_col(df)
    ref_keys = ref_df.select(F.col(ref_pk_column).alias("_ref_pk")).distinct()
    joined = df.join(ref_keys, df[fk_column] == ref_keys["_ref_pk"], "left")
    joined = _add_reason(joined, F.col("_ref_pk").isNull(), f"{fk_column}_not_found")
    return joined.drop("_ref_pk")


def check_has_related_records(df: DataFrame, pk_column: str, related_df: DataFrame, related_fk_column: str) -> DataFrame:
    """Rejeita linhas sem nenhum registro relacionado (ex.: nota sem nenhum item)."""
    df = _ensure_reasons_col(df)
    related_keys = related_df.select(F.col(related_fk_column).alias("_related_fk")).distinct()
    joined = df.join(related_keys, df[pk_column] == related_keys["_related_fk"], "left")
    joined = _add_reason(joined, F.col("_related_fk").isNull(), f"no_related_records_via_{related_fk_column}")
    return joined.drop("_related_fk")


def deduplicate_by_key(df: DataFrame, key_columns: List[str], order_column: str) -> DataFrame:
    """Mantém só a linha mais recente (maior order_column) por chave — resolve duplicidade de origem."""
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_valid_rejected(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Separa o DataFrame acumulado de checks em (válidos, rejeitados).
    Rejeitados ganham uma coluna rejection_reason (string, motivos concatenados)
    para ir à quarentena; válidos perdem a coluna de controle interna.
    """
    df = _ensure_reasons_col(df)
    valid_df = df.filter(F.size(F.col(_REASONS_COL)) == 0).drop(_REASONS_COL)
    rejected_df = (
        df.filter(F.size(F.col(_REASONS_COL)) > 0)
        .withColumn("rejection_reason", F.concat_ws(", ", F.col(_REASONS_COL)))
        .drop(_REASONS_COL)
    )
    return valid_df, rejected_df
