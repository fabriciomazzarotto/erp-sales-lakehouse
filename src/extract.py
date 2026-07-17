"""
extract.py

Extração do SQL Server (schema erp.*) via JDBC.

Duas estratégias, conforme o escopo do projeto:
- full: recarrega a tabela inteira (tabelas pequenas/baixa mutação).
- incremental: lê apenas registros novos/alterados via watermark (coluna
  UpdatedAt), aplicando o filtro como subquery no próprio dbtable — o corte
  é feito pelo SQL Server via predicate pushdown, não trazendo a tabela
  inteira pela rede para depois filtrar em memória no Spark.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.config import get_jdbc_properties, get_jdbc_url


def read_full_table(spark: SparkSession, source_table: str) -> DataFrame:
    """Lê a tabela inteira via JDBC (carga full)."""
    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url())
        .option("dbtable", source_table)
        .options(**get_jdbc_properties())
        .load()
    )


def read_incremental_table(
    spark: SparkSession,
    source_table: str,
    watermark_column: str,
    last_watermark: Optional[str],
) -> DataFrame:
    """
    Lê apenas registros com watermark_column > last_watermark.

    last_watermark vem sempre do nosso próprio Bronze (nunca de input externo/
    usuário), então é seguro embutir no predicado sem parametrização adicional.
    Se last_watermark for None (tabela ainda não existe na Bronze), traz tudo
    — é a carga inicial ("backfill") do padrão incremental.

    O SQL Server guarda DATETIME2 com 7 dígitos decimais (100ns); o Spark só
    enxerga 6 (microssegundo). CAST(... AS DATETIME2(6)) ARREDONDA no SQL
    Server, enquanto o Spark TRUNCA ao ler — nos casos em que o 7º dígito é
    exatamente 5, um lado arredonda pra cima e o outro trunca pra baixo,
    fazendo o filtro ">" reincluir a mesma linha para sempre. Em vez de
    perseguir precisão de microssegundo (Frágil por natureza e sem nenhum
    ganho de negócio aqui), truncamos os dois lados para o segundo inteiro —
    prática padrão em pipelines com watermark, e o MERGE idempotente cobre
    com folga a pequena sobreposição que isso introduz.

    A comparação é feita como INTEIRO (segundos desde uma âncora), não
    reconstruindo um DATETIME truncado com DATEADD(DATEDIFF(...)) para depois
    comparar contra uma string — essa forma (usada numa versão anterior deste
    módulo) disparava "conversão de varchar para datetime fora do intervalo"
    de forma intermitente nesta instância de SQL Server (2025 RTM-GDR), por
    alguma inferência de tipo do otimizador ao reconciliar o literal string
    com o resultado do DATEADD dentro do WHERE — reproduzido e confirmado via
    sqlcmd; comparar dois inteiros evita esse caminho de código inteiro.
    """
    if last_watermark is None:
        predicate = "1=1"
    else:
        # Âncora fixa recente (não '0'/1900-01-01): DATEDIFF(SECOND, ...) retorna INT,
        # e a diferença em segundos entre 1900-01-01 e hoje já estoura o INT (>68 anos).
        anchor = "'2020-01-01'"
        predicate = (
            f"DATEDIFF(SECOND, {anchor}, {watermark_column}) > "
            f"DATEDIFF(SECOND, {anchor}, CAST('{last_watermark}' AS DATETIME2))"
        )
    subquery = f"(SELECT * FROM {source_table} WHERE {predicate}) AS incremental_extract"

    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url())
        .option("dbtable", subquery)
        .options(**get_jdbc_properties())
        .load()
    )
