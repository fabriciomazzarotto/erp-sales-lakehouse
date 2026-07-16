"""
utils.py

Funções utilitárias compartilhadas: criação da SparkSession (com os ajustes
necessários para rodar PySpark + Delta Lake localmente no Windows), geração
de batch_id e logging estruturado.
"""
from __future__ import annotations

import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from src.config import MSSQL_JDBC_DRIVER_PACKAGE, RUN_MODE


def get_spark_session(app_name: str = "erp-sales-lakehouse"):
    """
    Cria (ou recupera) a SparkSession configurada com Delta Lake e o driver
    JDBC do SQL Server.

    No Windows local, força SPARK_LOCAL_IP/SPARK_LOCAL_HOSTNAME e bind em
    loopback: sem isso o worker Python do Spark morre silenciosamente nesta
    máquina (Docker Desktop reescreve a resolução do hostname local para
    "host.docker.internal"). Também requer Python 3.11 no venv — PySpark
    quebra no Windows com Python 3.12+ (SPARK-53759). Ver requirements.txt.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if RUN_MODE == "local":
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if RUN_MODE == "local":
        builder = (
            builder.master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )

    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[MSSQL_JDBC_DRIVER_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_batch_id() -> str:
    """Identificador único do batch de ingestão (rastreabilidade ponta a ponta)."""
    return f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}"


def get_logger(name: str) -> logging.Logger:
    """Logger com formato consistente, usado em todos os módulos do pipeline."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
