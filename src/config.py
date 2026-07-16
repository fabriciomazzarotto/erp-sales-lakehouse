"""
config.py

Carregamento centralizado de configurações (SQL Server, AWS, Databricks, Glue/Athena)
a partir de variáveis de ambiente (.env), para evitar credenciais/configuração
espalhadas pelo código.

RUN_MODE controla se o pipeline lê/grava em disco local (dev) ou em S3 (produção),
trocando apenas o "endereço" das camadas — a lógica de transformação não muda.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

RUN_MODE = os.getenv("RUN_MODE", "local")  # "local" ou "aws"

# --- SQL Server (origem) ---
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "ERP_Sales")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")

MSSQL_JDBC_DRIVER_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
MSSQL_JDBC_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def get_jdbc_url() -> str:
    """URL JDBC do SQL Server de origem (schema erp.*)."""
    return (
        f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT}"
        f";databaseName={SQLSERVER_DATABASE}"
        ";encrypt=true;trustServerCertificate=true"
    )


def get_jdbc_properties() -> dict:
    """Propriedades de conexão JDBC (usuário/senha/driver)."""
    return {
        "user": SQLSERVER_USER,
        "password": SQLSERVER_PASSWORD,
        "driver": MSSQL_JDBC_DRIVER_CLASS,
    }


# --- Camadas do Lakehouse (Bronze/Silver/Gold/Diamond) ---
LOCAL_LAKEHOUSE_ROOT = os.getenv("LOCAL_LAKEHOUSE_ROOT", "./data/lakehouse")

_AWS_LAYER_BUCKETS = {
    "bronze": os.getenv("AWS_S3_BUCKET_BRONZE"),
    "silver": os.getenv("AWS_S3_BUCKET_SILVER"),
    "gold": os.getenv("AWS_S3_BUCKET_GOLD"),
    "diamond": os.getenv("AWS_S3_BUCKET_DIAMOND"),
}


def get_layer_path(layer: str) -> str:
    """
    Retorna o path raiz de uma camada (bronze/silver/gold/diamond), local ou S3,
    conforme RUN_MODE. É o único lugar do código que sabe a diferença entre
    rodar localmente e rodar na AWS.
    """
    if layer not in ("bronze", "silver", "gold", "diamond"):
        raise ValueError(f"Camada desconhecida: '{layer}'")

    if RUN_MODE == "aws":
        bucket = _AWS_LAYER_BUCKETS.get(layer)
        if not bucket:
            raise ValueError(f"Bucket S3 não configurado para a camada '{layer}' (.env)")
        return f"s3a://{bucket}"

    return str(Path(LOCAL_LAKEHOUSE_ROOT) / layer)


def get_table_path(layer: str, table_name: str) -> str:
    """Path completo de uma tabela dentro de uma camada (ex.: bronze/erp_customers)."""
    return str(Path(get_layer_path(layer)) / table_name)
