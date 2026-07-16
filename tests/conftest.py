"""
conftest.py

Fixtures compartilhadas entre os testes de qualidade (test_quality.py) e de
transformação (test_transformations.py).

Usa src.utils.get_spark_session() (nunca cria SparkSession manualmente — ver
docstring da função para o motivo: Windows local precisa de
SPARK_LOCAL_IP/SPARK_LOCAL_HOSTNAME forçados e Python 3.11). Escopo "session":
criar uma SparkSession é caro (segundos), então uma única instância é
reaproveitada por todos os testes do processo pytest.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Garante que "src" seja importável mesmo que pytest não seja invocado com -m
# a partir da raiz do projeto (defensivo; não deveria ser necessário com
# `.venv/Scripts/python.exe -m pytest tests/` a partir da raiz).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src.utils import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="erp-sales-lakehouse-tests")
    yield session
    session.stop()
