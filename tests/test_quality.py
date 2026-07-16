"""
test_quality.py

Testes das funções de validação de qualidade em src/quality.py.

O padrão do módulo é: cada check_* acumula motivo(s) de rejeição na coluna
interna _rejection_reasons (array<string>) sem descartar nenhuma linha; só
split_valid_rejected() de fato separa válidos de rejeitados no final da
cadeia. Os testes abaixo cobrem, para cada check: caso feliz (nada
rejeitado), cada violação isolada, composição de múltiplos checks
acumulando todos os motivos na mesma linha, e os casos de borda citados no
pedido (dedup com empate, FK/relacionamento com tabela de referência vazia,
split_valid_rejected 100% válido e 100% rejeitado).

Comparações usam sorted()/dict-por-chave para não depender de ordem de linha
nem de ordem de elementos dentro do array de motivos (array_union não garante
ordem estável entre execuções).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.quality import (
    check_foreign_key_exists,
    check_has_related_records,
    check_not_future_date,
    check_not_null,
    check_positive,
    deduplicate_by_key,
    split_valid_rejected,
)


def _reasons(df, key_col="id"):
    """Coleta {chave: motivos ordenados} de um df com _rejection_reasons."""
    rows = df.select(key_col, "_rejection_reasons").collect()
    return {row[key_col]: sorted(row["_rejection_reasons"]) for row in rows}


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------


def test_check_not_null_happy_path_no_rejections(spark):
    df = spark.createDataFrame(
        [(1, "A", 10), (2, "B", 20)], ["id", "customer_id", "amount"]
    )
    result = check_not_null(df, ["customer_id", "amount"])
    assert _reasons(result) == {1: [], 2: []}


def test_check_not_null_flags_null_in_any_listed_column(spark):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("amount", IntegerType()),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(id=1, customer_id=None, amount=10),
            Row(id=2, customer_id="B", amount=None),
        ],
        schema,
    )
    result = check_not_null(df, ["customer_id", "amount"])
    assert _reasons(result) == {
        1: ["customer_id_is_null"],
        2: ["amount_is_null"],
    }


def test_check_not_null_accumulates_multiple_reasons_same_row(spark):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("amount", IntegerType()),
        ]
    )
    df = spark.createDataFrame([Row(id=1, customer_id=None, amount=None)], schema)
    result = check_not_null(df, ["customer_id", "amount"])
    assert _reasons(result) == {1: ["amount_is_null", "customer_id_is_null"]}


# ---------------------------------------------------------------------------
# check_positive
# ---------------------------------------------------------------------------


def test_check_positive_happy_path(spark):
    df = spark.createDataFrame([(1, 5), (2, 100)], ["id", "quantity"])
    result = check_positive(df, "quantity")
    assert _reasons(result) == {1: [], 2: []}


def test_check_positive_rejects_zero_and_negative_by_default(spark):
    """allow_zero=False (padrão): quantidade de item de nota, por exemplo -
    zero e negativo são ambos inválidos."""
    df = spark.createDataFrame([(1, 0), (2, -3), (3, 5)], ["id", "quantity"])
    result = check_positive(df, "quantity")
    assert _reasons(result) == {
        1: ["quantity_zero_or_negative"],
        2: ["quantity_zero_or_negative"],
        3: [],
    }


def test_check_positive_allow_zero_rejects_only_negative(spark):
    """allow_zero=True: valor unitário pode ser zero (ex.: brinde) mas não
    negativo."""
    df = spark.createDataFrame([(1, 0), (2, -3), (3, 5)], ["id", "unit_price"])
    result = check_positive(df, "unit_price", allow_zero=True)
    assert _reasons(result) == {
        1: [],
        2: ["unit_price_negative"],
        3: [],
    }


# ---------------------------------------------------------------------------
# check_not_future_date
# ---------------------------------------------------------------------------


def test_check_not_future_date(spark):
    past = datetime(2020, 1, 1)
    future = datetime.now() + timedelta(days=365)
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("issue_date", TimestampType()),
        ]
    )
    df = spark.createDataFrame(
        [Row(id=1, issue_date=past), Row(id=2, issue_date=future)], schema
    )
    result = check_not_future_date(df, "issue_date")
    assert _reasons(result) == {1: [], 2: ["issue_date_in_future"]}


# ---------------------------------------------------------------------------
# check_foreign_key_exists
# ---------------------------------------------------------------------------


def test_check_foreign_key_exists_happy_path(spark):
    df = spark.createDataFrame([(1, "C1"), (2, "C2")], ["id", "customer_id"])
    ref_df = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])
    result = check_foreign_key_exists(df, "customer_id", ref_df, "customer_id")
    assert _reasons(result) == {1: [], 2: []}


def test_check_foreign_key_exists_flags_missing_fk(spark):
    df = spark.createDataFrame([(1, "C1"), (2, "C_GHOST")], ["id", "customer_id"])
    ref_df = spark.createDataFrame([("C1",)], ["customer_id"])
    result = check_foreign_key_exists(df, "customer_id", ref_df, "customer_id")
    assert _reasons(result) == {1: [], 2: ["customer_id_not_found"]}


def test_check_foreign_key_exists_empty_ref_df_flags_everything(spark):
    """Tabela de referência vazia (ex.: dimensão de clientes ainda não
    carregada) não pode ser um "passa tudo" silencioso — todo mundo deve
    ser rejeitado por FK não encontrada."""
    df = spark.createDataFrame([(1, "C1"), (2, "C2")], ["id", "customer_id"])
    ref_df = spark.createDataFrame([], "customer_id string")
    result = check_foreign_key_exists(df, "customer_id", ref_df, "customer_id")
    assert _reasons(result) == {
        1: ["customer_id_not_found"],
        2: ["customer_id_not_found"],
    }


def test_check_foreign_key_exists_preserves_row_count_even_with_duplicate_refs(spark):
    """O check é implementado com join left contra ref_df.distinct(): mesmo
    que a tabela de referência tenha chave duplicada, o join não pode
    multiplicar linhas do df principal (bug clássico de join left sem
    distinct)."""
    df = spark.createDataFrame([(1, "C1"), (2, "C2")], ["id", "customer_id"])
    ref_df = spark.createDataFrame([("C1",), ("C1",)], ["customer_id"])
    result = check_foreign_key_exists(df, "customer_id", ref_df, "customer_id")
    assert result.count() == 2


# ---------------------------------------------------------------------------
# check_has_related_records
# ---------------------------------------------------------------------------


def test_check_has_related_records_happy_path(spark):
    notes = spark.createDataFrame([(1,), (2,)], ["note_id"])
    items = spark.createDataFrame([(1, "P1"), (2, "P2")], ["note_id", "product_id"])
    result = check_has_related_records(notes, "note_id", items, "note_id")
    assert _reasons(result, key_col="note_id") == {1: [], 2: []}


def test_check_has_related_records_flags_note_without_items(spark):
    notes = spark.createDataFrame([(1,), (2,)], ["note_id"])
    items = spark.createDataFrame([(1, "P1")], ["note_id", "product_id"])
    result = check_has_related_records(notes, "note_id", items, "note_id")
    assert _reasons(result, key_col="note_id") == {
        1: [],
        2: ["no_related_records_via_note_id"],
    }


def test_check_has_related_records_empty_related_df_flags_everything(spark):
    """Tabela de itens vazia -> toda nota deve ser rejeitada por não ter
    nenhum item relacionado."""
    notes = spark.createDataFrame([(1,), (2,)], ["note_id"])
    items = spark.createDataFrame([], "note_id int, product_id string")
    result = check_has_related_records(notes, "note_id", items, "note_id")
    assert _reasons(result, key_col="note_id") == {
        1: ["no_related_records_via_note_id"],
        2: ["no_related_records_via_note_id"],
    }


# ---------------------------------------------------------------------------
# composição de múltiplos checks na mesma linha
# ---------------------------------------------------------------------------


def test_multiple_checks_compose_and_accumulate_all_reasons(spark):
    """Encadear check_not_null + check_positive na mesma linha ruim deve
    acumular os DOIS motivos, não sobrescrever/perder o primeiro."""
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("quantity", IntegerType()),
        ]
    )
    df = spark.createDataFrame([Row(id=1, customer_id=None, quantity=-3)], schema)
    result = check_positive(check_not_null(df, ["customer_id"]), "quantity")
    assert _reasons(result) == {1: ["customer_id_is_null", "quantity_zero_or_negative"]}


# ---------------------------------------------------------------------------
# deduplicate_by_key
# ---------------------------------------------------------------------------


def test_deduplicate_by_key_keeps_highest_order_column(spark):
    df = spark.createDataFrame(
        [(1, "old", 1), (1, "new", 2), (2, "only", 1)],
        ["id", "value", "version"],
    )
    result = deduplicate_by_key(df, ["id"], "version")
    rows = {row["id"]: row["value"] for row in result.collect()}
    assert rows == {1: "new", 2: "only"}
    assert result.count() == 2


def test_deduplicate_by_key_tie_keeps_exactly_one_row(spark):
    """Em empate de order_column (dois registros com o mesmo timestamp de
    origem), não há garantia de QUAL sobrevive, mas deve sobrar EXATAMENTE
    uma linha - nunca zero (perderia o registro todo), nunca duas (bug
    clássico de dedup com row_number mal particionado)."""
    df = spark.createDataFrame(
        [(1, "a", 5), (1, "b", 5)],
        ["id", "value", "version"],
    )
    result = deduplicate_by_key(df, ["id"], "version")
    assert result.count() == 1
    assert result.collect()[0]["value"] in ("a", "b")


def test_deduplicate_by_key_composite_key(spark):
    """Chave composta (ex.: nota + item) - dedup deve respeitar todas as
    colunas da chave, não só a primeira."""
    df = spark.createDataFrame(
        [(1, 1, "old", 1), (1, 1, "new", 2), (1, 2, "other", 1)],
        ["note_id", "item_seq", "value", "version"],
    )
    result = deduplicate_by_key(df, ["note_id", "item_seq"], "version")
    rows = {(row["note_id"], row["item_seq"]): row["value"] for row in result.collect()}
    assert rows == {(1, 1): "new", (1, 2): "other"}


# ---------------------------------------------------------------------------
# split_valid_rejected
# ---------------------------------------------------------------------------


def test_split_valid_rejected_all_valid(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    checked = check_not_null(df, ["value"])
    valid_df, rejected_df = split_valid_rejected(checked)

    assert valid_df.count() == 2
    assert rejected_df.count() == 0
    assert "_rejection_reasons" not in valid_df.columns
    assert "rejection_reason" not in valid_df.columns
    assert "_rejection_reasons" not in rejected_df.columns


def test_split_valid_rejected_all_rejected(spark):
    schema = StructType(
        [StructField("id", IntegerType()), StructField("value", StringType())]
    )
    df = spark.createDataFrame([Row(id=1, value=None), Row(id=2, value=None)], schema)
    checked = check_not_null(df, ["value"])
    valid_df, rejected_df = split_valid_rejected(checked)

    assert valid_df.count() == 0
    assert rejected_df.count() == 2
    reasons = {row["id"]: row["rejection_reason"] for row in rejected_df.collect()}
    assert reasons == {1: "value_is_null", 2: "value_is_null"}


def test_split_valid_rejected_mixed_concatenates_multiple_reasons(spark):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("quantity", IntegerType()),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(id=1, customer_id="C1", quantity=5),
            Row(id=2, customer_id=None, quantity=-3),
        ],
        schema,
    )
    checked = check_positive(check_not_null(df, ["customer_id"]), "quantity")
    valid_df, rejected_df = split_valid_rejected(checked)

    assert [row["id"] for row in valid_df.collect()] == [1]
    rejected_row = rejected_df.collect()[0]
    assert rejected_row["id"] == 2
    # array_union não garante ordem estável entre motivos vindos de checks
    # diferentes - compara como conjunto, não como string exata.
    assert sorted(rejected_row["rejection_reason"].split(", ")) == [
        "customer_id_is_null",
        "quantity_zero_or_negative",
    ]


def test_split_valid_rejected_without_any_check_treats_all_as_valid(spark):
    """Se nenhum check_* foi chamado antes, a coluna interna nem existe.
    split_valid_rejected deve criá-la vazia via _ensure_reasons_col e tratar
    tudo como válido, sem estourar erro por coluna ausente."""
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    valid_df, rejected_df = split_valid_rejected(df)
    assert valid_df.count() == 2
    assert rejected_df.count() == 0
