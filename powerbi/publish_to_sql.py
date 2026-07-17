"""
publish_to_sql.py

Publica as 6 tabelas da camada Diamond num banco SQL Server dedicado
(ERP_Sales_BI), para o Power BI ler via conector nativo SQL Server (Import),
em vez de arquivos Parquet locais.

--------------------------------------------------------------------------
POR QUE ISSO EXISTE (o que veio antes e por que não bastava)
--------------------------------------------------------------------------
A primeira versão deste caminho local era `export_snapshot.py`: lê a Diamond
via Delta (respeitando `_delta_log`) e regrava como Parquet solto, porque o
Power BI Desktop não lê pastas Delta Lake nativamente. Isso funciona, mas o
relatório fica "olhando" um arquivo estático — mesmo com a automação diária
(Task Scheduler) mantendo o Parquet atualizado no disco, o Power BI nunca
sabe sozinho que o arquivo mudou; alguém precisa abrir o .pbix e clicar
"Atualizar" manualmente. Isso não reflete o motivo de a automação diária ter
sido construída (ingestão + reprocessamento sozinhos, sem intervenção).

A alternativa "de produção" documentada (`powerbi/README.md`, Caminho 2) é
Athena — mas isso exige migrar para AWS, decisão consciente de não fazer
agora.

Este script resolve o meio-termo: publica a Diamond num banco relacional
LOCAL (SQL Server, mesma instância já usada como origem do ERP, banco
separado `ERP_Sales_BI` — ver `sql/06_create_bi_database.sql`). Isso dá ao
Power BI um conector nativo de verdade (SQL Server), com um caminho real de
atualização automática *sem* depender de AWS: publicar o `.pbix` no Power BI
Service e agendar atualização via **Gateway de Dados Local** (gratuito,
roda nesta máquina) — algo que um arquivo Parquet local nunca conseguiria
(Power BI Service não tem como agendar refresh de um arquivo dentro do disco
de alguém sem gateway também, mas o conector de banco de dados é o caminho
padrão e documentado para isso, diferente de apontar para um path de arquivo
local). Ver a seção "Atualização automática" em `powerbi/README.md`.

`export_snapshot.py` continua no repositório como caminho alternativo (mais
simples, zero configuração de banco/gateway) — não foi removido, só deixou
de ser o passo padrão do pipeline agendado.

--------------------------------------------------------------------------
ISOLAMENTO DE PERMISSÕES
--------------------------------------------------------------------------
Este script escreve com o login `erp_bi_writer`, que só tem permissão dentro
de `ERP_Sales_BI` (db_owner desse banco específico) — nenhum acesso ao
schema `erp.*` de origem. É o inverso do login `erp_extractor` (só leitura na
origem, nenhum acesso à camada BI). Ver `sql/06_create_bi_database.sql`.

--------------------------------------------------------------------------
USO
--------------------------------------------------------------------------
    .venv/Scripts/python.exe powerbi/publish_to_sql.py

Sobrescreve as 6 tabelas em ERP_Sales_BI (mesmo padrão de overwrite/full
refresh já usado em todas as camadas do Lakehouse) a cada execução. Deve
rodar depois de `notebooks/04_create_diamond.py` — é o último passo de
`scripts/run_pipeline.ps1`.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_bi_jdbc_properties, get_bi_jdbc_url, get_table_path
from src.utils import get_logger, get_spark_session

logger = get_logger("publish_to_sql")

DIAMOND_TABLES = [
    "monthly_sales",
    "product_ranking",
    "customer_ranking",
    "salesperson_performance",
    "target_vs_actual",
    "commercial_kpis",
]


def publish_table(spark, table_name: str) -> dict:
    """
    Lê `diamond.<table_name>` pelo motor Delta (respeita _delta_log, portanto
    ignora arquivos obsoletos de overwrites anteriores) e grava em
    ERP_Sales_BI.dbo.<table_name> via JDBC, sobrescrevendo a tabela inteira
    (DROP + CREATE via mode "overwrite" — aceitável porque o login só tem
    acesso a este banco, que não guarda nada além do que este script
    regenera). Confere a contagem de linhas lendo a tabela de volta via JDBC.
    """
    source_path = get_table_path("diamond", table_name)
    df = spark.read.format("delta").load(source_path)
    source_count = df.count()

    df.write.format("jdbc").option("url", get_bi_jdbc_url()).options(
        **get_bi_jdbc_properties()
    ).option("dbtable", f"dbo.{table_name}").mode("overwrite").save()

    df_check = (
        spark.read.format("jdbc")
        .option("url", get_bi_jdbc_url())
        .options(**get_bi_jdbc_properties())
        .option("dbtable", f"dbo.{table_name}")
        .load()
    )
    check_count = df_check.count()

    status = "OK" if check_count == source_count else "MISMATCH"
    logger.info(
        f"[diamond.{table_name}] origem={source_count} linhas / ERP_Sales_BI.dbo.{table_name}={check_count} linhas -> {status}"
    )

    return {"table": table_name, "source_count": source_count, "published_count": check_count, "status": status}


def main():
    spark = get_spark_session("erp-publish-diamond-to-sql")
    results = []
    try:
        for table_name in DIAMOND_TABLES:
            results.append(publish_table(spark, table_name))
    finally:
        spark.stop()

    logger.info("== Resumo da publicação SQL (ERP_Sales_BI) ==")
    all_ok = True
    for r in results:
        if r["status"] != "OK":
            all_ok = False
        logger.info(f"  {r['table']}: {r['status']} ({r['published_count']} linhas)")
    logger.info(f"== Publicação SQL {'PASSOU' if all_ok else 'FALHOU'} ==")

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
