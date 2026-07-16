"""
export_snapshot.py

Gera um snapshot "achatado" (1 arquivo Parquet por tabela, sem `_delta_log`,
sem arquivos obsoletos de overwrites anteriores) de cada tabela da camada
Diamond, em `powerbi/export/<tabela>.parquet`.

--------------------------------------------------------------------------
POR QUE ESTE SCRIPT EXISTE (o problema que ele resolve)
--------------------------------------------------------------------------
Power BI Desktop standalone NÃO tem conector nativo para "pasta Delta Lake
em disco" (isso existe no Fabric/Synapse Lakehouse, não no Power BI Desktop
puro). A tentação óbvia seria apontar o conector de Parquet/pasta do Power BI
direto para dentro de `./data/lakehouse/diamond/<tabela>/` — isso é ERRADO e
silenciosamente incorreto, não apenas "não recomendado":

`write_diamond()` (ver notebooks/04_create_diamond.py) grava cada tabela com
`.mode("overwrite")`. O Delta Lake, ao fazer overwrite, MARCA os arquivos
parquet antigos como removidos no log de transação (`_delta_log/*.json`) mas
NÃO os apaga fisicamente do disco — isso só acontece rodando `VACUUM`
(nenhuma rotina deste projeto chama VACUUM). Um leitor que entende o log
(Spark+Delta, Athena com `delta_target`) sabe ignorar os arquivos removidos.
Um leitor "burro" de parquet (Power BI apontando pasta) lê TODOS os arquivos
.parquet fisicamente presentes, ignorando o log — ou seja, se a tabela já foi
sobrescrita mais de uma vez, ele soma dados duplicados/obsoletos.

Prova empírica (rodada em 2026-07-15, ver notas do PR/sessão): a pasta local
`data/lakehouse/diamond/commercial_kpis/` tinha 3 arquivos `part-*.parquet`
remanescentes de execuções anteriores do notebook 04. Resultado:
    spark.read.format("delta").load(path).count()  -> 19 linhas (CORRETO)
    spark.read.parquet(path).count()                -> 57 linhas (19 x 3, ERRADO)
Isso não é um risco teórico — é o que acontece na prática após a segunda
rodada do pipeline.

--------------------------------------------------------------------------
SOLUÇÃO: snapshot flat, regenerado a cada rodada
--------------------------------------------------------------------------
Este script lê cada tabela Diamond pelo motor Delta (respeitando o log de
transação, portanto sempre a versão correta e atual) e regrava como UM ÚNICO
arquivo Parquet solto por tabela, sem `_delta_log` e sem histórico de
versões — o Power BI aponta para esse arquivo achatado, nunca para a pasta
Delta original. Rodar este script sempre que `notebooks/04_create_diamond.py`
rodar de novo (mesmo princípio de refresh das outras camadas: sem
incremental, full snapshot a cada execução).

--------------------------------------------------------------------------
POR QUE PARQUET (não CSV)
--------------------------------------------------------------------------
Optou-se por Parquet, não CSV, como formato do snapshot:

1. Tipagem explícita e sem ambiguidade: Parquet carrega o schema (int64,
   double, string, date) no próprio arquivo. Power BI lê os tipos direto,
   sem inferência de texto. CSV depende de inferência de tipo do Power Query,
   que é sensível à configuração regional do Windows/Power BI — em uma
   máquina com localidade pt-BR (separador decimal ",", separador de milhar
   "."), um CSV gerado com decimal "." (padrão internacional) pode ser
   importado errado (coluna numérica virando texto, ou "1234.56" sendo lido
   como "123456") se o "Origem do Arquivo"/localidade não for ajustado à mão
   em cada tabela. Isso é uma classe inteira de erro silencioso que o Parquet
   elimina — não há "separador decimal" em um arquivo binário tipado.
2. Conector nativo: Power BI Desktop tem "Obter Dados > Arquivo > Parquet"
   desde 09/2022 (GA), portanto disponível em qualquer instalação atual do
   Power BI Desktop.
3. Arquivo único, pequeno (a Diamond tem no máximo alguns milhares de linhas
   nas tabelas de ranking) — nenhuma vantagem de compressão/particionamento
   é relevante aqui, mas também não há custo em usar Parquet a este volume.

CSV não foi descartado como opção permanente — se o usuário final preferir
abrir os arquivos no Excel para conferência manual, é trivial abrir um
.parquet pequeno com o Power BI/Excel Power Query também, ou pedir para este
script gerar CSV como formato adicional no futuro. Por ora, um único formato
(Parquet) evita manter dois caminhos de exportação.

--------------------------------------------------------------------------
USO
--------------------------------------------------------------------------
    .venv/Scripts/python.exe powerbi/export_snapshot.py

Gera/sobrescreve `powerbi/export/<tabela>.parquet` para as 6 tabelas Diamond.
`powerbi/export/` é gerado (não versionado — ver .gitignore), regenerável a
qualquer momento rodando este script após a Diamond ser recalculada.
"""
import os
import shutil
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_table_path
from src.utils import get_logger, get_spark_session

logger = get_logger("export_snapshot")

DIAMOND_TABLES = [
    "monthly_sales",
    "product_ranking",
    "customer_ranking",
    "salesperson_performance",
    "target_vs_actual",
    "commercial_kpis",
]

EXPORT_DIR = Path(__file__).resolve().parent / "export"


def export_table(spark, table_name: str) -> dict:
    """
    Lê `diamond.<table_name>` pelo motor Delta (respeita _delta_log, portanto
    ignora arquivos obsoletos de overwrites anteriores), grava um snapshot
    Parquet de arquivo único em powerbi/export/<table_name>.parquet, e
    confere (mesma sessão Spark) que o snapshot lido de volta bate em
    contagem de linhas e colunas com a origem.
    """
    source_path = get_table_path("diamond", table_name)
    df = spark.read.format("delta").load(source_path)
    source_count = df.count()
    source_columns = df.columns

    tmp_dir = EXPORT_DIR / f"_tmp_{table_name}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    # coalesce(1) força um único arquivo de saída (a Diamond é pequena o
    # suficiente para isso não virar gargalo) — sem isso o Spark grava N
    # partes (part-00000, part-00001, ...) e o Power BI teria que apontar
    # para uma pasta, reintroduzindo o mesmo problema de "múltiplos arquivos"
    # que este script existe para evitar.
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))

    part_files = list(tmp_dir.glob("*.parquet"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"[{table_name}] esperava 1 arquivo parquet em {tmp_dir}, encontrou {len(part_files)}"
        )

    target_path = EXPORT_DIR / f"{table_name}.parquet"
    if target_path.exists():
        target_path.unlink()
    shutil.move(str(part_files[0]), str(target_path))
    shutil.rmtree(tmp_dir)

    # Confere o snapshot escrito lendo-o de volta (mesma sessão Spark) —
    # não há Power BI disponível neste ambiente para validação visual, então
    # a conferência automatizada de contagem/schema é o que garante que o
    # arquivo exportado é fiel à tabela Diamond de origem.
    df_check = spark.read.parquet(str(target_path))
    check_count = df_check.count()
    check_columns = df_check.columns

    status = "OK" if (check_count == source_count and check_columns == source_columns) else "MISMATCH"
    logger.info(
        f"[diamond.{table_name}] origem={source_count} linhas / snapshot={check_count} linhas "
        f"/ colunas_iguais={check_columns == source_columns} -> {status}"
    )

    return {
        "table": table_name,
        "source_count": source_count,
        "snapshot_count": check_count,
        "columns_match": check_columns == source_columns,
        "status": status,
        "path": str(target_path),
    }


def main():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark_session("erp-export-powerbi-snapshot")
    results = []
    try:
        for table_name in DIAMOND_TABLES:
            results.append(export_table(spark, table_name))
    finally:
        spark.stop()

    logger.info("== Resumo do export Power BI ==")
    all_ok = True
    for r in results:
        if r["status"] != "OK":
            all_ok = False
        logger.info(f"  {r['table']}: {r['status']} ({r['snapshot_count']} linhas) -> {r['path']}")
    logger.info(f"== Export {'PASSOU' if all_ok else 'FALHOU'} ==")

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
