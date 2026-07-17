"""
fix_source_encoding.py

Script de manutenção ÚNICA EXECUÇÃO (não faz parte do pipeline agendado):
corrige texto corrompido nas tabelas erp.* do banco de origem (ERP_Sales),
causado por um bug de codificação — não é um problema de qualidade de dado
"de negócio" (não é isso que a Silver existe para tratar), é um acidente de
ferramenta.

--------------------------------------------------------------------------
CAUSA RAIZ (por que o texto ficou "SÃ£o Paulo" em vez de "São Paulo")
--------------------------------------------------------------------------
`sql/02_insert_sample_data.sql` e `sql/05_simulate_daily_activity.sql` são
arquivos .sql salvos em UTF-8 com texto acentuado literal (ex.: "São Paulo",
"Divergência no pedido"). Rodar esses scripts via `sqlcmd` SEM especificar a
code page de entrada (`-f 65001`) faz o sqlcmd interpretar o arquivo usando a
code page padrão do console Windows (não UTF-8) — cada caractere acentuado,
que ocupa 2 bytes em UTF-8, é lido como 2 caracteres Latin-1/CP-1252
separados. Resultado: "ã" (bytes UTF-8 0xC3 0xA3) vira o texto "Ã£" quando
gravado na coluna VARCHAR (não-Unicode) do SQL Server.

Isso NÃO é dado sujo que um ERP real produziria — é um bug de invocação de
ferramenta, corrigido em `scripts/run_daily_erp_simulation.ps1` (flag
`-f 65001`) para não acontecer de novo. Este script aqui só corrige o que já
foi gravado errado até agora.

--------------------------------------------------------------------------
COMO A CORREÇÃO FUNCIONA
--------------------------------------------------------------------------
Para cada valor, tenta reverter a dupla codificação: reinterpreta o texto
armazenado como bytes CP-1252 (recupera a sequência de bytes UTF-8 original
que o sqlcmd deveria ter decodificado) e decodifica esses bytes como UTF-8
(obtém o texto correto). Se o valor não estiver corrompido (texto puro
ASCII, ou não formar uma sequência UTF-8 válida ao reverter), o round-trip
falha ou não muda nada — nesse caso o valor é deixado como está. Isso torna
seguro rodar o script sobre TODAS as linhas, não só as sabidamente
corrompidas.

Cada linha corrigida também tem `UpdatedAt` atualizado para `SYSUTCDATETIME()`
— necessário para que a extração incremental da Bronze (watermark)
identifique a correção como uma "linha alterada" na próxima rodada do
pipeline e a propague por Silver → Gold → Diamond →
`ERP_Sales_BI` (publish_to_sql.py) automaticamente, sem precisar apagar e
reprocessar nada do zero.

--------------------------------------------------------------------------
USO
--------------------------------------------------------------------------
    .venv/Scripts/python.exe scripts/fix_source_encoding.py

Conecta via Windows Integrated Security (mesmo usuário admin usado para
rodar os scripts sql/04 e sql/06 via `sqlcmd -E`) — não usa o login
`erp_extractor` (só leitura, de propósito) nem `erp_bi_writer` (escopo
isolado em ERP_Sales_BI, sem acesso a ERP_Sales).
"""
import pyodbc

SERVER = "localhost,14333"
DATABASE = "ERP_Sales"

# (tabela, coluna de chave primária, coluna de texto a corrigir)
TARGETS = [
    ("erp.Regions", "RegionID", "RegionName"),
    ("erp.Customers", "CustomerID", "CustomerName"),
    ("erp.Products", "ProductID", "ProductName"),
    ("erp.Products", "ProductID", "CategoryName"),
    ("erp.Salespersons", "SalespersonID", "SalespersonName"),
    ("erp.PaymentMethods", "PaymentMethodID", "PaymentMethodName"),
    ("erp.SalesReturns", "ReturnID", "ReturnReason"),
]


def fix_mojibake(value: str) -> str | None:
    """
    Tenta reverter double-encoding UTF-8-como-CP1252. Retorna o texto
    corrigido se a reversão produzir uma string diferente e válida;
    retorna None se o valor não estiver corrompido (nada a fazer).
    """
    if value is None:
        return None
    try:
        fixed = value.encode("cp1252").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return None
    return fixed if fixed != value else None


def main():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    cursor = conn.cursor()

    total_fixed = 0
    try:
        for table, pk_col, text_col in TARGETS:
            cursor.execute(f"SELECT {pk_col}, {text_col} FROM {table}")
            rows = cursor.fetchall()

            fixed_count = 0
            for pk_value, text_value in rows:
                fixed = fix_mojibake(text_value)
                if fixed is None:
                    continue
                cursor.execute(
                    f"UPDATE {table} SET {text_col} = ?, UpdatedAt = SYSUTCDATETIME() WHERE {pk_col} = ?",
                    fixed,
                    pk_value,
                )
                print(f"[{table}.{text_col}] {pk_col}={pk_value}: {text_value!r} -> {fixed!r}")
                fixed_count += 1

            total_fixed += fixed_count
            print(f"[{table}.{text_col}] {fixed_count} linha(s) corrigida(s) de {len(rows)} lida(s).")

        conn.commit()
        print(f"== Correção concluída: {total_fixed} linha(s) corrigida(s) no total (commitado) ==")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
