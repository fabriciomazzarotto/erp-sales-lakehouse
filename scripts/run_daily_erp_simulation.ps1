# run_daily_erp_simulation.ps1
#
# Simula um dia de atividade no ERP de origem (novas notas fiscais/itens,
# devolução ocasional, atualização de cadastros) via
# sql/05_simulate_daily_activity.sql. Deve rodar ANTES de run_pipeline.ps1
# no agendamento do Task Scheduler — ver docs/automation.md.

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$LogDir = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogDir "erp_simulation_$Timestamp.log"

$SqlServer = "localhost,14333"
$SqlScript = Join-Path $ProjectRoot "sql\05_simulate_daily_activity.sql"

"=== Simulacao ERP iniciada em $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $LogFile -Append -Encoding utf8

# Captura via pipeline do PowerShell (nao redirecionamento *>> cru) para evitar
# o output do sqlcmd (console codepage/UTF-16) virar texto com espacos entre
# cada caractere no arquivo de log.
#
# -f 65001 (UTF-8): o .sql tem texto acentuado literal (ex.: "Divergencia no
# pedido", linha de motivos de devolucao). Sem essa flag, sqlcmd le o arquivo
# usando a code page padrao do console (nao UTF-8), gravando cada caractere
# acentuado como 2 caracteres Latin-1 separados no banco ("Divergencia" vira
# "DivergÃªncia") -- bug real encontrado e corrigido nesta sessao, ver
# scripts/fix_source_encoding.py e docs/automation.md.
$result = & sqlcmd -S $SqlServer -E -C -f 65001 -i $SqlScript 2>&1
$result | Out-File -FilePath $LogFile -Append -Encoding utf8

if ($LASTEXITCODE -ne 0) {
    "!!! FALHOU (exit code $LASTEXITCODE) - ver $LogFile" | Out-File -FilePath $LogFile -Append -Encoding utf8
    exit 1
}

"=== Simulacao ERP concluida com sucesso ===" | Out-File -FilePath $LogFile -Append -Encoding utf8
exit 0
