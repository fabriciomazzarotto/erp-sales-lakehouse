# run_pipeline.ps1
#
# Orquestra a execucao diaria do pipeline ERP Sales Lakehouse:
# Bronze -> Silver -> Gold -> Diamond -> export para Power BI.
# Deve rodar DEPOIS de run_daily_erp_simulation.ps1 no agendamento do
# Task Scheduler (precisa de dado novo na origem para ter o que processar)
# — ver docs/automation.md.
#
# Para na primeira falha (nao adianta rodar Gold se a Silver quebrou).

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$LogDir = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogDir "pipeline_$Timestamp.log"

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

$Steps = @(
    "notebooks\01_ingest_bronze.py",
    "notebooks\02_transform_silver.py",
    "notebooks\03_model_gold.py",
    "notebooks\04_create_diamond.py",
    "powerbi\export_snapshot.py"
)

# Toda escrita no log passa por aqui, com retry — o arquivo pode ficar
# momentaneamente travado logo depois que o cmd.exe libera o handle
# (observado nesta maquina: provavelmente o antivirus escaneando o arquivo
# recem-escrito), o que derrubaria um Out-File direto com IOException.
function Write-Log {
    param([string]$Message)

    $attempts = 0
    while ($attempts -lt 5) {
        try {
            Add-Content -Path $LogFile -Value $Message -Encoding utf8 -ErrorAction Stop
            return
        } catch {
            $attempts++
            Start-Sleep -Milliseconds 300
        }
    }
    # Ultima tentativa sem engolir o erro, para nao mascarar um problema real
    Add-Content -Path $LogFile -Value $Message -Encoding utf8
}

Write-Log "=== Pipeline iniciado em $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==="

for ($i = 0; $i -lt $Steps.Count; $i++) {
    $step = $Steps[$i]
    $stepPath = Join-Path $ProjectRoot $step
    Write-Log "--- Rodando $step ---"

    # Redirecionamento via cmd.exe (nao *>>/2>&1 do PowerShell): o Spark escreve
    # log normal (INFO/WARN) no stderr, e o operador nativo do PowerShell trata
    # cada linha de stderr como ErrorRecord — com $ErrorActionPreference="Stop"
    # isso aborta o script tratando log normal como falha fatal, mesmo quando o
    # processo termina com exit code 0. cmd /c usa a redirecao do proprio SO,
    # sem essa reinterpretacao.
    $quotedPython = '"' + $Python + '"'
    $quotedStep = '"' + $stepPath + '"'
    $quotedLog = '"' + $LogFile + '"'
    & cmd /c "$quotedPython $quotedStep >> $quotedLog 2>&1"
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-Log "!!! FALHOU em $step (exit code $exitCode) - pipeline abortado. Ver $LogFile"
        exit 1
    }

    # Pequena folga entre etapas (nao apos a ultima — nao ha proxima JVM para
    # dar tempo de subir): cada uma sobe/derruba sua propria JVM do Spark, e
    # sem esse intervalo a proxima pode tentar subir antes da anterior liberar
    # totalmente a porta/recursos, causando falha de conexao transitoria
    # (observado nesta maquina, nao um bug de logica).
    if ($i -lt ($Steps.Count - 1)) {
        Start-Sleep -Seconds 5
    }
}

Write-Log "=== Pipeline concluido com sucesso em $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==="
exit 0
