# Demo local de MPComm (Parte 1) com 3 peers, XML-RPC e UDP confiavel.
# PowerShell 5+; rodar na pasta do projeto com: .\demo_local.ps1
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$Python = (Get-Command python -ErrorAction Stop).Source
$LogDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$Processes = New-Object System.Collections.ArrayList

function Start-MPProcess([string]$Script, [string[]]$Arguments, [string]$Name) {
    $outPath = Join-Path $LogDir "$Name.stdout.log"
    $errPath = Join-Path $LogDir "$Name.stderr.log"
    $proc = Start-Process -FilePath $Python -ArgumentList (@('-u', $Script) + $Arguments) `
        -WorkingDirectory $PSScriptRoot -PassThru -RedirectStandardOutput $outPath `
        -RedirectStandardError $errPath
    [void]$Processes.Add($proc)
    return $proc
}

try {
    Write-Host 'Iniciando gerenciador RPC...' -ForegroundColor Cyan
    $manager = Start-MPProcess 'GroupMngr.py' @('--bind','127.0.0.1','--port','5680') 'group'
    Start-Sleep -Milliseconds 600
    if ($manager.HasExited) { throw 'Gerenciador nao iniciou; veja logs/group.stderr.log' }

    Write-Host 'Iniciando tres peers UDP + RPC...' -ForegroundColor Cyan
    $peerA = Start-MPProcess 'peerCommunicatorUDP.py' @('--peer-id','A','--udp-port','6789','--rpc-port','5679','--drop-first-data-every','2','--drop-first-ack-every','3') 'peerA'
    $peerB = Start-MPProcess 'peerCommunicatorUDP.py' @('--peer-id','B','--udp-port','6790','--rpc-port','5681','--drop-first-data-every','2','--drop-first-ack-every','3') 'peerB'
    $peerC = Start-MPProcess 'peerCommunicatorUDP.py' @('--peer-id','C','--udp-port','6791','--rpc-port','5682','--drop-first-data-every','2','--drop-first-ack-every','3') 'peerC'
    Start-Sleep -Seconds 2
    if ($peerA.HasExited -or $peerB.HasExited -or $peerC.HasExited) { throw 'Um peer encerrou antes do teste; veja logs/peer*.stderr.log' }

    Write-Host 'Executando experimento: 3 peers x 8 mensagens com perdas simuladas...' -ForegroundColor Cyan
    & $Python -u comparisonServer.py --bind 127.0.0.1 --peers 3 --messages 8 --timeout 40 --log-dir logs
    if ($LASTEXITCODE -ne 0) { throw "Experimento falhou (codigo $LASTEXITCODE)" }
    Write-Host 'SUCESSO: UDP confiavel e FIFO verificados. Consulte logs/round_*.json' -ForegroundColor Green
} finally {
    Write-Host 'Encerrando processos de demonstracao...' -ForegroundColor Yellow
    foreach ($proc in $Processes) {
        try { if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } }
        catch { Write-Warning "Erro ao parar processo: $_" }
    }
}
