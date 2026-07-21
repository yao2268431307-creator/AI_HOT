param(
  [int]$Hours = 72,
  [int]$IntervalSeconds = 900
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RunDir = Join-Path $Root ".data\run"
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Output = Join-Path $Root ".data\acceptance\local-soak.jsonl"
$Summary = Join-Path $Root ".data\acceptance\local-soak-summary.json"
New-Item -ItemType Directory -Force -Path (Split-Path $Output) | Out-Null
$PidFile = Join-Path $RunDir "soak.pid"
if (Test-Path $PidFile) {
  $existing = Get-Process -Id ([int](Get-Content $PidFile)) -ErrorAction SilentlyContinue
  if ($existing) {
    Write-Host "Local soak monitor is already running. PID=$($existing.Id)"
    exit 0
  }
}
$process = Start-Process -FilePath $Python -WorkingDirectory $Root -WindowStyle Hidden -PassThru `
  -ArgumentList @("tools/local_soak_monitor.py", "--duration-hours", $Hours, "--interval-seconds", $IntervalSeconds, "--output", $Output, "--summary", $Summary) `
  -RedirectStandardOutput (Join-Path $RunDir "soak.stdout.log") `
  -RedirectStandardError (Join-Path $RunDir "soak.stderr.log")
Set-Content -Path $PidFile -Value $process.Id
Write-Host "Local soak monitor started. PID=$($process.Id), summary=$Summary"
