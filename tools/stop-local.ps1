param([switch]$KeepDatabase)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RunDir = Join-Path $Root ".data\run"

function Stop-ProcessTree([int]$ProcessId) {
  $children = Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue
  foreach ($child in $children) { Stop-ProcessTree ([int]$child.ProcessId) }
  Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

foreach ($name in @("web", "worker", "api", "soak")) {
  $pidFile = Join-Path $RunDir "$name.pid"
  if (Test-Path $pidFile) {
    $processId = [int](Get-Content $pidFile)
    Stop-ProcessTree $processId
    Remove-Item -LiteralPath $pidFile -Force
  }
}
if (-not $KeepDatabase) {
  & docker compose -f (Join-Path $Root "docker-compose.yml") stop postgres *> $null
}
Write-Host "AI Hot Radar local processes stopped."
