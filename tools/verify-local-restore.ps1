param([Parameter(Mandatory=$true)][string]$BackupFile)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Backup = (Resolve-Path $BackupFile).Path
$Temp = Join-Path $Root ".data\restore-verify"
if (Test-Path $Temp) { Remove-Item -LiteralPath $Temp -Recurse -Force }
New-Item -ItemType Directory -Force -Path $Temp | Out-Null
Expand-Archive -Path $Backup -DestinationPath $Temp
$Dump = Join-Path $Temp "ai_hot.dump"
if (-not (Test-Path $Dump)) { throw "Backup archive has no ai_hot.dump" }

Set-Location $Root
$container = (& docker compose ps -q postgres).Trim()
if (-not $container) { throw "Local PostgreSQL is not running" }
$Database = "ai_hot_restore_verify_" + (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
& docker exec $container createdb -U radar $Database
try {
  & docker cp $Dump "${container}:/tmp/ai-hot-restore.dump"
  & docker exec $container pg_restore -U radar -d $Database --no-owner --no-privileges /tmp/ai-hot-restore.dump
  if ($LASTEXITCODE -ne 0) { throw "Backup restore failed" }
  $counts = & docker exec $container psql -U radar -d $Database -At -c "SELECT (SELECT count(*) FROM observations) || ',' || (SELECT count(*) FROM events);"
  Write-Host "Restore verified. observations,events=$counts"
} finally {
  & docker exec $container rm -f /tmp/ai-hot-restore.dump *> $null
  & docker exec $container dropdb -U radar --if-exists $Database *> $null
  Remove-Item -LiteralPath $Temp -Recurse -Force
}
