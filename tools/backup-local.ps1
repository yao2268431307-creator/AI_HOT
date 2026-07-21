param([string]$Destination = "")

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root
$BackupRoot = if ($Destination) { $Destination } else { Join-Path $Root ".data\backups" }
New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null
$Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$Work = Join-Path $BackupRoot $Stamp
New-Item -ItemType Directory -Force -Path $Work | Out-Null

$container = (& docker compose ps -q postgres).Trim()
if (-not $container) { throw "Local PostgreSQL is not running" }
$IntegrityQuery = @"
SELECT
  (SELECT count(*) FROM event_embeddings c WHERE NOT EXISTS (SELECT 1 FROM events p WHERE p.id=c.event_id)) +
  (SELECT count(*) FROM event_metric_snapshots c WHERE NOT EXISTS (SELECT 1 FROM events p WHERE p.id=c.event_id)) +
  (SELECT count(*) FROM event_observations c WHERE NOT EXISTS (SELECT 1 FROM events p WHERE p.id=c.event_id) OR NOT EXISTS (SELECT 1 FROM observations p WHERE p.id=c.observation_id)) +
  (SELECT count(*) FROM observation_processing c WHERE NOT EXISTS (SELECT 1 FROM observations p WHERE p.id=c.observation_id)) +
  (SELECT count(*) FROM score_runs c WHERE NOT EXISTS (SELECT 1 FROM events p WHERE p.id=c.event_id));
"@
$OrphanCount = (& docker exec $container psql -U radar -d ai_hot -At -c $IntegrityQuery).Trim()
if ([int64]$OrphanCount -ne 0) {
  throw "Database integrity check failed: $OrphanCount orphan rows. Backup was not created."
}
$containerDump = "/tmp/ai-hot-$Stamp.dump"
& docker exec $container pg_dump -U radar -d ai_hot -Fc -f $containerDump
if ($LASTEXITCODE -ne 0) { throw "pg_dump failed" }
& docker cp "${container}:$containerDump" (Join-Path $Work "ai_hot.dump")
& docker exec $container rm -f $containerDump

Copy-Item "config/feeds.local.json" $Work
Copy-Item "config/source_identities.local.json" $Work
$EvidenceRoot = Join-Path $Root ".data\evidence"
if (Test-Path $EvidenceRoot) {
  Get-ChildItem -Path $EvidenceRoot -Recurse -File | Select-Object FullName,Length,LastWriteTimeUtc |
    Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $Work "evidence-index.csv")
} else {
  Set-Content -Path (Join-Path $Work "evidence-index.csv") -Value '"FullName","Length","LastWriteTimeUtc"'
}

$Archive = Join-Path $BackupRoot "$Stamp.zip"
Compress-Archive -Path (Join-Path $Work "*") -DestinationPath $Archive -CompressionLevel Optimal
Remove-Item -LiteralPath $Work -Recurse -Force
Get-ChildItem -Path $BackupRoot -Filter "*.zip" | Sort-Object LastWriteTimeUtc -Descending | Select-Object -Skip 7 |
  Remove-Item -Force
Write-Host $Archive
