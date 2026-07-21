param(
  [switch]$SkipModelSetup,
  [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

if (-not (Test-Path ".env")) {
  Copy-Item ".env.local.example" ".env"
}

Get-Content ".env" | ForEach-Object {
  $line = $_.Trim()
  if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
    $parts = $line.Split("=", 2)
    if (-not (Test-Path "Env:$($parts[0])")) {
      Set-Item -Path "Env:$($parts[0])" -Value $parts[1]
    }
  }
}

if ($env:RUNTIME_PROFILE -ne "local") {
  throw "start-local.ps1 requires RUNTIME_PROFILE=local"
}
if ($env:AUTH_REQUIRED -ne "false") {
  throw "local single-user mode requires AUTH_REQUIRED=false"
}
if ($env:FREE_ONLY_MODE -ne "true" -or [double]$env:EXTERNAL_DATA_BUDGET_RMB -ne 0) {
  throw "local mode requires FREE_ONLY_MODE=true and a zero external-data budget"
}
$WebHost = if ($env:RADAR_WEB_HOST) { $env:RADAR_WEB_HOST } else { "127.0.0.1" }
$WebPort = if ($env:RADAR_WEB_PORT) { [int]$env:RADAR_WEB_PORT } else { 3210 }
if ($WebHost -notin @("127.0.0.1", "localhost", "::1")) {
  throw "local single-user mode requires RADAR_WEB_HOST to be a loopback host"
}
if ($WebPort -lt 1 -or $WebPort -gt 65535) {
  throw "RADAR_WEB_PORT must be between 1 and 65535"
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker Desktop is not running" }

$drive = Get-PSDrive -Name ([System.IO.Path]::GetPathRoot($Root).TrimEnd('\').TrimEnd(':'))
if ($drive.Free -lt 8GB) { throw "At least 8 GB of free disk space is required" }

& docker compose up -d postgres
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL failed to start" }
$savedErrorPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& docker compose --profile full stop redis object-store *> $null
$ErrorActionPreference = $savedErrorPreference

$healthy = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
  $container = (& docker compose ps -q postgres).Trim()
  if ($container) {
    $state = (& docker inspect --format '{{.State.Health.Status}}' $container 2>$null).Trim()
    if ($state -eq "healthy") { $healthy = $true; break }
  }
  Start-Sleep -Seconds 2
}
if (-not $healthy) { throw "PostgreSQL did not become healthy" }

$container = (& docker compose ps -q postgres).Trim()
$savedErrorPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& docker exec -i $container psql -v ON_ERROR_STOP=1 -U radar -d ai_hot -f /docker-entrypoint-initdb.d/000_local_roles.sql *> $null
$rolesExit = $LASTEXITCODE
& docker exec -i $container psql -v ON_ERROR_STOP=1 -U radar -d ai_hot -f /docker-entrypoint-initdb.d/001_init.sql *> $null
$migrationExit = $LASTEXITCODE
$ErrorActionPreference = $savedErrorPreference
if ($rolesExit -ne 0) { throw "Local PostgreSQL roles could not be applied" }
if ($migrationExit -ne 0) { throw "Database migration failed" }

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { throw "Python environment is missing: .venv" }

if (-not $SkipModelSetup) {
  $savedErrorPreference = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & $Python -c "import sentence_transformers" 2>$null
  $modelDependencyMissing = $LASTEXITCODE -ne 0
  $ErrorActionPreference = $savedErrorPreference
  if ($modelDependencyMissing) {
    & $Python -m pip install -r "services/api/requirements-local-ml.txt"
    if ($LASTEXITCODE -ne 0) { throw "Local embedding dependencies failed to install" }
  }
  $savedErrorPreference = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & $Python -c "import os; from sentence_transformers import SentenceTransformer; SentenceTransformer(os.getenv('EMBEDDING_MODEL','BAAI/bge-m3'), cache_folder=os.getenv('EMBEDDING_CACHE_DIR','.data/models/bge-m3'), device='cpu')"
  $modelPreloadFailed = $LASTEXITCODE -ne 0
  $ErrorActionPreference = $savedErrorPreference
  if ($modelPreloadFailed) {
    Write-Warning "BGE-M3 could not be downloaded from the current network. Starting in rule-clustering degraded mode; the worker will retry later."
  }
}

$RunDir = Join-Path $Root ".data\run"
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
$env:PYTHONPATH = Join-Path $Root "services\api"

function Start-RadarProcess([string]$Name, [string]$Program, [string[]]$Arguments, [string]$WorkingDirectory, [int]$Port = 0) {
  $pidFile = Join-Path $RunDir "$Name.pid"
  if (Test-Path $pidFile) {
    $existing = Get-Process -Id ([int](Get-Content $pidFile)) -ErrorAction SilentlyContinue
    if ($existing) { return $existing }
  }
  if ($Port -gt 0) {
    $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($listener) { throw "Port $Port is already in use; stop the existing local service first" }
  }
  $process = Start-Process -FilePath $Program -ArgumentList $Arguments -WorkingDirectory $WorkingDirectory `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $RunDir "$Name.stdout.log") `
    -RedirectStandardError (Join-Path $RunDir "$Name.stderr.log")
  Set-Content -Path $pidFile -Value $process.Id
  return $process
}

Start-RadarProcess "api" $Python @("services/api/run.py") $Root 8017 | Out-Null
Start-RadarProcess "worker" $Python @("-m", "radar.runner", "--interval-seconds", "900") $Root | Out-Null
Start-RadarProcess "web" "npm.cmd" @("run", "dev", "--", "--host", $WebHost, "--port", "$WebPort") (Join-Path $Root "web") $WebPort | Out-Null

$apiReady = $false
for ($attempt = 0; $attempt -lt 60; $attempt++) {
  try {
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:8017/health" -TimeoutSec 2
    if ($health.status -eq "ok" -and $health.runtimeProfile -eq "local") { $apiReady = $true; break }
  } catch { }
  Start-Sleep -Seconds 2
}
if (-not $apiReady) { throw "Local API did not become ready; inspect .data/run/api.stderr.log" }

if (-not $NoBrowser) { Start-Process "http://${WebHost}:$WebPort" | Out-Null }
Write-Host "AI Hot Radar is running locally at http://${WebHost}:$WebPort"
Write-Host "No Redis, R2, OIDC, or metered connector is enabled."
