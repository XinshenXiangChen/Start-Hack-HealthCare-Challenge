$ErrorActionPreference = "Stop"

# Build/run your Docker stack from a clean slate (no cache).
# This script is Windows PowerShell-focused.

$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $rootDir "..")
Set-Location $rootDir

Write-Host "[build] Stopping containers + removing volumes..."
docker compose down --volumes --remove-orphans 2>$null | Out-Null

Write-Host "[build] Removing runtime\ folder..."
if (Test-Path (Join-Path $rootDir "runtime")) {
  Remove-Item -Recurse -Force (Join-Path $rootDir "runtime")
}

Write-Host "[build] Building app image with --no-cache..."
docker compose build --no-cache app

Write-Host "[build] Starting stack..."
docker compose up -d --force-recreate

Write-Host "[build] Done. Dashboard should be on http://127.0.0.1:8000/"

