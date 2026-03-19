@echo off
setlocal EnableExtensions

REM One-click Windows script:
REM 1) docker compose down --volumes --remove-orphans
REM 2) delete local runtime/ folder
REM 3) docker compose build --no-cache app
REM 4) docker compose up -d --force-recreate

set ROOT_DIR=%~dp0..
pushd "%ROOT_DIR%"

echo [build] Stopping containers + removing volumes...
docker compose down --volumes --remove-orphans >nul 2>&1

echo [build] Removing runtime\ folder...
if exist "runtime\" rmdir /s /q runtime

echo [build] Building app image with --no-cache...
docker compose build --no-cache app

echo [build] Starting stack...
docker compose up -d --force-recreate

echo.
echo [build] Done. Dashboard should be on http://127.0.0.1:8000/
echo.

popd
endlocal

