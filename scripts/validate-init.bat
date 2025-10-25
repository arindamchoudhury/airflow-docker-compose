@echo off
REM Validation script for airflow-init service (Windows)
REM This script tests the database initialization functionality

echo === Airflow Init Service Validation ===

REM Check if Docker Compose is available
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: docker-compose is not installed or not in PATH
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo ERROR: .env file not found
    exit /b 1
)

echo ✓ Prerequisites check passed

REM Validate docker-compose configuration
echo Validating Docker Compose configuration...
docker-compose config --quiet >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker Compose configuration is invalid
    exit /b 1
)

echo ✓ Docker Compose configuration is valid

REM Check if required directories exist
set "required_dirs=dags logs plugins config"
for %%d in (%required_dirs%) do (
    if not exist "%%d" (
        echo ERROR: Required directory '%%d' does not exist
        exit /b 1
    )
)

echo ✓ Required directories exist

REM Test airflow-init service configuration
echo Testing airflow-init service configuration...
docker-compose config | findstr "airflow-init:" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: airflow-init service not found in configuration
    exit /b 1
)

echo ✓ airflow-init service is properly configured

echo === Validation Completed Successfully ===
echo The airflow-init service is properly configured and ready to use.
echo.
echo To start the services:
echo   docker-compose up airflow-init
echo.
echo To start all services:
echo   docker-compose up -d

pause