@echo off
REM Validation script for Airflow Worker service configuration
echo ========================================
echo Airflow Worker Service Validation
echo ========================================

echo.
echo Checking Docker Compose configuration...
docker-compose config --quiet
if %ERRORLEVEL% neq 0 (
    echo ERROR: Docker Compose configuration is invalid!
    exit /b 1
)
echo ✓ Docker Compose configuration is valid

echo.
echo Checking if Redis service is configured...
docker-compose config | findstr "redis:" >nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Redis service not found in configuration!
    exit /b 1
)
echo ✓ Redis service is configured

echo.
echo Checking if Worker service is configured with celery profile...
docker-compose --profile celery config --services | findstr "airflow-worker" >nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Airflow Worker service not found in celery profile!
    exit /b 1
)
echo ✓ Airflow Worker service is configured with celery profile

echo.
echo Checking CeleryExecutor configuration...
docker-compose --profile celery config | findstr "AIRFLOW__CORE__EXECUTOR: CeleryExecutor" >nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: CeleryExecutor not configured in worker service!
    exit /b 1
)
echo ✓ CeleryExecutor is configured

echo.
echo Checking Redis broker URL configuration...
docker-compose --profile celery config | findstr "AIRFLOW__CELERY__BROKER_URL" >nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Redis broker URL not configured!
    exit /b 1
)
echo ✓ Redis broker URL is configured

echo.
echo ========================================
echo ✓ All Worker service validations passed!
echo ========================================
echo.
echo To start with CeleryExecutor mode, run:
echo docker-compose --profile celery up -d
echo.
echo To start with LocalExecutor mode (default), run:
echo docker-compose up -d