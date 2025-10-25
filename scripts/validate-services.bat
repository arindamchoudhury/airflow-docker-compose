@echo off
REM Comprehensive service validation script for Airflow 3.1.0 (Windows)
REM Tests service startup, connectivity, and health checks

echo ========================================
echo Airflow 3.1.0 Services Validation
echo ========================================

REM Check prerequisites
echo.
echo [1/8] Checking prerequisites...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not installed or not in PATH
    exit /b 1
)

docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: docker-compose is not installed or not in PATH
    exit /b 1
)

if not exist ".env" (
    echo ERROR: .env file not found. Copy .env.example to .env and configure it.
    exit /b 1
)

echo ✓ Prerequisites check passed

REM Validate Docker Compose configuration
echo.
echo [2/8] Validating Docker Compose configuration...
docker-compose config --quiet >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker Compose configuration is invalid
    exit /b 1
)
echo ✓ Docker Compose configuration is valid

REM Check required directories
echo.
echo [3/8] Checking required directories...
set "required_dirs=dags logs plugins config"
for %%d in (%required_dirs%) do (
    if not exist "%%d" (
        echo ERROR: Required directory '%%d' does not exist
        exit /b 1
    )
)
echo ✓ Required directories exist

REM Test service definitions
echo.
echo [4/8] Validating service definitions...
docker-compose config --services | findstr "postgres" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: PostgreSQL service not found
    exit /b 1
)

docker-compose config --services | findstr "airflow-api-server" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow API server service not found
    exit /b 1
)

docker-compose config --services | findstr "airflow-scheduler" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow scheduler service not found
    exit /b 1
)

docker-compose config --services | findstr "airflow-dag-processor" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow DAG processor service not found
    exit /b 1
)

docker-compose config --services | findstr "airflow-triggerer" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow triggerer service not found
    exit /b 1
)

echo ✓ All required services are defined

REM Test health check configurations
echo.
echo [5/8] Validating health check configurations...
docker-compose config | findstr "healthcheck:" >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: No health checks found in configuration
) else (
    echo ✓ Health checks are configured
)

REM Test volume mounts
echo.
echo [6/8] Validating volume mounts...
docker-compose config | findstr "./dags:/opt/airflow/dags" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: DAGs volume mount not configured correctly
    exit /b 1
)

docker-compose config | findstr "./logs:/opt/airflow/logs" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Logs volume mount not configured correctly
    exit /b 1
)

echo ✓ Volume mounts are configured correctly

REM Test environment variables
echo.
echo [7/8] Validating environment variables...
docker-compose config | findstr "AIRFLOW__CORE__EXECUTOR" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Executor configuration not found
    exit /b 1
)

docker-compose config | findstr "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Database connection configuration not found
    exit /b 1
)

echo ✓ Environment variables are configured

REM Test network configuration
echo.
echo [8/8] Validating network configuration...
docker-compose config | findstr "airflow-network" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow network not configured
    exit /b 1
)

echo ✓ Network configuration is valid

echo.
echo ========================================
echo ✓ All service validations passed!
echo ========================================
echo.
echo Services are ready to start. Use:
echo   docker-compose up -d          (LocalExecutor mode)
echo   docker-compose --profile celery up -d  (CeleryExecutor mode)
echo.
echo To validate running services, use:
echo   scripts\validate-connectivity.bat

pause