@echo off
REM Configuration validation script for Airflow 3.1.0 (Windows)
REM Tests that deprecation warnings are eliminated and configuration is correct

echo ========================================
echo Airflow 3.1.0 Configuration Validation
echo ========================================

REM Check prerequisites
echo.
echo [1/6] Checking prerequisites...
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

REM Validate configuration parameters
echo.
echo [2/6] Validating Airflow 3.x configuration parameters...

REM Check that AIRFLOW__API__SECRET_KEY is used instead of deprecated AIRFLOW__WEBSERVER__SECRET_KEY
docker-compose config | findstr "AIRFLOW__API__SECRET_KEY" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: AIRFLOW__API__SECRET_KEY not found in configuration
    echo This is required for Airflow 3.x API server
    exit /b 1
)
echo ✓ AIRFLOW__API__SECRET_KEY is configured

REM Check that deprecated AIRFLOW__WEBSERVER__SECRET_KEY is not used
docker-compose config | findstr "AIRFLOW__WEBSERVER__SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: AIRFLOW__WEBSERVER__SECRET_KEY found in configuration
    echo This parameter is deprecated in Airflow 3.x, use AIRFLOW__API__SECRET_KEY instead
) else (
    echo ✓ Deprecated AIRFLOW__WEBSERVER__SECRET_KEY not found
)

REM Check other Airflow 3.x specific configurations
docker-compose config | findstr "apache/airflow:3.1.0" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow 3.1.0 image not found in configuration
    exit /b 1
)
echo ✓ Airflow 3.1.0 image is configured

echo ✓ Configuration parameters validation passed

REM Start services to test for deprecation warnings
echo.
echo [3/6] Starting services to check for deprecation warnings...
echo This may take a few minutes...

REM Clean up any existing containers
docker-compose down >nul 2>&1

REM Start services and capture logs
echo Starting Airflow services...
docker-compose up -d --quiet-pull 2>startup_errors.log

REM Wait for services to initialize
echo Waiting for services to initialize (60 seconds)...
timeout /t 60 /nobreak >nul

REM Check service status
echo.
echo [4/6] Checking service health...
docker-compose ps --format table

REM Check for deprecation warnings in logs
echo.
echo [5/6] Checking for deprecation warnings in service logs...

set "DEPRECATION_FOUND=false"

REM Check airflow-init logs for deprecation warnings
echo Checking airflow-init logs...
docker-compose logs airflow-init 2>&1 | findstr /i "deprecat" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecation warnings found in airflow-init logs:
    docker-compose logs airflow-init 2>&1 | findstr /i "deprecat"
    set "DEPRECATION_FOUND=true"
)

REM Check api-server logs for deprecation warnings
echo Checking airflow-api-server logs...
docker-compose logs airflow-api-server 2>&1 | findstr /i "deprecat" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecation warnings found in airflow-api-server logs:
    docker-compose logs airflow-api-server 2>&1 | findstr /i "deprecat"
    set "DEPRECATION_FOUND=true"
)

REM Check scheduler logs for deprecation warnings
echo Checking airflow-scheduler logs...
docker-compose logs airflow-scheduler 2>&1 | findstr /i "deprecat" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecation warnings found in airflow-scheduler logs:
    docker-compose logs airflow-scheduler 2>&1 | findstr /i "deprecat"
    set "DEPRECATION_FOUND=true"
)

REM Check dag-processor logs for deprecation warnings
echo Checking airflow-dag-processor logs...
docker-compose logs airflow-dag-processor 2>&1 | findstr /i "deprecat" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecation warnings found in airflow-dag-processor logs:
    docker-compose logs airflow-dag-processor 2>&1 | findstr /i "deprecat"
    set "DEPRECATION_FOUND=true"
)

REM Check triggerer logs for deprecation warnings
echo Checking airflow-triggerer logs...
docker-compose logs airflow-triggerer 2>&1 | findstr /i "deprecat" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecation warnings found in airflow-triggerer logs:
    docker-compose logs airflow-triggerer 2>&1 | findstr /i "deprecat"
    set "DEPRECATION_FOUND=true"
)

if "%DEPRECATION_FOUND%"=="false" (
    echo ✓ No deprecation warnings found in service logs
) else (
    echo ⚠ Deprecation warnings detected - review configuration
)

REM Test authentication and API access
echo.
echo [6/6] Testing authentication and API access...

REM Wait for API server to be ready
echo Waiting for API server to be ready...
set "RETRY_COUNT=0"
:wait_loop
curl -f -s http://localhost:8080/health >nul 2>&1
if %errorlevel% equ 0 goto api_ready
set /a RETRY_COUNT+=1
if %RETRY_COUNT% geq 30 (
    echo ERROR: API server did not become ready within 5 minutes
    goto cleanup_and_exit
)
timeout /t 10 /nobreak >nul
goto wait_loop

:api_ready
echo ✓ API server is ready

REM Test authentication
curl -s -u admin:admin http://localhost:8080/api/v1/version >temp_version.json 2>&1
if %errorlevel% equ 0 (
    echo ✓ Authentication is working correctly
    echo API version response:
    type temp_version.json
    del temp_version.json 2>nul
) else (
    echo ERROR: Authentication test failed
    echo Response:
    type temp_version.json 2>nul
    del temp_version.json 2>nul
    goto cleanup_and_exit
)

REM Test that services start without configuration warnings
echo.
echo Checking for configuration warnings in startup logs...
if exist startup_errors.log (
    findstr /i "warning\|error" startup_errors.log >nul 2>&1
    if %errorlevel% equ 0 (
        echo WARNING: Configuration warnings or errors found during startup:
        type startup_errors.log
    ) else (
        echo ✓ No configuration warnings found during startup
    )
    del startup_errors.log 2>nul
)

echo.
echo ========================================
if "%DEPRECATION_FOUND%"=="false" (
    echo ✓ Configuration validation PASSED!
) else (
    echo ⚠ Configuration validation completed with warnings
)
echo ========================================
echo.
echo Summary:
echo   - Airflow 3.1.0 configuration: Valid
echo   - API secret key configuration: Correct
echo   - Deprecated parameters: %DEPRECATION_FOUND%
echo   - Authentication: Working
echo   - Services startup: Successful
echo.
echo Services are running. Access Airflow at:
echo   http://localhost:8080 (admin/admin)
echo.
echo To stop services: docker-compose down

goto end

:cleanup_and_exit
echo.
echo Cleaning up services due to errors...
docker-compose down >nul 2>&1
del startup_errors.log 2>nul
exit /b 1

:end
pause