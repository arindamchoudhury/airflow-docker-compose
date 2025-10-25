@echo off
REM Comprehensive test runner for Airflow 3.1.0 validation (Windows)
REM Runs all validation and testing scripts in sequence

echo ========================================
echo Airflow 3.1.0 Comprehensive Test Suite
echo ========================================

set "SCRIPT_DIR=%~dp0"
set "TEST_FAILED=false"

echo Starting comprehensive validation of Airflow 3.1.0 setup...
echo.

REM Test 1: Service Configuration Validation
echo ========================================
echo TEST 1: Service Configuration Validation
echo ========================================
call "%SCRIPT_DIR%validate-services.bat"
if %errorlevel% neq 0 (
    echo ERROR: Service validation failed
    set "TEST_FAILED=true"
    goto :end_tests
)
echo.

REM Test 2: Service Connectivity (requires running services)
echo ========================================
echo TEST 2: Service Connectivity Testing
echo ========================================
echo Checking if services are running...
docker-compose ps --services --filter "status=running" | findstr "airflow-api-server" >nul 2>&1
if %errorlevel% neq 0 (
    echo Services are not running. Starting services...
    echo This may take a few minutes...
    docker-compose up -d
    
    REM Wait for services to be ready
    echo Waiting for services to start up...
    timeout /t 60 >nul 2>&1
    
    REM Check again
    docker-compose ps --services --filter "status=running" | findstr "airflow-api-server" >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Failed to start services
        set "TEST_FAILED=true"
        goto :end_tests
    )
)

call "%SCRIPT_DIR%validate-connectivity.bat"
if %errorlevel% neq 0 (
    echo ERROR: Connectivity validation failed
    set "TEST_FAILED=true"
    goto :end_tests
)
echo.

REM Test 3: Authentication Testing
echo ========================================
echo TEST 3: Authentication Testing
echo ========================================
call "%SCRIPT_DIR%test-auth.bat"
if %errorlevel% neq 0 (
    echo ERROR: Authentication testing failed
    set "TEST_FAILED=true"
    goto :end_tests
)
echo.

REM Test 4: DAG Loading and Execution Testing
echo ========================================
echo TEST 4: DAG Loading and Execution Testing
echo ========================================
call "%SCRIPT_DIR%test-dags.bat"
if %errorlevel% neq 0 (
    echo ERROR: DAG testing failed
    set "TEST_FAILED=true"
    goto :end_tests
)
echo.

REM Test 5: Worker Validation (if CeleryExecutor is configured)
echo ========================================
echo TEST 5: Worker Validation (Optional)
echo ========================================
docker-compose ps --services --filter "status=running" | findstr "redis" >nul 2>&1
if %errorlevel% equ 0 (
    echo CeleryExecutor mode detected, running worker validation...
    call "%SCRIPT_DIR%validate-worker.bat"
    if %errorlevel% neq 0 (
        echo WARNING: Worker validation failed (non-critical)
    )
) else (
    echo LocalExecutor mode detected, skipping worker validation
)
echo.

:end_tests
if "%TEST_FAILED%"=="true" (
    echo ========================================
    echo ❌ TEST SUITE FAILED
    echo ========================================
    echo One or more tests failed. Please check the output above.
    echo.
    echo Common troubleshooting steps:
    echo 1. Ensure Docker Desktop is running
    echo 2. Check .env file configuration
    echo 3. Verify all required directories exist
    echo 4. Check Docker container logs: docker-compose logs
    echo.
    exit /b 1
) else (
    echo ========================================
    echo ✅ ALL TESTS PASSED!
    echo ========================================
    echo.
    echo Your Airflow 3.1.0 setup is working correctly!
    echo.
    echo Access Points:
    echo   Web UI: http://localhost:8080
    echo   Username: admin
    echo   Password: admin
    echo.
    docker-compose ps --services --filter "status=running" | findstr "flower" >nul 2>&1
    if %errorlevel% equ 0 (
        echo   Flower: http://localhost:5555
    )
    echo.
    echo To stop services: docker-compose down
    echo To view logs: docker-compose logs [service-name]
)

pause