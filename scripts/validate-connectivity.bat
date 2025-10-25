@echo off
REM Connectivity validation script for running Airflow services (Windows)
REM Tests if services are running and accessible

echo ========================================
echo Airflow Services Connectivity Test
echo ========================================

REM Check if curl is available
curl --version >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: curl not found. Some connectivity tests will be skipped.
    set CURL_AVAILABLE=false
) else (
    set CURL_AVAILABLE=true
)

REM Check if services are running
echo.
echo [1/6] Checking if Docker containers are running...
docker-compose ps --services --filter "status=running" | findstr "postgres" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: PostgreSQL service is not running
    echo Run: docker-compose up -d
    exit /b 1
)

docker-compose ps --services --filter "status=running" | findstr "airflow-api-server" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow API server is not running
    echo Run: docker-compose up -d
    exit /b 1
)

echo ✓ Core services are running

REM Test database connectivity
echo.
echo [2/6] Testing database connectivity...
docker-compose exec -T postgres pg_isready -U airflow >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: PostgreSQL database is not ready
    exit /b 1
)
echo ✓ Database is accessible

REM Test Airflow API server health
echo.
echo [3/6] Testing Airflow API server health...
if "%CURL_AVAILABLE%"=="true" (
    timeout /t 5 >nul 2>&1
    curl -f -s http://localhost:8080/health >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Airflow API server health check failed
        echo Check if the service is fully started: docker-compose logs airflow-api-server
        exit /b 1
    )
    echo ✓ API server is healthy
) else (
    echo SKIPPED: curl not available
)

REM Test web UI accessibility
echo.
echo [4/6] Testing web UI accessibility...
if "%CURL_AVAILABLE%"=="true" (
    curl -f -s http://localhost:8080/ >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Airflow web UI is not accessible
        echo Check: http://localhost:8080
        exit /b 1
    )
    echo ✓ Web UI is accessible at http://localhost:8080
) else (
    echo SKIPPED: curl not available
)

REM Check scheduler health
echo.
echo [5/6] Testing scheduler connectivity...
docker-compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname airflow-scheduler >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: Scheduler job check failed (may be starting up)
) else (
    echo ✓ Scheduler is running
)

REM Check if CeleryExecutor services are running (if profile is active)
echo.
echo [6/6] Checking optional services...
docker-compose ps --services --filter "status=running" | findstr "redis" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Redis service is running (CeleryExecutor mode)
    
    docker-compose ps --services --filter "status=running" | findstr "airflow-worker" >nul 2>&1
    if %errorlevel% equ 0 (
        echo ✓ Airflow worker is running
    ) else (
        echo WARNING: Airflow worker is not running
    )
    
    docker-compose ps --services --filter "status=running" | findstr "flower" >nul 2>&1
    if %errorlevel% equ 0 (
        echo ✓ Flower monitoring is running at http://localhost:5555
    )
) else (
    echo ✓ Running in LocalExecutor mode (Redis not needed)
)

echo.
echo ========================================
echo ✓ Connectivity tests completed!
echo ========================================
echo.
echo Access points:
echo   Web UI: http://localhost:8080
echo   Default credentials: admin / admin
if "%CURL_AVAILABLE%"=="true" (
    docker-compose ps --services --filter "status=running" | findstr "flower" >nul 2>&1
    if %errorlevel% equ 0 (
        echo   Flower: http://localhost:5555
    )
)
echo.
echo To test authentication and API access, use:
echo   scripts\test-auth.bat

pause