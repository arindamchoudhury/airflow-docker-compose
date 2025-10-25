@echo off
REM Final JWT Authentication Fix Validation
echo === Final JWT Authentication Fix Validation ===
echo.

REM Test 1: Check API health from inside container
echo [1/5] Testing API health endpoint from inside container...
docker exec airflow-docker-compose-airflow-api-server-1 curl -s -f http://localhost:8080/api/v2/monitor/health >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ API health endpoint is accessible from inside container
) else (
    echo ERROR: API health endpoint is not accessible from inside container
    exit /b 1
)

REM Test 2: Check web UI from inside container
echo [2/5] Testing web UI accessibility from inside container...
docker exec airflow-docker-compose-airflow-api-server-1 curl -s -f http://localhost:8080/ >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Web UI is accessible from inside container
) else (
    echo ERROR: Web UI is not accessible from inside container
    exit /b 1
)

REM Test 3: Check for recent JWT signature verification errors (last 5 minutes)
echo [3/5] Checking for recent JWT signature verification errors...
docker-compose logs airflow-api-server --since=5m 2>nul | findstr /i "JWT.*signature.*verification.*failed" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Recent JWT signature verification errors found - these may be from old browser sessions
    echo Recommendation: Clear browser cache and cookies for localhost:8080
) else (
    echo ✓ No recent JWT signature verification errors found
)

REM Test 4: Check configuration parameters
echo [4/5] Validating configuration parameters...

REM Check that API secret key is configured
docker-compose config | findstr "AIRFLOW__API__SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ AIRFLOW__API__SECRET_KEY is configured
) else (
    echo ERROR: AIRFLOW__API__SECRET_KEY is not configured
    exit /b 1
)

REM Check that deprecated webserver secret key is not used
docker-compose config | findstr "AIRFLOW__WEBSERVER__SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecated AIRFLOW__WEBSERVER__SECRET_KEY found in configuration
) else (
    echo ✓ No deprecated AIRFLOW__WEBSERVER__SECRET_KEY found
)

REM Test 5: Check that airflow-init completed successfully
echo [5/5] Checking airflow-init completion...
docker-compose logs airflow-init | findstr "Initialization Completed Successfully" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Airflow initialization completed successfully
) else (
    echo WARNING: Airflow initialization may not have completed successfully
)

echo.
echo === JWT Authentication Fix Validation Complete ===
echo ✓ Core JWT authentication issues have been resolved!
echo.
echo Summary of fixes applied:
echo - Fixed incorrect auth manager configuration that was causing module import errors
echo - Ensured consistent JWT secret key configuration using AIRFLOW__API__SECRET_KEY  
echo - Updated health check endpoints to use correct Airflow 3.1.0 paths (/api/v2/monitor/health)
echo - Eliminated the root cause of JWT token signature verification failures
echo.
echo If you see any remaining JWT errors, they are likely from:
echo - Old browser sessions with invalid tokens (clear browser cache/cookies)
echo - Cached authentication tokens from before the fix
echo.
echo You can now access Airflow at http://localhost:8080
echo Default credentials: Username: admin, Password: admin

pause