@echo off
REM Test JWT Authentication Configuration
REM This script validates that JWT authentication is working correctly

echo === Testing JWT Authentication Configuration ===
echo.

REM Function to check if services are running
echo [1/5] Checking if Airflow services are running...

docker-compose ps | findstr "airflow-api-server.*Up" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow API server is not running
    echo Please start services with: docker-compose up -d
    exit /b 1
)

echo ✓ Airflow API server is running

REM Function to test API health endpoint
echo [2/5] Testing API health endpoint...

REM Test from inside the container since external access might have issues
docker-compose exec -T airflow-api-server curl -s http://localhost:8080/api/v2/monitor/health > temp_health.txt 2>&1

findstr "healthy" temp_health.txt >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ API health endpoint is accessible
) else (
    echo ERROR: API health endpoint is not accessible
    echo Check if the API server is properly started
    del temp_health.txt >nul 2>&1
    exit /b 1
)

del temp_health.txt >nul 2>&1

REM Function to test authentication endpoint
echo [3/5] Testing authentication endpoint...

REM Test from inside the container
docker-compose exec -T airflow-api-server curl -s -o /dev/null -w "%%{http_code}" http://localhost:8080/auth/login > temp_auth.txt 2>&1

set /p AUTH_CODE=<temp_auth.txt
if "%AUTH_CODE%"=="200" (
    echo ✓ Authentication login page is accessible
) else (
    echo ERROR: Authentication login page is not accessible HTTP %AUTH_CODE%
    del temp_auth.txt >nul 2>&1
    exit /b 1
)

del temp_auth.txt >nul 2>&1

REM Function to test JWT token generation
echo [4/5] Testing JWT token generation...

REM Get the actual password from Simple Auth Manager
for /f "tokens=*" %%i in ('docker-compose exec -T airflow-api-server python -c "from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager; auth_manager = SimpleAuthManager(); users = auth_manager.get_users(); passwords = auth_manager.get_passwords(users); print(passwords['admin'])" 2^>nul') do set ACTUAL_PASSWORD=%%i

echo Using password from Simple Auth Manager: %ACTUAL_PASSWORD%

REM Attempt to get a JWT token using the actual generated credentials
docker-compose exec -T airflow-api-server curl -s -X POST http://localhost:8080/auth/token -H "Content-Type: application/json" -d "{\"username\": \"admin\", \"password\": \"%ACTUAL_PASSWORD%\"}" > temp_response.txt 2>&1

findstr "access_token" temp_response.txt >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ JWT token generation is working
    echo Token response:
    type temp_response.txt
) else (
    echo WARNING: JWT token generation may have issues
    echo Response:
    type temp_response.txt
)

del temp_response.txt >nul 2>&1

REM Function to check configuration
echo [5/5] Checking configuration parameters...

REM Check that API secret key is configured
docker-compose config | findstr "AIRFLOW__API__SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ AIRFLOW__API__SECRET_KEY is configured
) else (
    echo ERROR: AIRFLOW__API__SECRET_KEY is not configured
    exit /b 1
)

REM Check that Simple Auth Manager is configured
docker-compose config | findstr "AIRFLOW__CORE__AUTH_MANAGER.*SimpleAuthManager" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Simple Auth Manager is configured
) else (
    echo WARNING: Simple Auth Manager configuration not found
)

REM Check that JWT secret key is configured
docker-compose config | findstr "AIRFLOW__SIMPLE_AUTH_MANAGER__JWT_SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ JWT secret key is configured
) else (
    echo WARNING: JWT secret key configuration not found
)

REM Check for deprecated configurations
docker-compose config | findstr "AIRFLOW__WEBSERVER__SECRET_KEY" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Deprecated AIRFLOW__WEBSERVER__SECRET_KEY found
) else (
    echo ✓ No deprecated webserver secret key found
)

REM Function to check logs for JWT errors
echo.
echo === Checking recent logs for JWT-related errors ===

REM Check API server logs for JWT errors
docker-compose logs airflow-api-server 2>nul | findstr /i "jwt token signature" > temp_logs.txt 2>&1

if exist temp_logs.txt (
    for /f %%i in ('type temp_logs.txt ^| find /c /v ""') do set line_count=%%i
    if !line_count! gtr 0 (
        echo Recent JWT-related log entries:
        type temp_logs.txt
    ) else (
        echo ✓ No recent JWT-related errors found in logs
    )
    del temp_logs.txt >nul 2>&1
) else (
    echo ✓ No recent JWT-related errors found in logs
)

echo.
echo === JWT Authentication Test Complete ===
echo If you're still experiencing JWT token validation errors:
echo 1. Restart the services: docker-compose down ^&^& docker-compose up -d
echo 2. Check that the AIRFLOW_SECRET_KEY in .env is properly set
echo 3. Verify that config/users.json exists and contains valid user data
echo 4. Check the API server logs: docker-compose logs airflow-api-server

pause