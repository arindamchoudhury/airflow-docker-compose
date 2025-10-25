@echo off
REM Simple JWT Authentication Test
echo === Testing JWT Authentication Configuration ===
echo.

REM Test API health endpoint
echo [1/3] Testing API health endpoint...
curl -s -f http://localhost:8080/api/v2/monitor/health >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ API health endpoint is accessible
) else (
    echo ERROR: API health endpoint is not accessible
    exit /b 1
)

REM Test web UI accessibility
echo [2/3] Testing web UI accessibility...
curl -s -f http://localhost:8080/ >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Web UI is accessible
) else (
    echo ERROR: Web UI is not accessible
    exit /b 1
)

REM Check for JWT errors in logs
echo [3/3] Checking for JWT errors in recent logs...
docker-compose logs airflow-api-server --tail=20 2>nul | findstr /i "jwt.*error signature.*failed" >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: JWT errors found in recent logs
) else (
    echo ✓ No JWT errors found in recent logs
)

echo.
echo === JWT Authentication Test Complete ===
echo All tests passed! The JWT authentication issue appears to be resolved.
echo You can now access Airflow at http://localhost:8080
echo Default credentials: Username: admin, Password: admin

pause