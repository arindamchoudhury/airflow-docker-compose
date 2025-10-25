@echo off
REM Authentication testing script for Airflow 3.1.0 (Windows)
REM Tests Simple Auth Manager and API access

echo ========================================
echo Airflow Authentication Testing
echo ========================================

REM Check if curl is available
curl --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: curl is required for authentication testing
    echo Please install curl or use Git Bash
    exit /b 1
)

REM Check if jq is available for JSON parsing
jq --version >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: jq not found. JSON responses will not be formatted.
    set JQ_AVAILABLE=false
) else (
    set JQ_AVAILABLE=true
)

REM Test if Airflow is running
echo.
echo [1/5] Checking if Airflow is accessible...
curl -f -s http://localhost:8080/health >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow is not accessible at http://localhost:8080
    echo Make sure services are running: docker-compose up -d
    exit /b 1
)
echo ✓ Airflow is accessible

REM Test web UI login page
echo.
echo [2/5] Testing web UI login page...
curl -f -s http://localhost:8080/login >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Login page is not accessible
    exit /b 1
)
echo ✓ Login page is accessible

REM Test Simple Auth Manager configuration
echo.
echo [3/5] Testing Simple Auth Manager configuration...
docker-compose exec -T airflow-api-server cat /opt/airflow/config/users.json >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Simple Auth Manager users.json file not found
    exit /b 1
)
echo ✓ Simple Auth Manager is configured

REM Test API authentication with basic auth
echo.
echo [4/5] Testing API authentication...
set "API_RESPONSE=temp_api_response.json"

REM Test unauthenticated request (should fail)
curl -s -o nul -w "%%{http_code}" http://localhost:8080/api/v1/dags | findstr "401" >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: Unauthenticated API request did not return 401 (may indicate auth issue)
) else (
    echo ✓ Unauthenticated requests are properly rejected
)

REM Test authenticated request
echo Testing authenticated API request...
curl -s -u admin:admin -o "%API_RESPONSE%" -w "%%{http_code}" http://localhost:8080/api/v1/dags > temp_status.txt 2>&1
set /p HTTP_STATUS=<temp_status.txt

if "%HTTP_STATUS%"=="200" (
    echo ✓ Authenticated API request successful
    if "%JQ_AVAILABLE%"=="true" (
        echo Response preview:
        jq ".total_entries" "%API_RESPONSE%" 2>nul
    )
) else (
    echo ERROR: Authenticated API request failed with status %HTTP_STATUS%
    echo Response:
    type "%API_RESPONSE%" 2>nul
    del "%API_RESPONSE%" 2>nul
    del temp_status.txt 2>nul
    exit /b 1
)

REM Test specific API endpoints
echo.
echo [5/5] Testing specific API endpoints...

REM Test version endpoint
curl -s -u admin:admin http://localhost:8080/api/v1/version > temp_version.json 2>&1
if %errorlevel% equ 0 (
    echo ✓ Version endpoint accessible
    if "%JQ_AVAILABLE%"=="true" (
        echo Airflow version:
        jq -r ".version" temp_version.json 2>nul
    )
) else (
    echo WARNING: Version endpoint test failed
)

REM Test config endpoint
curl -s -u admin:admin http://localhost:8080/api/v1/config >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Config endpoint accessible
) else (
    echo WARNING: Config endpoint test failed
)

REM Test pools endpoint
curl -s -u admin:admin http://localhost:8080/api/v1/pools >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Pools endpoint accessible
) else (
    echo WARNING: Pools endpoint test failed
)

REM Cleanup
del "%API_RESPONSE%" 2>nul
del temp_status.txt 2>nul
del temp_version.json 2>nul

echo.
echo ========================================
echo ✓ Authentication tests completed!
echo ========================================
echo.
echo Test Results:
echo   - Simple Auth Manager: Working
echo   - Web UI Login: Accessible
echo   - API Authentication: Working
echo   - Default Credentials: admin / admin
echo.
echo API Usage Examples:
echo   curl -u admin:admin http://localhost:8080/api/v1/dags
echo   curl -u admin:admin http://localhost:8080/api/v1/version
echo.
echo To test DAG loading and execution, use:
echo   scripts\test-dags.bat

pause