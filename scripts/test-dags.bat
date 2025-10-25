@echo off
REM DAG testing script for Airflow 3.1.0 (Windows)
REM Tests DAG loading, parsing, and execution

echo ========================================
echo Airflow DAG Testing
echo ========================================

REM Check if curl is available
curl --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: curl is required for DAG testing
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
echo [1/6] Checking if Airflow is accessible...
curl -f -s http://localhost:8080/health >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Airflow is not accessible at http://localhost:8080
    echo Make sure services are running: docker-compose up -d
    exit /b 1
)
echo ✓ Airflow is accessible

REM Check if DAG files exist
echo.
echo [2/6] Checking DAG files...
if not exist "dags\example_basic_dag.py" (
    echo ERROR: example_basic_dag.py not found in dags directory
    exit /b 1
)

if not exist "dags\example_advanced_dag.py" (
    echo ERROR: example_advanced_dag.py not found in dags directory
    exit /b 1
)

echo ✓ DAG files are present

REM Test DAG loading via API
echo.
echo [3/6] Testing DAG loading via API...
set "DAGS_RESPONSE=temp_dags.json"

curl -s -u admin:admin -o "%DAGS_RESPONSE%" http://localhost:8080/api/v1/dags
if %errorlevel% neq 0 (
    echo ERROR: Failed to fetch DAGs from API
    exit /b 1
)

REM Check if our example DAGs are loaded
findstr "example_basic_dag" "%DAGS_RESPONSE%" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: example_basic_dag not found in loaded DAGs
    echo Available DAGs:
    if "%JQ_AVAILABLE%"=="true" (
        jq -r ".dags[].dag_id" "%DAGS_RESPONSE%" 2>nul
    ) else (
        type "%DAGS_RESPONSE%"
    )
    del "%DAGS_RESPONSE%" 2>nul
    exit /b 1
)

findstr "example_advanced_dag" "%DAGS_RESPONSE%" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: example_advanced_dag not found in loaded DAGs
    del "%DAGS_RESPONSE%" 2>nul
    exit /b 1
)

echo ✓ Example DAGs are loaded successfully

if "%JQ_AVAILABLE%"=="true" (
    echo Loaded DAGs:
    jq -r ".dags[].dag_id" "%DAGS_RESPONSE%" 2>nul
)

REM Test DAG details
echo.
echo [4/6] Testing DAG details...
curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag > temp_dag_details.json 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Failed to fetch DAG details
    del "%DAGS_RESPONSE%" 2>nul
    exit /b 1
)

echo ✓ DAG details accessible

if "%JQ_AVAILABLE%"=="true" (
    echo Basic DAG info:
    jq -r ".dag_id, .description, .is_paused" temp_dag_details.json 2>nul
)

REM Test DAG tasks
echo.
echo [5/6] Testing DAG tasks...
curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/tasks > temp_tasks.json 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Failed to fetch DAG tasks
    del "%DAGS_RESPONSE%" 2>nul
    del temp_dag_details.json 2>nul
    exit /b 1
)

echo ✓ DAG tasks accessible

if "%JQ_AVAILABLE%"=="true" (
    echo Basic DAG tasks:
    jq -r ".tasks[].task_id" temp_tasks.json 2>nul
)

REM Test DAG execution (trigger a DAG run)
echo.
echo [6/6] Testing DAG execution...
echo Triggering example_basic_dag...

REM Create a simple trigger request
echo {"conf": {}} > temp_trigger.json

curl -s -u admin:admin -X POST -H "Content-Type: application/json" -d @temp_trigger.json http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns > temp_run_response.json 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Failed to trigger DAG run
    del temp_trigger.json 2>nul
    del "%DAGS_RESPONSE%" 2>nul
    del temp_dag_details.json 2>nul
    del temp_tasks.json 2>nul
    exit /b 1
)

echo ✓ DAG run triggered successfully

if "%JQ_AVAILABLE%"=="true" (
    echo DAG run info:
    jq -r ".dag_run_id, .state, .execution_date" temp_run_response.json 2>nul
)

REM Wait a moment and check DAG run status
echo Waiting 10 seconds for DAG run to start...
timeout /t 10 >nul 2>&1

curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns > temp_runs.json 2>&1
if %errorlevel% equ 0 (
    echo ✓ DAG runs status accessible
    if "%JQ_AVAILABLE%"=="true" (
        echo Recent DAG runs:
        jq -r ".dag_runs[0:3][] | .dag_run_id + \" - \" + .state" temp_runs.json 2>nul
    )
) else (
    echo WARNING: Could not fetch DAG runs status
)

REM Cleanup
del "%DAGS_RESPONSE%" 2>nul
del temp_dag_details.json 2>nul
del temp_tasks.json 2>nul
del temp_trigger.json 2>nul
del temp_run_response.json 2>nul
del temp_runs.json 2>nul

echo.
echo ========================================
echo ✓ DAG tests completed!
echo ========================================
echo.
echo Test Results:
echo   - DAG Files: Present
echo   - DAG Loading: Working
echo   - DAG Details: Accessible
echo   - DAG Tasks: Accessible
echo   - DAG Execution: Triggered
echo.
echo Web UI Access:
echo   http://localhost:8080/dags
echo   Username: admin
echo   Password: admin
echo.
echo To check DAG run status in detail:
echo   curl -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns

pause