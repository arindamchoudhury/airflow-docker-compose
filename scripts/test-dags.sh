#!/bin/bash

# DAG testing script for Airflow 3.1.0
# Tests DAG loading, parsing, and execution

echo "========================================"
echo "Airflow DAG Testing"
echo "========================================"

# Check if curl is available
if ! command -v curl &> /dev/null; then
    echo "ERROR: curl is required for DAG testing"
    echo "Please install curl"
    exit 1
fi

# Check if jq is available for JSON parsing
if command -v jq &> /dev/null; then
    JQ_AVAILABLE=true
else
    echo "WARNING: jq not found. JSON responses will not be formatted."
    JQ_AVAILABLE=false
fi

# Test if Airflow is running
echo
echo "[1/6] Checking if Airflow is accessible..."
if ! curl -f -s http://localhost:8080/health &> /dev/null; then
    echo "ERROR: Airflow is not accessible at http://localhost:8080"
    echo "Make sure services are running: docker-compose up -d"
    exit 1
fi
echo "✓ Airflow is accessible"

# Check if DAG files exist
echo
echo "[2/6] Checking DAG files..."
if [ ! -f "dags/example_basic_dag.py" ]; then
    echo "ERROR: example_basic_dag.py not found in dags directory"
    exit 1
fi

if [ ! -f "dags/example_advanced_dag.py" ]; then
    echo "ERROR: example_advanced_dag.py not found in dags directory"
    exit 1
fi

echo "✓ DAG files are present"

# Test DAG loading via API
echo
echo "[3/6] Testing DAG loading via API..."
DAGS_RESPONSE="temp_dags.json"

if ! curl -s -u admin:admin -o "$DAGS_RESPONSE" http://localhost:8080/api/v1/dags; then
    echo "ERROR: Failed to fetch DAGs from API"
    exit 1
fi

# Check if our example DAGs are loaded
if ! grep -q "example_basic_dag" "$DAGS_RESPONSE"; then
    echo "ERROR: example_basic_dag not found in loaded DAGs"
    echo "Available DAGs:"
    if [ "$JQ_AVAILABLE" = true ]; then
        jq -r ".dags[].dag_id" "$DAGS_RESPONSE" 2>/dev/null || cat "$DAGS_RESPONSE"
    else
        cat "$DAGS_RESPONSE"
    fi
    rm -f "$DAGS_RESPONSE"
    exit 1
fi

if ! grep -q "example_advanced_dag" "$DAGS_RESPONSE"; then
    echo "ERROR: example_advanced_dag not found in loaded DAGs"
    rm -f "$DAGS_RESPONSE"
    exit 1
fi

echo "✓ Example DAGs are loaded successfully"

if [ "$JQ_AVAILABLE" = true ]; then
    echo "Loaded DAGs:"
    jq -r ".dags[].dag_id" "$DAGS_RESPONSE" 2>/dev/null
fi

# Test DAG details
echo
echo "[4/6] Testing DAG details..."
if ! curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag > temp_dag_details.json; then
    echo "ERROR: Failed to fetch DAG details"
    rm -f "$DAGS_RESPONSE"
    exit 1
fi

echo "✓ DAG details accessible"

if [ "$JQ_AVAILABLE" = true ]; then
    echo "Basic DAG info:"
    jq -r ".dag_id, .description, .is_paused" temp_dag_details.json 2>/dev/null
fi

# Test DAG tasks
echo
echo "[5/6] Testing DAG tasks..."
if ! curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/tasks > temp_tasks.json; then
    echo "ERROR: Failed to fetch DAG tasks"
    rm -f "$DAGS_RESPONSE" temp_dag_details.json
    exit 1
fi

echo "✓ DAG tasks accessible"

if [ "$JQ_AVAILABLE" = true ]; then
    echo "Basic DAG tasks:"
    jq -r ".tasks[].task_id" temp_tasks.json 2>/dev/null
fi

# Test DAG execution (trigger a DAG run)
echo
echo "[6/6] Testing DAG execution..."
echo "Triggering example_basic_dag..."

# Create a simple trigger request
echo '{"conf": {}}' > temp_trigger.json

if ! curl -s -u admin:admin -X POST -H "Content-Type: application/json" -d @temp_trigger.json http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns > temp_run_response.json; then
    echo "ERROR: Failed to trigger DAG run"
    rm -f temp_trigger.json "$DAGS_RESPONSE" temp_dag_details.json temp_tasks.json
    exit 1
fi

echo "✓ DAG run triggered successfully"

if [ "$JQ_AVAILABLE" = true ]; then
    echo "DAG run info:"
    jq -r ".dag_run_id, .state, .execution_date" temp_run_response.json 2>/dev/null
fi

# Wait a moment and check DAG run status
echo "Waiting 10 seconds for DAG run to start..."
sleep 10

if curl -s -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns > temp_runs.json; then
    echo "✓ DAG runs status accessible"
    if [ "$JQ_AVAILABLE" = true ]; then
        echo "Recent DAG runs:"
        jq -r ".dag_runs[0:3][] | .dag_run_id + \" - \" + .state" temp_runs.json 2>/dev/null
    fi
else
    echo "WARNING: Could not fetch DAG runs status"
fi

# Cleanup
rm -f "$DAGS_RESPONSE" temp_dag_details.json temp_tasks.json temp_trigger.json temp_run_response.json temp_runs.json

echo
echo "========================================"
echo "✓ DAG tests completed!"
echo "========================================"
echo
echo "Test Results:"
echo "  - DAG Files: Present"
echo "  - DAG Loading: Working"
echo "  - DAG Details: Accessible"
echo "  - DAG Tasks: Accessible"
echo "  - DAG Execution: Triggered"
echo
echo "Web UI Access:"
echo "  http://localhost:8080/dags"
echo "  Username: admin"
echo "  Password: admin"
echo
echo "To check DAG run status in detail:"
echo "  curl -u admin:admin http://localhost:8080/api/v1/dags/example_basic_dag/dagRuns"