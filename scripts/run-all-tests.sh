#!/bin/bash

# Comprehensive test runner for Airflow 3.1.0 validation
# Runs all validation and testing scripts in sequence

echo "========================================"
echo "Airflow 3.1.0 Comprehensive Test Suite"
echo "========================================"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_FAILED=false

echo "Starting comprehensive validation of Airflow 3.1.0 setup..."
echo

# Test 1: Service Configuration Validation
echo "========================================"
echo "TEST 1: Service Configuration Validation"
echo "========================================"
if ! bash "$SCRIPT_DIR/validate-services.sh"; then
    echo "ERROR: Service validation failed"
    TEST_FAILED=true
    exit 1
fi
echo

# Test 2: Service Connectivity (requires running services)
echo "========================================"
echo "TEST 2: Service Connectivity Testing"
echo "========================================"
echo "Checking if services are running..."
if ! docker-compose ps --services --filter "status=running" | grep -q "airflow-api-server"; then
    echo "Services are not running. Starting services..."
    echo "This may take a few minutes..."
    docker-compose up -d
    
    # Wait for services to be ready
    echo "Waiting for services to start up..."
    sleep 60
    
    # Check again
    if ! docker-compose ps --services --filter "status=running" | grep -q "airflow-api-server"; then
        echo "ERROR: Failed to start services"
        TEST_FAILED=true
        exit 1
    fi
fi

if ! bash "$SCRIPT_DIR/validate-connectivity.sh"; then
    echo "ERROR: Connectivity validation failed"
    TEST_FAILED=true
    exit 1
fi
echo

# Test 3: Authentication Testing
echo "========================================"
echo "TEST 3: Authentication Testing"
echo "========================================"
if ! bash "$SCRIPT_DIR/test-auth.sh"; then
    echo "ERROR: Authentication testing failed"
    TEST_FAILED=true
    exit 1
fi
echo

# Test 4: DAG Loading and Execution Testing
echo "========================================"
echo "TEST 4: DAG Loading and Execution Testing"
echo "========================================"
if ! bash "$SCRIPT_DIR/test-dags.sh"; then
    echo "ERROR: DAG testing failed"
    TEST_FAILED=true
    exit 1
fi
echo

# Test 5: Worker Validation (if CeleryExecutor is configured)
echo "========================================"
echo "TEST 5: Worker Validation (Optional)"
echo "========================================"
if docker-compose ps --services --filter "status=running" | grep -q "redis"; then
    echo "CeleryExecutor mode detected, running worker validation..."
    if ! bash "$SCRIPT_DIR/validate-worker.sh" 2>/dev/null; then
        echo "WARNING: Worker validation failed (non-critical)"
    fi
else
    echo "LocalExecutor mode detected, skipping worker validation"
fi
echo

if [ "$TEST_FAILED" = true ]; then
    echo "========================================"
    echo "❌ TEST SUITE FAILED"
    echo "========================================"
    echo "One or more tests failed. Please check the output above."
    echo
    echo "Common troubleshooting steps:"
    echo "1. Ensure Docker Desktop is running"
    echo "2. Check .env file configuration"
    echo "3. Verify all required directories exist"
    echo "4. Check Docker container logs: docker-compose logs"
    echo
    exit 1
else
    echo "========================================"
    echo "✅ ALL TESTS PASSED!"
    echo "========================================"
    echo
    echo "Your Airflow 3.1.0 setup is working correctly!"
    echo
    echo "Access Points:"
    echo "  Web UI: http://localhost:8080"
    echo "  Username: admin"
    echo "  Password: admin"
    echo
    if docker-compose ps --services --filter "status=running" | grep -q "flower"; then
        echo "  Flower: http://localhost:5555"
    fi
    echo
    echo "To stop services: docker-compose down"
    echo "To view logs: docker-compose logs [service-name]"
fi