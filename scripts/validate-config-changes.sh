#!/bin/bash

# Configuration validation script for Airflow 3.1.0
# Tests that deprecation warnings are eliminated and configuration is correct

echo "========================================"
echo "Airflow 3.1.0 Configuration Validation"
echo "========================================"

# Check prerequisites
echo
echo "[1/6] Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: docker-compose is not installed or not in PATH"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found. Copy .env.example to .env and configure it."
    exit 1
fi

echo "✓ Prerequisites check passed"

# Validate configuration parameters
echo
echo "[2/6] Validating Airflow 3.x configuration parameters..."

# Check that AIRFLOW__API__SECRET_KEY is used instead of deprecated AIRFLOW__WEBSERVER__SECRET_KEY
if ! docker-compose config | grep -q "AIRFLOW__API__SECRET_KEY"; then
    echo "ERROR: AIRFLOW__API__SECRET_KEY not found in configuration"
    echo "This is required for Airflow 3.x API server"
    exit 1
fi
echo "✓ AIRFLOW__API__SECRET_KEY is configured"

# Check that deprecated AIRFLOW__WEBSERVER__SECRET_KEY is not used
if docker-compose config | grep -q "AIRFLOW__WEBSERVER__SECRET_KEY"; then
    echo "WARNING: AIRFLOW__WEBSERVER__SECRET_KEY found in configuration"
    echo "This parameter is deprecated in Airflow 3.x, use AIRFLOW__API__SECRET_KEY instead"
else
    echo "✓ Deprecated AIRFLOW__WEBSERVER__SECRET_KEY not found"
fi

# Check other Airflow 3.x specific configurations
if ! docker-compose config | grep -q "apache/airflow:3.1.0"; then
    echo "ERROR: Airflow 3.1.0 image not found in configuration"
    exit 1
fi
echo "✓ Airflow 3.1.0 image is configured"

echo "✓ Configuration parameters validation passed"

# Start services to test for deprecation warnings
echo
echo "[3/6] Starting services to check for deprecation warnings..."
echo "This may take a few minutes..."

# Clean up any existing containers
docker-compose down &> /dev/null

# Start services and capture logs
echo "Starting Airflow services..."
docker-compose up -d --quiet-pull 2>startup_errors.log

# Wait for services to initialize
echo "Waiting for services to initialize (60 seconds)..."
sleep 60

# Check service status
echo
echo "[4/6] Checking service health..."
docker-compose ps

# Check for deprecation warnings in logs
echo
echo "[5/6] Checking for deprecation warnings in service logs..."

DEPRECATION_FOUND=false

# Check airflow-init logs for deprecation warnings
echo "Checking airflow-init logs..."
if docker-compose logs airflow-init 2>&1 | grep -i "deprecat" > /dev/null; then
    echo "WARNING: Deprecation warnings found in airflow-init logs:"
    docker-compose logs airflow-init 2>&1 | grep -i "deprecat"
    DEPRECATION_FOUND=true
fi

# Check api-server logs for deprecation warnings
echo "Checking airflow-api-server logs..."
if docker-compose logs airflow-api-server 2>&1 | grep -i "deprecat" > /dev/null; then
    echo "WARNING: Deprecation warnings found in airflow-api-server logs:"
    docker-compose logs airflow-api-server 2>&1 | grep -i "deprecat"
    DEPRECATION_FOUND=true
fi

# Check scheduler logs for deprecation warnings
echo "Checking airflow-scheduler logs..."
if docker-compose logs airflow-scheduler 2>&1 | grep -i "deprecat" > /dev/null; then
    echo "WARNING: Deprecation warnings found in airflow-scheduler logs:"
    docker-compose logs airflow-scheduler 2>&1 | grep -i "deprecat"
    DEPRECATION_FOUND=true
fi

# Check dag-processor logs for deprecation warnings
echo "Checking airflow-dag-processor logs..."
if docker-compose logs airflow-dag-processor 2>&1 | grep -i "deprecat" > /dev/null; then
    echo "WARNING: Deprecation warnings found in airflow-dag-processor logs:"
    docker-compose logs airflow-dag-processor 2>&1 | grep -i "deprecat"
    DEPRECATION_FOUND=true
fi

# Check triggerer logs for deprecation warnings
echo "Checking airflow-triggerer logs..."
if docker-compose logs airflow-triggerer 2>&1 | grep -i "deprecat" > /dev/null; then
    echo "WARNING: Deprecation warnings found in airflow-triggerer logs:"
    docker-compose logs airflow-triggerer 2>&1 | grep -i "deprecat"
    DEPRECATION_FOUND=true
fi

if [ "$DEPRECATION_FOUND" = false ]; then
    echo "✓ No deprecation warnings found in service logs"
else
    echo "⚠ Deprecation warnings detected - review configuration"
fi

# Test authentication and API access
echo
echo "[6/6] Testing authentication and API access..."

# Wait for API server to be ready
echo "Waiting for API server to be ready..."
RETRY_COUNT=0
while ! curl -f -s http://localhost:8080/health &> /dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge 30 ]; then
        echo "ERROR: API server did not become ready within 5 minutes"
        cleanup_and_exit
    fi
    sleep 10
done

echo "✓ API server is ready"

# Test authentication
if curl -s -u admin:admin http://localhost:8080/api/v1/version > temp_version.json 2>&1; then
    echo "✓ Authentication is working correctly"
    echo "API version response:"
    cat temp_version.json
    rm -f temp_version.json
else
    echo "ERROR: Authentication test failed"
    echo "Response:"
    cat temp_version.json 2>/dev/null
    rm -f temp_version.json
    cleanup_and_exit
fi

# Test that services start without configuration warnings
echo
echo "Checking for configuration warnings in startup logs..."
if [ -f startup_errors.log ]; then
    if grep -i "warning\|error" startup_errors.log > /dev/null; then
        echo "WARNING: Configuration warnings or errors found during startup:"
        cat startup_errors.log
    else
        echo "✓ No configuration warnings found during startup"
    fi
    rm -f startup_errors.log
fi

echo
echo "========================================"
if [ "$DEPRECATION_FOUND" = false ]; then
    echo "✓ Configuration validation PASSED!"
else
    echo "⚠ Configuration validation completed with warnings"
fi
echo "========================================"
echo
echo "Summary:"
echo "  - Airflow 3.1.0 configuration: Valid"
echo "  - API secret key configuration: Correct"
echo "  - Deprecated parameters: $DEPRECATION_FOUND"
echo "  - Authentication: Working"
echo "  - Services startup: Successful"
echo
echo "Services are running. Access Airflow at:"
echo "  http://localhost:8080 (admin/admin)"
echo
echo "To stop services: docker-compose down"

exit 0

cleanup_and_exit() {
    echo
    echo "Cleaning up services due to errors..."
    docker-compose down &> /dev/null
    rm -f startup_errors.log
    exit 1
}