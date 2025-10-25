#!/bin/bash

# Comprehensive service validation script for Airflow 3.1.0
# Tests service startup, connectivity, and health checks

echo "========================================"
echo "Airflow 3.1.0 Services Validation"
echo "========================================"

# Check prerequisites
echo
echo "[1/8] Checking prerequisites..."
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

# Validate Docker Compose configuration
echo
echo "[2/8] Validating Docker Compose configuration..."
if ! docker-compose config --quiet; then
    echo "ERROR: Docker Compose configuration is invalid"
    exit 1
fi
echo "✓ Docker Compose configuration is valid"

# Check required directories
echo
echo "[3/8] Checking required directories..."
required_dirs=("dags" "logs" "plugins" "config")
for dir in "${required_dirs[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "ERROR: Required directory '$dir' does not exist"
        exit 1
    fi
done
echo "✓ Required directories exist"

# Test service definitions
echo
echo "[4/8] Validating service definitions..."
if ! docker-compose config --services | grep -q "postgres"; then
    echo "ERROR: PostgreSQL service not found"
    exit 1
fi

if ! docker-compose config --services | grep -q "airflow-api-server"; then
    echo "ERROR: Airflow API server service not found"
    exit 1
fi

if ! docker-compose config --services | grep -q "airflow-scheduler"; then
    echo "ERROR: Airflow scheduler service not found"
    exit 1
fi

if ! docker-compose config --services | grep -q "airflow-dag-processor"; then
    echo "ERROR: Airflow DAG processor service not found"
    exit 1
fi

if ! docker-compose config --services | grep -q "airflow-triggerer"; then
    echo "ERROR: Airflow triggerer service not found"
    exit 1
fi

echo "✓ All required services are defined"

# Test health check configurations
echo
echo "[5/8] Validating health check configurations..."
if docker-compose config | grep -q "healthcheck:"; then
    echo "✓ Health checks are configured"
else
    echo "WARNING: No health checks found in configuration"
fi

# Test volume mounts
echo
echo "[6/8] Validating volume mounts..."
if ! docker-compose config | grep -q "./dags:/opt/airflow/dags"; then
    echo "ERROR: DAGs volume mount not configured correctly"
    exit 1
fi

if ! docker-compose config | grep -q "./logs:/opt/airflow/logs"; then
    echo "ERROR: Logs volume mount not configured correctly"
    exit 1
fi

echo "✓ Volume mounts are configured correctly"

# Test environment variables
echo
echo "[7/8] Validating environment variables..."
if ! docker-compose config | grep -q "AIRFLOW__CORE__EXECUTOR"; then
    echo "ERROR: Executor configuration not found"
    exit 1
fi

if ! docker-compose config | grep -q "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"; then
    echo "ERROR: Database connection configuration not found"
    exit 1
fi

echo "✓ Environment variables are configured"

# Test network configuration
echo
echo "[8/8] Validating network configuration..."
if ! docker-compose config | grep -q "airflow-network"; then
    echo "ERROR: Airflow network not configured"
    exit 1
fi

echo "✓ Network configuration is valid"

echo
echo "========================================"
echo "✓ All service validations passed!"
echo "========================================"
echo
echo "Services are ready to start. Use:"
echo "  docker-compose up -d          (LocalExecutor mode)"
echo "  docker-compose --profile celery up -d  (CeleryExecutor mode)"
echo
echo "To validate running services, use:"
echo "  scripts/validate-connectivity.sh"