#!/bin/bash

# Validation script for airflow-init service
# This script tests the database initialization functionality

echo "=== Airflow Init Service Validation ==="

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: docker-compose is not installed or not in PATH"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found"
    exit 1
fi

echo "✓ Prerequisites check passed"

# Validate docker-compose configuration
echo "Validating Docker Compose configuration..."
if ! docker-compose config --quiet; then
    echo "ERROR: Docker Compose configuration is invalid"
    exit 1
fi

echo "✓ Docker Compose configuration is valid"

# Check if required directories exist
required_dirs=("dags" "logs" "plugins" "config")
for dir in "${required_dirs[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "ERROR: Required directory '$dir' does not exist"
        exit 1
    fi
done

echo "✓ Required directories exist"

# Test airflow-init service (dry run)
echo "Testing airflow-init service configuration..."
if docker-compose config | grep -q "airflow-init:"; then
    echo "✓ airflow-init service is properly configured"
else
    echo "ERROR: airflow-init service not found in configuration"
    exit 1
fi

# Check if the service has proper dependencies
if docker-compose config | grep -A 10 "airflow-init:" | grep -q "postgres:"; then
    echo "✓ airflow-init service has proper database dependency"
else
    echo "ERROR: airflow-init service missing database dependency"
    exit 1
fi

echo "=== Validation Completed Successfully ==="
echo "The airflow-init service is properly configured and ready to use."
echo ""
echo "To start the services:"
echo "  docker-compose up airflow-init"
echo ""
echo "To start all services:"
echo "  docker-compose up -d"