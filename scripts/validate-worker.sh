#!/bin/bash

# Validation script for Airflow Worker service configuration
echo "========================================"
echo "Airflow Worker Service Validation"
echo "========================================"

echo
echo "Checking Docker Compose configuration..."
if ! docker-compose config --quiet; then
    echo "ERROR: Docker Compose configuration is invalid!"
    exit 1
fi
echo "✓ Docker Compose configuration is valid"

echo
echo "Checking if Redis service is configured..."
if ! docker-compose config | grep -q "redis:"; then
    echo "ERROR: Redis service not found in configuration!"
    exit 1
fi
echo "✓ Redis service is configured"

echo
echo "Checking if Worker service is configured with celery profile..."
if ! docker-compose --profile celery config --services | grep -q "airflow-worker"; then
    echo "ERROR: Airflow Worker service not found in celery profile!"
    exit 1
fi
echo "✓ Airflow Worker service is configured with celery profile"

echo
echo "Checking CeleryExecutor configuration..."
if ! docker-compose --profile celery config | grep -q "AIRFLOW__CORE__EXECUTOR: CeleryExecutor"; then
    echo "ERROR: CeleryExecutor not configured in worker service!"
    exit 1
fi
echo "✓ CeleryExecutor is configured"

echo
echo "Checking Redis broker URL configuration..."
if ! docker-compose --profile celery config | grep -q "AIRFLOW__CELERY__BROKER_URL"; then
    echo "ERROR: Redis broker URL not configured!"
    exit 1
fi
echo "✓ Redis broker URL is configured"

echo
echo "========================================"
echo "✓ All Worker service validations passed!"
echo "========================================"
echo
echo "To start with CeleryExecutor mode, run:"
echo "docker-compose --profile celery up -d"
echo
echo "To start with LocalExecutor mode (default), run:"
echo "docker-compose up -d"