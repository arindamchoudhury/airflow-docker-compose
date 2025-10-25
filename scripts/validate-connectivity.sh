#!/bin/bash

# Connectivity validation script for running Airflow services
# Tests if services are running and accessible

echo "========================================"
echo "Airflow Services Connectivity Test"
echo "========================================"

# Check if curl is available
if command -v curl &> /dev/null; then
    CURL_AVAILABLE=true
else
    echo "WARNING: curl not found. Some connectivity tests will be skipped."
    CURL_AVAILABLE=false
fi

# Check if services are running
echo
echo "[1/6] Checking if Docker containers are running..."
if ! docker-compose ps --services --filter "status=running" | grep -q "postgres"; then
    echo "ERROR: PostgreSQL service is not running"
    echo "Run: docker-compose up -d"
    exit 1
fi

if ! docker-compose ps --services --filter "status=running" | grep -q "airflow-api-server"; then
    echo "ERROR: Airflow API server is not running"
    echo "Run: docker-compose up -d"
    exit 1
fi

echo "✓ Core services are running"

# Test database connectivity
echo
echo "[2/6] Testing database connectivity..."
if ! docker-compose exec -T postgres pg_isready -U airflow &> /dev/null; then
    echo "ERROR: PostgreSQL database is not ready"
    exit 1
fi
echo "✓ Database is accessible"

# Test Airflow API server health
echo
echo "[3/6] Testing Airflow API server health..."
if [ "$CURL_AVAILABLE" = true ]; then
    # Wait a moment for service to be ready
    sleep 5
    if ! curl -f -s http://localhost:8080/health &> /dev/null; then
        echo "ERROR: Airflow API server health check failed"
        echo "Check if the service is fully started: docker-compose logs airflow-api-server"
        exit 1
    fi
    echo "✓ API server is healthy"
else
    echo "SKIPPED: curl not available"
fi

# Test web UI accessibility
echo
echo "[4/6] Testing web UI accessibility..."
if [ "$CURL_AVAILABLE" = true ]; then
    if ! curl -f -s http://localhost:8080/ &> /dev/null; then
        echo "ERROR: Airflow web UI is not accessible"
        echo "Check: http://localhost:8080"
        exit 1
    fi
    echo "✓ Web UI is accessible at http://localhost:8080"
else
    echo "SKIPPED: curl not available"
fi

# Check scheduler health
echo
echo "[5/6] Testing scheduler connectivity..."
if ! docker-compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname airflow-scheduler &> /dev/null; then
    echo "WARNING: Scheduler job check failed (may be starting up)"
else
    echo "✓ Scheduler is running"
fi

# Check if CeleryExecutor services are running (if profile is active)
echo
echo "[6/6] Checking optional services..."
if docker-compose ps --services --filter "status=running" | grep -q "redis"; then
    echo "✓ Redis service is running (CeleryExecutor mode)"
    
    if docker-compose ps --services --filter "status=running" | grep -q "airflow-worker"; then
        echo "✓ Airflow worker is running"
    else
        echo "WARNING: Airflow worker is not running"
    fi
    
    if docker-compose ps --services --filter "status=running" | grep -q "flower"; then
        echo "✓ Flower monitoring is running at http://localhost:5555"
    fi
else
    echo "✓ Running in LocalExecutor mode (Redis not needed)"
fi

echo
echo "========================================"
echo "✓ Connectivity tests completed!"
echo "========================================"
echo
echo "Access points:"
echo "  Web UI: http://localhost:8080"
echo "  Default credentials: admin / admin"
if docker-compose ps --services --filter "status=running" | grep -q "flower"; then
    echo "  Flower: http://localhost:5555"
fi
echo
echo "To test authentication and API access, use:"
echo "  scripts/test-auth.sh"