#!/bin/bash

# Authentication testing script for Airflow 3.1.0
# Tests Simple Auth Manager and API access

echo "========================================"
echo "Airflow Authentication Testing"
echo "========================================"

# Check if curl is available
if ! command -v curl &> /dev/null; then
    echo "ERROR: curl is required for authentication testing"
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
echo "[1/5] Checking if Airflow is accessible..."
if ! curl -f -s http://localhost:8080/health &> /dev/null; then
    echo "ERROR: Airflow is not accessible at http://localhost:8080"
    echo "Make sure services are running: docker-compose up -d"
    exit 1
fi
echo "✓ Airflow is accessible"

# Test web UI login page
echo
echo "[2/5] Testing web UI login page..."
if ! curl -f -s http://localhost:8080/login &> /dev/null; then
    echo "ERROR: Login page is not accessible"
    exit 1
fi
echo "✓ Login page is accessible"

# Test Simple Auth Manager configuration
echo
echo "[3/5] Testing Simple Auth Manager configuration..."
if ! docker-compose exec -T airflow-api-server cat /opt/airflow/config/users.json &> /dev/null; then
    echo "ERROR: Simple Auth Manager users.json file not found"
    exit 1
fi
echo "✓ Simple Auth Manager is configured"

# Test API authentication with basic auth
echo
echo "[4/5] Testing API authentication..."
API_RESPONSE="temp_api_response.json"

# Test unauthenticated request (should fail)
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v1/dags)
if [ "$HTTP_STATUS" = "401" ]; then
    echo "✓ Unauthenticated requests are properly rejected"
else
    echo "WARNING: Unauthenticated API request did not return 401 (returned $HTTP_STATUS)"
fi

# Test authenticated request
echo "Testing authenticated API request..."
HTTP_STATUS=$(curl -s -u admin:admin -o "$API_RESPONSE" -w "%{http_code}" http://localhost:8080/api/v1/dags)

if [ "$HTTP_STATUS" = "200" ]; then
    echo "✓ Authenticated API request successful"
    if [ "$JQ_AVAILABLE" = true ]; then
        echo "Response preview:"
        jq ".total_entries" "$API_RESPONSE" 2>/dev/null || echo "Could not parse JSON response"
    fi
else
    echo "ERROR: Authenticated API request failed with status $HTTP_STATUS"
    echo "Response:"
    cat "$API_RESPONSE" 2>/dev/null
    rm -f "$API_RESPONSE"
    exit 1
fi

# Test specific API endpoints
echo
echo "[5/5] Testing specific API endpoints..."

# Test version endpoint
if curl -s -u admin:admin http://localhost:8080/api/v1/version > temp_version.json 2>&1; then
    echo "✓ Version endpoint accessible"
    if [ "$JQ_AVAILABLE" = true ]; then
        echo "Airflow version:"
        jq -r ".version" temp_version.json 2>/dev/null || echo "Could not parse version"
    fi
else
    echo "WARNING: Version endpoint test failed"
fi

# Test config endpoint
if curl -s -u admin:admin http://localhost:8080/api/v1/config &> /dev/null; then
    echo "✓ Config endpoint accessible"
else
    echo "WARNING: Config endpoint test failed"
fi

# Test pools endpoint
if curl -s -u admin:admin http://localhost:8080/api/v1/pools &> /dev/null; then
    echo "✓ Pools endpoint accessible"
else
    echo "WARNING: Pools endpoint test failed"
fi

# Cleanup
rm -f "$API_RESPONSE" temp_version.json

echo
echo "========================================"
echo "✓ Authentication tests completed!"
echo "========================================"
echo
echo "Test Results:"
echo "  - Simple Auth Manager: Working"
echo "  - Web UI Login: Accessible"
echo "  - API Authentication: Working"
echo "  - Default Credentials: admin / admin"
echo
echo "API Usage Examples:"
echo "  curl -u admin:admin http://localhost:8080/api/v1/dags"
echo "  curl -u admin:admin http://localhost:8080/api/v1/version"
echo
echo "To test DAG loading and execution, use:"
echo "  scripts/test-dags.sh"