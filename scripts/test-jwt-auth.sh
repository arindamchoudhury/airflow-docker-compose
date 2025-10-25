#!/bin/bash

# Test JWT Authentication Configuration
# This script validates that JWT authentication is working correctly

echo "=== Testing JWT Authentication Configuration ==="
echo

# Function to check if services are running
check_services() {
    echo "[1/5] Checking if Airflow services are running..."
    
    if ! docker-compose ps | grep -q "airflow-api-server.*Up"; then
        echo "ERROR: Airflow API server is not running"
        echo "Please start services with: docker-compose up -d"
        exit 1
    fi
    
    echo "✓ Airflow API server is running"
}

# Function to test API health endpoint
test_api_health() {
    echo "[2/5] Testing API health endpoint..."
    
    # Test from inside the container since external access might have issues
    health_response=$(docker-compose exec -T airflow-api-server curl -s http://localhost:8080/api/v2/monitor/health 2>/dev/null)
    
    if echo "$health_response" | grep -q "healthy"; then
        echo "✓ API health endpoint is accessible"
    else
        echo "ERROR: API health endpoint is not accessible"
        echo "Check if the API server is properly started"
        exit 1
    fi
}

# Function to test authentication endpoint
test_auth_endpoint() {
    echo "[3/5] Testing authentication endpoint..."
    
    # Test login page accessibility from inside container
    auth_code=$(docker-compose exec -T airflow-api-server curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/auth/login 2>/dev/null)
    
    if [ "$auth_code" = "200" ]; then
        echo "✓ Authentication login page is accessible"
    else
        echo "ERROR: Authentication login page is not accessible (HTTP $auth_code)"
        exit 1
    fi
}

# Function to test JWT token generation
test_jwt_token() {
    echo "[4/5] Testing JWT token generation..."
    
    # Get the actual password from Simple Auth Manager
    ACTUAL_PASSWORD=$(docker-compose exec -T airflow-api-server python -c "
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
auth_manager = SimpleAuthManager()
users = auth_manager.get_users()
passwords = auth_manager.get_passwords(users)
print(passwords['admin'])
" 2>/dev/null | tr -d '\r')
    
    echo "Using password from Simple Auth Manager: $ACTUAL_PASSWORD"
    
    # Attempt to get a JWT token using the actual generated credentials
    response=$(docker-compose exec -T airflow-api-server curl -s -X POST http://localhost:8080/auth/token \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"admin\", \"password\": \"$ACTUAL_PASSWORD\"}")
    
    if echo "$response" | grep -q "access_token"; then
        echo "✓ JWT token generation is working"
        echo "Token response: $response"
    else
        echo "WARNING: JWT token generation may have issues"
        echo "Response: $response"
    fi
}

# Function to check configuration
check_configuration() {
    echo "[5/5] Checking configuration parameters..."
    
    # Check that API secret key is configured
    if docker-compose config | grep -q "AIRFLOW__API__SECRET_KEY"; then
        echo "✓ AIRFLOW__API__SECRET_KEY is configured"
    else
        echo "ERROR: AIRFLOW__API__SECRET_KEY is not configured"
        exit 1
    fi
    
    # Check that Simple Auth Manager is configured
    if docker-compose config | grep -q "AIRFLOW__CORE__AUTH_MANAGER.*SimpleAuthManager"; then
        echo "✓ Simple Auth Manager is configured"
    else
        echo "WARNING: Simple Auth Manager configuration not found"
    fi
    
    # Check that JWT secret key is configured
    if docker-compose config | grep -q "AIRFLOW__SIMPLE_AUTH_MANAGER__JWT_SECRET_KEY"; then
        echo "✓ JWT secret key is configured"
    else
        echo "WARNING: JWT secret key configuration not found"
    fi
    
    # Check for deprecated configurations
    if docker-compose config | grep -q "AIRFLOW__WEBSERVER__SECRET_KEY"; then
        echo "WARNING: Deprecated AIRFLOW__WEBSERVER__SECRET_KEY found"
    else
        echo "✓ No deprecated webserver secret key found"
    fi
}

# Function to check logs for JWT errors
check_logs() {
    echo
    echo "=== Checking recent logs for JWT-related errors ==="
    
    # Check API server logs for JWT errors
    jwt_errors=$(docker-compose logs airflow-api-server 2>/dev/null | grep -i "jwt\|token\|signature" | tail -5)
    
    if [ -n "$jwt_errors" ]; then
        echo "Recent JWT-related log entries:"
        echo "$jwt_errors"
    else
        echo "✓ No recent JWT-related errors found in logs"
    fi
}

# Main execution
main() {
    check_services
    test_api_health
    test_auth_endpoint
    test_jwt_token
    check_configuration
    check_logs
    
    echo
    echo "=== JWT Authentication Test Complete ==="
    echo "If you're still experiencing JWT token validation errors:"
    echo "1. Restart the services: docker-compose down && docker-compose up -d"
    echo "2. Check that the AIRFLOW_SECRET_KEY in .env is properly set"
    echo "3. Verify that config/users.json exists and contains valid user data"
    echo "4. Check the API server logs: docker-compose logs airflow-api-server"
}

# Run the main function
main