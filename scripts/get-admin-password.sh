#!/bin/bash

# Get the current admin password from Simple Auth Manager

echo "=== Airflow Admin Password ==="
echo

# Check if services are running
if ! docker-compose ps | grep -q "airflow-api-server.*Up"; then
    echo "ERROR: Airflow API server is not running"
    echo "Please start services with: docker-compose up -d"
    exit 1
fi

echo "Getting current admin password..."
echo

# Get the password from the Simple Auth Manager
ADMIN_PASSWORD=$(docker-compose exec -T airflow-api-server python -c "
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
auth_manager = SimpleAuthManager()
users = auth_manager.get_users()
passwords = auth_manager.get_passwords(users)
print(passwords['admin'])
" | tr -d '\r')

echo "=== Login Credentials ==="
echo "Username: admin"
echo "Password: $ADMIN_PASSWORD"
echo
echo "Web UI: http://localhost:8080"
echo
echo "=== Test Authentication ==="
echo "You can test authentication with:"
echo "curl -X POST http://localhost:8080/auth/token -H \"Content-Type: application/json\" -d '{\"username\": \"admin\", \"password\": \"$ADMIN_PASSWORD\"}'"