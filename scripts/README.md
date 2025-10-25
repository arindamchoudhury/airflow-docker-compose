# Airflow 3.1.0 Validation and Testing Scripts

This directory contains comprehensive validation and testing scripts for the Airflow 3.1.0 Docker Compose setup.

## Overview

The validation scripts test different aspects of the Airflow setup:

1. **Service Configuration** - Validates Docker Compose configuration and prerequisites
2. **Service Connectivity** - Tests if running services are accessible and healthy
3. **Authentication** - Tests Simple Auth Manager and API access
4. **DAG Loading & Execution** - Tests DAG parsing, loading, and execution
5. **Worker Validation** - Tests CeleryExecutor worker configuration (optional)

## Scripts Description

### Core Validation Scripts

| Script | Windows | Linux/Mac | Description |
|--------|---------|-----------|-------------|
| Service Validation | `validate-services.bat` | `validate-services.sh` | Validates Docker Compose configuration, directories, and service definitions |
| Connectivity Testing | `validate-connectivity.bat` | `validate-connectivity.sh` | Tests running services connectivity and health checks |
| Authentication Testing | `test-auth.bat` | `test-auth.sh` | Tests Simple Auth Manager, web UI login, and API authentication |
| DAG Testing | `test-dags.bat` | `test-dags.sh` | Tests DAG loading, parsing, and execution |
| Worker Validation | `validate-worker.bat` | `validate-worker.sh` | Tests CeleryExecutor worker configuration |

### Comprehensive Test Runner

| Script | Windows | Linux/Mac | Description |
|--------|---------|-----------|-------------|
| All Tests | `run-all-tests.bat` | `run-all-tests.sh` | Runs all validation scripts in sequence |

### Legacy Scripts

| Script | Description |
|--------|-------------|
| `validate-init.bat/sh` | Validates airflow-init service configuration |

## Usage

### Prerequisites

Before running any validation scripts, ensure:

1. **Docker Desktop** is installed and running
2. **docker-compose** is available in PATH
3. **.env file** exists (copy from .env.example and configure)
4. **Required directories** exist: `dags/`, `logs/`, `plugins/`, `config/`

### Quick Start - Run All Tests

**Windows:**
```cmd
scripts\run-all-tests.bat
```

**Linux/Mac:**
```bash
scripts/run-all-tests.sh
```

This will run all validation tests in sequence and provide a comprehensive report.

### Individual Test Scripts

#### 1. Service Configuration Validation

Tests Docker Compose configuration without starting services.

**Windows:**
```cmd
scripts\validate-services.bat
```

**Linux/Mac:**
```bash
scripts/validate-services.sh
```

**What it tests:**
- Docker and docker-compose availability
- .env file existence
- Docker Compose configuration validity
- Required directories existence
- Service definitions
- Volume mounts configuration
- Environment variables
- Network configuration

#### 2. Service Connectivity Testing

Tests connectivity to running services (requires services to be started).

**Windows:**
```cmd
scripts\validate-connectivity.bat
```

**Linux/Mac:**
```bash
scripts/validate-connectivity.sh
```

**What it tests:**
- Docker containers are running
- Database connectivity
- API server health endpoint
- Web UI accessibility
- Scheduler connectivity
- Optional services (Redis, Worker, Flower)

#### 3. Authentication Testing

Tests authentication methods and API access.

**Windows:**
```cmd
scripts\test-auth.bat
```

**Linux/Mac:**
```bash
scripts/test-auth.sh
```

**What it tests:**
- Web UI login page accessibility
- Simple Auth Manager configuration
- Unauthenticated request rejection
- Authenticated API requests
- Specific API endpoints (version, config, pools)

#### 4. DAG Testing

Tests DAG loading, parsing, and execution.

**Windows:**
```cmd
scripts\test-dags.bat
```

**Linux/Mac:**
```bash
scripts/test-dags.sh
```

**What it tests:**
- DAG files existence
- DAG loading via API
- DAG details accessibility
- DAG tasks accessibility
- DAG execution (triggers a test run)

#### 5. Worker Validation

Tests CeleryExecutor worker configuration (optional).

**Windows:**
```cmd
scripts\validate-worker.bat
```

**Linux/Mac:**
```bash
scripts/validate-worker.sh
```

**What it tests:**
- Redis service configuration
- Worker service configuration
- CeleryExecutor configuration
- Broker URL configuration

## Requirements Coverage

These scripts fulfill the following requirements from the specification:

- **Requirement 1.1**: Tests Docker Compose system startup and service accessibility
- **Requirement 1.3**: Tests API server exposure on port 8080
- **Requirement 3.1**: Tests authentication configuration and API access
- **Requirement 6.1**: Tests Simple Auth Manager configuration and JWT token support

## Dependencies

### Required Tools

- **Docker Desktop** - Container runtime
- **docker-compose** - Multi-container orchestration
- **curl** - HTTP client for API testing (required for connectivity and auth tests)

### Optional Tools

- **jq** - JSON processor for formatted output (improves readability)

### Windows-Specific Notes

- Scripts use `.bat` extension for Windows compatibility
- Uses `findstr` instead of `grep` for text searching
- Uses `timeout` instead of `sleep` for delays
- Handles Windows path separators appropriately

### Linux/Mac-Specific Notes

- Scripts use `.sh` extension
- Uses standard Unix tools (`grep`, `sleep`, etc.)
- Scripts should be executable (`chmod +x scripts/*.sh`)

## Troubleshooting

### Common Issues

1. **Docker not running**
   - Start Docker Desktop
   - Verify with: `docker --version`

2. **Services not starting**
   - Check .env file configuration
   - Verify required directories exist
   - Check logs: `docker-compose logs`

3. **Authentication failures**
   - Verify Simple Auth Manager configuration
   - Check users.json file in config directory
   - Ensure default credentials: admin/admin

4. **DAG loading issues**
   - Check DAG files syntax
   - Verify volume mounts
   - Check DAG processor logs: `docker-compose logs airflow-dag-processor`

5. **API connectivity issues**
   - Ensure API server is fully started
   - Check health endpoint: `curl http://localhost:8080/health`
   - Verify port 8080 is not blocked

### Log Analysis

To debug issues, check service logs:

```bash
# All services
docker-compose logs

# Specific service
docker-compose logs airflow-api-server
docker-compose logs airflow-scheduler
docker-compose logs postgres
```

### Service Status

Check service status:

```bash
# Running services
docker-compose ps

# Service health
docker-compose ps --services --filter "status=running"
```

## Output Examples

### Successful Test Run

```
========================================
✅ ALL TESTS PASSED!
========================================

Your Airflow 3.1.0 setup is working correctly!

Access Points:
  Web UI: http://localhost:8080
  Username: admin
  Password: admin

To stop services: docker-compose down
To view logs: docker-compose logs [service-name]
```

### Failed Test Example

```
========================================
❌ TEST SUITE FAILED
========================================
One or more tests failed. Please check the output above.

Common troubleshooting steps:
1. Ensure Docker Desktop is running
2. Check .env file configuration
3. Verify all required directories exist
4. Check Docker container logs: docker-compose logs
```

## Integration with CI/CD

These scripts can be integrated into CI/CD pipelines for automated testing:

```yaml
# Example GitHub Actions step
- name: Run Airflow Validation Tests
  run: |
    cp .env.example .env
    # Configure .env with test values
    scripts/run-all-tests.sh
```

## Contributing

When adding new validation scripts:

1. Create both Windows (.bat) and Linux/Mac (.sh) versions
2. Follow the existing naming convention
3. Include comprehensive error handling
4. Add appropriate documentation
5. Update this README with the new script information