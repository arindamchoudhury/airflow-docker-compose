# Airflow 3.1.0 Docker Compose Design Document

## Overview

This design document outlines the architecture and implementation approach for a Docker Compose setup that provides Apache Airflow 3.1.0 for local development on Windows 11. The solution leverages Airflow's new 3.x architecture with separate services for optimal performance and follows official best practices.

## Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "Docker Compose Environment"
        subgraph "Airflow Services"
            API[airflow-api-server<br/>Port 8080]
            SCHED[airflow-scheduler]
            DAG[airflow-dag-processor]
            TRIG[airflow-triggerer]
            WORKER[airflow-worker<br/>Optional]
            INIT[airflow-init<br/>One-time]
        end
        
        subgraph "Infrastructure"
            PG[(postgres<br/>Database)]
            REDIS[(redis<br/>Message Broker)]
        end
        
        subgraph "Monitoring (Optional)"
            FLOWER[flower<br/>Port 5555]
        end
        
        subgraph "Host Volumes"
            DAGS[./dags]
            LOGS[./logs]
            PLUGINS[./plugins]
            CONFIG[./config]
        end
    end
    
    DEV[Developer] --> API
    API --> PG
    SCHED --> PG
    SCHED --> REDIS
    DAG --> PG
    TRIG --> PG
    WORKER --> REDIS
    WORKER --> PG
    FLOWER --> REDIS
    
    DAGS --> API
    DAGS --> SCHED
    DAGS --> DAG
    LOGS --> API
    LOGS --> SCHED
    PLUGINS --> API
    CONFIG --> API
```

### Service Architecture

The design follows Airflow 3.1.0's microservices architecture with clear separation of concerns:

1. **API Server**: Handles web UI and REST API requests (replaces webserver)
2. **Scheduler**: Monitors DAGs and schedules task instances
3. **DAG Processor**: Parses DAG files independently from scheduler
4. **Triggerer**: Handles deferred and asynchronous tasks
5. **Worker**: Executes tasks (optional, for CeleryExecutor)
6. **Init Service**: One-time database initialization and setup

## Components and Interfaces

### Core Services Configuration

#### 1. Airflow API Server
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `api-server`
- **Ports**: `8080:8080`
- **Purpose**: Serves web UI and REST API
- **Dependencies**: postgres, airflow-init

#### 2. Airflow Scheduler
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `scheduler`
- **Purpose**: Schedules and monitors task execution
- **Dependencies**: postgres, redis (if CeleryExecutor), airflow-init

#### 3. Airflow DAG Processor
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `dag-processor`
- **Purpose**: Parses DAG files and updates metadata
- **Dependencies**: postgres, airflow-init

#### 4. Airflow Triggerer
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `triggerer`
- **Purpose**: Handles deferred tasks and async operations
- **Dependencies**: postgres, airflow-init

#### 5. Airflow Worker (Optional)
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `celery worker`
- **Purpose**: Executes tasks in CeleryExecutor mode
- **Dependencies**: postgres, redis, airflow-init

#### 6. Airflow Init
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: Custom initialization script
- **Purpose**: Database migration and initial setup
- **Dependencies**: postgres

### Infrastructure Services

#### PostgreSQL Database
- **Image**: `postgres:17`
- **Port**: `5432` (internal)
- **Environment**:
  - `POSTGRES_USER`: airflow
  - `POSTGRES_PASSWORD`: airflow
  - `POSTGRES_DB`: airflow
- **Volume**: `postgres-db-volume:/var/lib/postgresql/data`

#### Redis (Optional - CeleryExecutor only)
- **Image**: `redis:8-alpine`
- **Port**: `6379` (internal)
- **Command**: `redis-server --requirepass airflow`

#### Flower (Optional - CeleryExecutor monitoring)
- **Image**: `apache/airflow:3.1.0-python3.13`
- **Command**: `celery flower`
- **Port**: `5555:5555`
- **Profile**: `flower` (optional service)

### Volume Mounts

#### Host Directory Structure
```
./
├── dags/           # DAG files
├── logs/           # Airflow logs
├── plugins/        # Custom plugins
├── config/         # Configuration files
└── docker-compose.yml
```

#### Volume Mappings (Windows-compatible)
- `./dags:/opt/airflow/dags`
- `./logs:/opt/airflow/logs`
- `./plugins:/opt/airflow/plugins`
- `./config:/opt/airflow/config`

## Data Models

### Environment Variables Configuration

#### Core Airflow Configuration
```yaml
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor  # or CeleryExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
  AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: true
  AIRFLOW__CORE__LOAD_EXAMPLES: false
  AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
  AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: true
```

#### Authentication Configuration (Simple Auth - Default)
```yaml
environment:
  AIRFLOW__CORE__AUTH_MANAGER: airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager
  AIRFLOW__SIMPLE_AUTH_MANAGER__USERS_FILE: /opt/airflow/config/users.json
```



#### CeleryExecutor Configuration (Optional)
```yaml
environment:
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor
  AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
  AIRFLOW__CELERY__BROKER_URL: redis://:airflow@redis:6379/0
```

### User Configuration Files

#### Simple Auth Users File (config/users.json)
```json
{
  "admin": {
    "password": "admin",
    "role": "Admin"
  }
}
```

## Error Handling

### Database Migration Error Handling
- **Retry Logic**: Implement retry mechanism for database connections
- **Validation**: Check database connectivity before migration
- **Logging**: Comprehensive logging for troubleshooting
- **Rollback**: Support for migration rollback if needed

### Service Health Checks
```yaml
healthcheck:
  test: ["CMD-SHELL", "airflow jobs check --job-type SchedulerJob --hostname \"$${HOSTNAME}\""]
  interval: 30s
  timeout: 10s
  retries: 5
  start_period: 30s
```

### Startup Dependencies
- Use `depends_on` with `condition: service_healthy` for proper startup order
- Implement wait scripts for database readiness
- Handle service restart scenarios gracefully

## Testing Strategy

### Local Development Testing
1. **Service Startup**: Verify all services start correctly
2. **Web UI Access**: Confirm UI accessibility at http://localhost:8080
3. **DAG Loading**: Test DAG file detection and parsing
4. **Task Execution**: Validate task execution with sample DAGs
5. **Authentication**: Test login with configured credentials

### Integration Testing
1. **Database Connectivity**: Verify PostgreSQL connection and schema
2. **Volume Mounts**: Confirm file persistence across container restarts
3. **Service Communication**: Test inter-service communication
4. **Health Checks**: Validate service health monitoring

### Windows-Specific Testing
1. **Volume Mount Compatibility**: Test Windows path handling
2. **Line Ending Handling**: Verify CRLF/LF compatibility
3. **Docker Desktop Integration**: Ensure compatibility with Docker Desktop
4. **Performance**: Monitor resource usage on Windows

### Executor Testing
1. **LocalExecutor**: Test single-node task execution
2. **CeleryExecutor**: Test distributed task execution (optional)
3. **Task Scaling**: Verify task parallelization
4. **Error Handling**: Test task failure scenarios

### Configuration Testing
1. **Environment Variables**: Test configuration overrides
2. **Authentication Methods**: Test Simple Auth Manager configuration
3. **Custom Plugins**: Verify plugin loading
4. **Configuration Files**: Test custom airflow.cfg settings

## Implementation Considerations

### Windows 11 Compatibility
- Use forward slashes in volume paths for cross-platform compatibility
- Set appropriate file permissions for shared volumes
- Handle Windows-specific Docker Desktop networking
- Provide Windows-specific documentation

### Performance Optimization
- Use specific Python version (3.13) for optimal performance
- Configure appropriate resource limits
- Optimize database connection pooling
- Use Redis persistence for CeleryExecutor reliability

### Security Considerations
- Use strong default passwords (change in production)
- Implement proper network isolation
- Secure volume mount permissions
- Enable authentication by default

### Scalability Design
- Support both LocalExecutor and CeleryExecutor
- Allow easy addition of worker nodes
- Implement proper load balancing for workers
- Support horizontal scaling of services

### Maintenance and Updates
- Use specific version tags for reproducibility
- Implement backup strategies for database
- Provide upgrade path documentation
- Support configuration management