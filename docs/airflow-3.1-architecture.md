# Apache Airflow 3.1.0 Architecture and Configuration Guide

## Core Components Overview

Apache Airflow 3.1.0 consists of several key components that work together to manage and execute workflows:

1. **Webserver**
   - Provides the user interface
   - Handles API requests
   - Cannot execute DAG author code directly
   - Only executes code installed as packages/plugins by Deployment Manager
   - Authenticates users and manages permissions

2. **Scheduler**
   - Monitors and triggers task execution
   - Coordinates with the metadata database
   - Can run multiple instances for high availability
   - Manages task instance states
   - Uses Task Execution API for improved isolation

3. **Metadata Database**
   - Central source of truth
   - Stores:
     - DAG definitions
     - Task instance states
     - Variables and connections
     - User information and permissions
     - Configuration settings
     - Execution history

4. **DAG Processor**
   - Separate process from Scheduler (standard in 3.x)
   - Parses DAG files
   - Communicates via Task Execution API
   - Provides security isolation

5. **Workers**
   - Execute tasks
   - Decoupled from metadata DB
   - Communicate via REST API
   - Can be scaled independently

## Component Connectivity

### Authentication Flow

1. **API Authentication**
   ```
   Client → Webserver → Auth Manager → JWT Token
   ```
   - Default: Token-based authentication
   - JWT tokens required for API interactions
   - Tokens obtained via POST to `/auth/token`

2. **Component Communication**
   ```
   Webserver ↔ Metadata DB ↔ Scheduler
         ↑          ↑           ↑
         └──────────┴───────────┘
   ```
   - All components must share the same `secret_key`
   - Consistent configuration across all hosts
   - Zero-trust principles implemented

## Critical Configurations

### 1. Security Configurations

```ini
[webserver]
secret_key = your_secret_key
auth_type = auth_db  # or oauth, ldap, etc.
auth_user_registration = False
auth_user_registration_role = Public

[api]
auth_backends = airflow.api.auth.backend.basic_auth
enable_auth_rate_limiting = True
auth_rate_limit = 5/minute
```

### 2. Database Connectivity

```ini
[database]
sql_alchemy_conn = postgresql+psycopg2://user:pass@host:5432/airflow
sql_engine_encoding = utf-8
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 1800
```

### 3. Component Communication

```ini
[core]
executor = CeleryExecutor  # or LocalExecutor, KubernetesExecutor
dag_file_processor_timeout = 600
dag_dir_list_interval = 300
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 16

[scheduler]
scheduler_heartbeat_sec = 5
scheduler_health_check_threshold = 30
scheduler_idle_sleep_time = 1
```

## Security Model

1. **Role-Based Access Control**
   - Deployment Manager: Full system access
   - DAG Author: DAG creation and editing
   - Operations User: DAG triggering and monitoring

2. **Component Isolation**
   - Tasks communicate only via REST API
   - Workers decoupled from metadata DB
   - DAG processor isolated from scheduler
   - Webserver restricted from executing user code

3. **Resource Access**
   - Variables and connections accessed via API
   - Direct DB access from tasks restricted
   - Scoped tokens for task execution
   - Auth manager handles all authentication/authorization

## Best Practices

1. **Configuration Management**
   - Use same configurations across all components
   - Store sensitive data in environment variables
   - Implement secret backends for credentials
   - Regular rotation of security keys

2. **Performance Optimization**
   - Configure appropriate pool sizes
   - Set reasonable task concurrency limits
   - Monitor database connections
   - Implement proper logging levels

3. **High Availability Setup**
   - Run multiple scheduler instances
   - Use robust database backend (PostgreSQL recommended)
   - Implement proper backup strategies
   - Monitor component health

## Major Changes in 3.1.0

1. **Architecture Improvements**
   - Enhanced DAG serialization
   - Improved task isolation
   - Better component separation
   - Standardized DAG processor separation

2. **Security Enhancements**
   - Restricted direct DB access
   - Enhanced auth management
   - Improved token handling
   - Better role separation

3. **Performance Updates**
   - Optimized UI bundle size
   - Improved graph UI loading
   - Dynamic translation loading
   - Better task scheduling algorithms

## Deployment Considerations

1. **Environment Configuration**
   - Consistent settings across components
   - Proper network security groups
   - Adequate resource allocation
   - Monitoring and alerting setup

2. **Scaling Strategy**
   - Horizontal scaling of workers
   - Database connection management
   - Cache configuration
   - Load balancing considerations

3. **Maintenance Procedures**
   - Regular backups
   - Version upgrade strategy
   - Performance monitoring
   - Security auditing