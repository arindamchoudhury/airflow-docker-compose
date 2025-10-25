# Airflow 3.1.0 Docker Compose - Comprehensive Usage Guide

This comprehensive guide covers everything you need to know about using Apache Airflow 3.1.0 with Docker Compose on Windows 11.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Authentication Methods](#authentication-methods)
3. [DAG Development Best Practices](#dag-development-best-practices)
4. [Service Management](#service-management)
5. [Configuration Management](#configuration-management)
6. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
7. [Windows 11 Specific Considerations](#windows-11-specific-considerations)

## Getting Started

### Prerequisites

Before starting, ensure you have:

- **Docker Desktop for Windows 11** (version 4.0 or later)
- **WSL2 backend enabled** (recommended for better performance)
- **At least 4GB RAM** allocated to Docker Desktop
- **Git for Windows** (for cloning and version control)
- **Python 3.8+** (for generating Fernet keys and local development)

### Initial Setup

1. **Clone or download the project**
   ```bash
   git clone <your-repo-url>
   cd airflow-docker-compose
   ```

2. **Configure environment variables**
   ```bash
   # Copy the example environment file
   copy .env.example .env
   
   # Generate a Fernet key for encryption
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   
   # Edit .env file and paste the generated key
   notepad .env
   ```

3. **Initialize the database**
   ```bash
   # First-time database setup
   docker-compose up airflow-init
   
   # Wait for initialization to complete (look for "airflow-init exited with code 0")
   ```

4. **Start all services**
   ```bash
   # Start in detached mode
   docker-compose up -d
   
   # Or start with logs visible
   docker-compose up
   ```

5. **Access Airflow**
   - Open your browser and navigate to http://localhost:8080
   - Login with default credentials: `admin` / `admin`

### Verification Steps

After startup, verify everything is working:

1. **Check service status**
   ```bash
   docker-compose ps
   ```

2. **View service logs**
   ```bash
   # View all logs
   docker-compose logs
   
   # View specific service logs
   docker-compose logs airflow-scheduler
   ```

3. **Test DAG loading**
   - Check that example DAGs appear in the web UI
   - Verify DAG parsing is working without errors

## Authentication Methods

Airflow 3.1.0 supports two authentication methods. Choose the one that best fits your needs.

### Simple Auth Manager (Default)

**Best for**: Development, testing, API-first workflows, lightweight deployments

**Features**:
- Configuration-based user management
- JWT token support for API access
- Fast and lightweight
- Easy to configure

**Usage**:
```bash
# Start with Simple Auth (default)
docker-compose up -d
```

**User Management**:
- Users are defined in `config/users.json`
- Default users: admin/admin, user/user, viewer/viewer
- Roles: Admin, User, Viewer

**API Access**:
```bash
# Get JWT token
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'

# Use JWT token
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  http://localhost:8080/api/v1/dags

# Or use basic auth
curl -u admin:admin http://localhost:8080/api/v1/dags
```

For detailed authentication guide, see:
- [Simple Auth Usage Guide](../config/simple-auth-usage.md)

## DAG Development Best Practices

### Airflow 3.1.0 New Features

Airflow 3.1.0 introduces several improvements for DAG development:

1. **Enhanced Task Groups**: Better visualization and organization
2. **Improved Async Support**: Better handling of deferred tasks
3. **New Grid View**: Enhanced task instance visualization
4. **FastAPI Backend**: Improved API performance
5. **Better Error Handling**: More detailed error messages

### DAG Structure Best Practices

#### 1. Use Modern DAG Declaration

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Use context manager for cleaner code
with DAG(
    'modern_dag_example',
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A modern DAG example for Airflow 3.1.0',
    schedule='@daily',
    catchup=False,
    tags=['example', 'modern'],
) as dag:
    
    # Define tasks here
    pass
```

#### 2. Leverage Task Groups

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("data_processing") as data_processing:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_function,
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_function,
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_function,
    )
    
    extract_task >> transform_task >> load_task
```

#### 3. Use Async Patterns for I/O Operations

```python
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# Use sensors for waiting on external conditions
wait_for_file = FileSensor(
    task_id='wait_for_input_file',
    filepath='/opt/airflow/data/input.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    mode='poke',  # or 'reschedule' for better resource usage
)

# Use deferrable operators when available
async_task = PythonOperator(
    task_id='async_processing',
    python_callable=async_function,
    deferrable=True,  # New in Airflow 3.x
)
```

### DAG Organization

#### Directory Structure
```
dags/
├── common/                 # Shared utilities
│   ├── __init__.py
│   ├── operators.py        # Custom operators
│   ├── sensors.py         # Custom sensors
│   └── utils.py           # Utility functions
├── data_pipelines/        # Data processing DAGs
│   ├── daily_etl.py
│   └── weekly_reports.py
├── monitoring/            # Monitoring and alerting DAGs
│   └── health_checks.py
└── examples/              # Example and test DAGs
    └── example_dag.py
```

#### Configuration Management

```python
# Use Airflow Variables for configuration
from airflow.models import Variable

# Set variables via UI or CLI
# docker-compose exec airflow-api-server airflow variables set my_var "my_value"

config = {
    'database_url': Variable.get('database_url', default_var='localhost'),
    'api_key': Variable.get('api_key'),
    'batch_size': Variable.get('batch_size', default_var=1000, deserialize_json=False),
}
```

#### Connection Management

```python
# Use Airflow Connections for external systems
from airflow.hooks.base import BaseHook

# Create connections via UI or CLI
# docker-compose exec airflow-api-server airflow connections add 'my_db' \
#   --conn-type 'postgres' \
#   --conn-host 'localhost' \
#   --conn-login 'user' \
#   --conn-password 'password' \
#   --conn-schema 'mydb'

def get_database_connection():
    conn = BaseHook.get_connection('my_db')
    return conn
```

### Error Handling and Monitoring

#### 1. Implement Proper Error Handling

```python
from airflow.exceptions import AirflowException
import logging

def robust_task_function(**context):
    try:
        # Your task logic here
        result = perform_operation()
        
        # Log success
        logging.info(f"Task completed successfully: {result}")
        return result
        
    except Exception as e:
        # Log the error with context
        logging.error(f"Task failed: {str(e)}")
        logging.error(f"Task instance: {context['task_instance']}")
        
        # Re-raise as AirflowException for proper handling
        raise AirflowException(f"Task failed: {str(e)}")
```

#### 2. Use Task Callbacks

```python
def task_success_callback(context):
    logging.info(f"Task {context['task_instance'].task_id} succeeded")

def task_failure_callback(context):
    logging.error(f"Task {context['task_instance'].task_id} failed")
    # Send notification, create ticket, etc.

task = PythonOperator(
    task_id='monitored_task',
    python_callable=my_function,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
)
```

### Testing DAGs

#### 1. Unit Testing

```python
# tests/test_dags.py
import pytest
from airflow.models import DagBag

def test_dag_loaded():
    """Test that DAG is loaded without errors"""
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='my_dag')
    assert dag is not None
    assert len(dag.tasks) > 0

def test_dag_structure():
    """Test DAG structure and dependencies"""
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='my_dag')
    
    # Test task dependencies
    task1 = dag.get_task('task1')
    task2 = dag.get_task('task2')
    assert task2 in task1.downstream_list
```

#### 2. Integration Testing

```python
# Test task execution
from airflow.utils.state import State

def test_task_execution():
    """Test that tasks execute successfully"""
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='my_dag')
    
    # Create a test run
    dag_run = dag.create_dagrun(
        run_id='test_run',
        start_date=datetime.now(),
        external_trigger=True,
        state=State.RUNNING
    )
    
    # Test task execution
    task = dag.get_task('my_task')
    task_instance = dag_run.get_task_instance(task.task_id)
    task_instance.run()
    
    assert task_instance.state == State.SUCCESS
```

## Service Management

### Starting and Stopping Services

#### Basic Operations
```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart specific service
docker-compose restart airflow-scheduler

# View service status
docker-compose ps

# View resource usage
docker stats
```

#### Service-Specific Operations

```bash
# Scheduler operations
docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob

# DAG operations
docker-compose exec airflow-api-server airflow dags list
docker-compose exec airflow-api-server airflow dags trigger my_dag

# Task operations
docker-compose exec airflow-api-server airflow tasks test my_dag my_task 2024-01-01
```

### Scaling Services

#### CeleryExecutor with Workers

```bash
# Start with CeleryExecutor
AIRFLOW__CORE__EXECUTOR=CeleryExecutor docker-compose up -d

# Scale workers
docker-compose up -d --scale airflow-worker=3

# Monitor with Flower
docker-compose --profile flower up -d
# Access Flower at http://localhost:5555
```

### Health Monitoring

#### Service Health Checks

```bash
# Check service health
docker-compose exec airflow-api-server curl -f http://localhost:8080/health

# Check scheduler health
docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob

# Check database connectivity
docker-compose exec postgres pg_isready -U airflow
```

## Configuration Management

### Environment Variables

Key configuration options in `.env`:

```bash
# Core Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Database Configuration
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Authentication Configuration
AIRFLOW__CORE__AUTH_MANAGER=airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager

# API Configuration
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

# Scheduler Configuration
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300

# Logging Configuration
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
```

### Custom Configuration Files

#### Custom airflow.cfg

```bash
# Create custom configuration
# config/airflow.cfg

[core]
dags_folder = /opt/airflow/dags
plugins_folder = /opt/airflow/plugins
base_log_folder = /opt/airflow/logs

[webserver]
expose_config = True
enable_proxy_fix = True

[scheduler]
catchup_by_default = False
max_active_runs_per_dag = 1
```

#### Volume Mount Custom Config

```yaml
# In docker-compose.yml
volumes:
  - ./config/airflow.cfg:/opt/airflow/airflow.cfg:ro
```

## Monitoring and Troubleshooting

### Log Management

#### Accessing Logs

```bash
# Service logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-api-server
docker-compose logs postgres

# Follow logs in real-time
docker-compose logs -f airflow-scheduler

# Task logs (via web UI)
# Navigate to DAG > Task > Logs in the web interface
```

#### Log Configuration

```bash
# Increase log verbosity
AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG

# Configure log rotation
AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE={{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log
```

### Performance Monitoring

#### Resource Usage

```bash
# Monitor container resources
docker stats

# Check disk usage
docker system df

# Monitor database performance
docker-compose exec postgres psql -U airflow -c "SELECT * FROM pg_stat_activity;"
```

#### Airflow Metrics

```bash
# Check DAG performance
docker-compose exec airflow-api-server airflow dags report

# Monitor task instances
docker-compose exec airflow-api-server airflow tasks states-for-dag-run my_dag 2024-01-01
```

### Common Issues and Solutions

#### 1. Services Won't Start

**Symptoms**: Services fail to start or exit immediately

**Solutions**:
```bash
# Check Docker Desktop is running
docker version

# Verify resource allocation (minimum 4GB RAM)
# Docker Desktop > Settings > Resources

# Check for port conflicts
netstat -an | findstr :8080
netstat -an | findstr :5432

# Reset Docker environment
docker-compose down -v
docker system prune -f
```

#### 2. Database Connection Issues

**Symptoms**: "Connection refused" or "Database not ready" errors

**Solutions**:
```bash
# Check database service
docker-compose logs postgres

# Verify database initialization
docker-compose logs airflow-init

# Test database connectivity
docker-compose exec postgres pg_isready -U airflow

# Reinitialize database
docker-compose down -v
docker-compose up airflow-init
```

#### 3. DAG Loading Issues

**Symptoms**: DAGs not appearing in web UI or parsing errors

**Solutions**:
```bash
# Check DAG syntax
python dags/my_dag.py

# Check DAG processor logs
docker-compose logs airflow-dag-processor

# Test DAG loading
docker-compose exec airflow-api-server airflow dags list

# Check file permissions
ls -la dags/
```

#### 4. Authentication Problems

**Symptoms**: Login failures or permission denied errors

**Solutions**:
```bash
# Verify user configuration
cat config/users.json

# Check authentication logs
docker-compose logs airflow-api-server | grep -i auth

# Verify Simple Auth Manager configuration
cat config/users.json
```

## Windows 11 Specific Considerations

### Docker Desktop Configuration

#### Recommended Settings

1. **Enable WSL2 Backend**
   - Docker Desktop > Settings > General > Use WSL 2 based engine

2. **Resource Allocation**
   - Docker Desktop > Settings > Resources
   - Memory: Minimum 4GB, Recommended 8GB
   - CPU: Minimum 2 cores, Recommended 4 cores

3. **File Sharing**
   - Ensure project directory is accessible to Docker
   - Docker Desktop > Settings > Resources > File Sharing

#### WSL2 Integration

```bash
# Enable WSL2 integration
# Docker Desktop > Settings > Resources > WSL Integration
# Enable integration with your WSL2 distributions

# Verify WSL2 is active
wsl --list --verbose

# Access from WSL2
wsl
cd /mnt/c/path/to/your/project
docker-compose up -d
```

### Path and Volume Considerations

#### Windows Path Handling

```yaml
# Use forward slashes for cross-platform compatibility
volumes:
  - ./dags:/opt/airflow/dags          # ✓ Correct
  - .\dags:/opt/airflow/dags          # ✗ Windows-specific
  - C:\path\dags:/opt/airflow/dags    # ✗ Absolute Windows path
```

#### File Permissions

```bash
# If you encounter permission issues
# Set appropriate permissions in WSL2
chmod -R 755 dags/
chmod -R 755 logs/
chmod -R 755 plugins/
```

### Network Configuration

#### Port Conflicts

```bash
# Check for port conflicts
netstat -an | findstr :8080    # Airflow web UI
netstat -an | findstr :5432    # PostgreSQL
netstat -an | findstr :6379    # Redis
netstat -an | findstr :5555    # Flower

# Use different ports if needed
# Edit docker-compose.yml ports section
```

#### Firewall Considerations

```bash
# Windows Defender Firewall may block Docker
# Allow Docker Desktop through Windows Firewall
# Control Panel > System and Security > Windows Defender Firewall
# > Allow an app or feature through Windows Defender Firewall
```

### Performance Optimization

#### Docker Desktop Performance

1. **Use WSL2 Backend**: Better performance than Hyper-V
2. **Allocate Sufficient Resources**: Monitor usage with `docker stats`
3. **Use Volume Mounts Efficiently**: Avoid deep directory nesting
4. **Enable BuildKit**: Faster image builds

#### Windows-Specific Optimizations

```bash
# Use Windows Terminal for better experience
# Install Windows Terminal from Microsoft Store

# Use PowerShell 7 for better Docker integration
# Install PowerShell 7 from GitHub releases

# Consider using Windows Subsystem for Linux (WSL2)
# Better performance for Docker workloads
```

### Troubleshooting Windows Issues

#### Common Windows-Specific Problems

1. **Line Ending Issues**
   ```bash
   # Configure Git to handle line endings
   git config --global core.autocrlf true
   
   # Or use .gitattributes file
   echo "* text=auto" > .gitattributes
   ```

2. **Path Length Limitations**
   ```bash
   # Enable long path support in Windows
   # Run as Administrator in PowerShell:
   New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
   ```

3. **Docker Desktop Service Issues**
   ```bash
   # Restart Docker Desktop service
   # Services > Docker Desktop Service > Restart
   
   # Or via PowerShell (as Administrator)
   Restart-Service -Name "com.docker.service"
   ```

4. **WSL2 Integration Problems**
   ```bash
   # Reset WSL2 if needed
   wsl --shutdown
   wsl --unregister <distribution>
   wsl --install <distribution>
   ```

### Development Workflow on Windows

#### Recommended Tools

1. **Visual Studio Code** with extensions:
   - Docker
   - Python
   - Remote - WSL
   - GitLens

2. **Windows Terminal** for better command-line experience

3. **Git for Windows** with proper line ending configuration

#### Best Practices

1. **Use WSL2 for Development**: Better performance and compatibility
2. **Keep Project in WSL2 Filesystem**: Faster file access
3. **Use Volume Mounts Carefully**: Avoid performance issues
4. **Monitor Resource Usage**: Use Docker Desktop dashboard
5. **Regular Cleanup**: Remove unused containers and images

This comprehensive guide should help you successfully use Airflow 3.1.0 with Docker Compose on Windows 11. For additional help, consult the troubleshooting section or check the Airflow documentation.