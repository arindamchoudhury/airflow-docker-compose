# DAG Development Best Practices for Airflow 3.1.0

This guide covers best practices for developing DAGs in Apache Airflow 3.1.0, leveraging the new features and architectural improvements.

## Table of Contents

1. [Airflow 3.1.0 New Features](#airflow-310-new-features)
2. [Modern DAG Patterns](#modern-dag-patterns)
3. [Task Design Principles](#task-design-principles)
4. [Error Handling and Monitoring](#error-handling-and-monitoring)
5. [Performance Optimization](#performance-optimization)
6. [Testing Strategies](#testing-strategies)
7. [Configuration Management](#configuration-management)
8. [Security Best Practices](#security-best-practices)

## Airflow 3.1.0 New Features

### Key Improvements

1. **FastAPI Backend**: Improved API performance over Flask-AppBuilder
2. **Enhanced Grid View**: Better task instance visualization
3. **Improved Async Support**: Better handling of deferred tasks
4. **New Authentication System**: Simple Auth Manager as default
5. **Better Error Messages**: More detailed error reporting
6. **Performance Improvements**: Faster DAG parsing and execution

### New Components

- **API Server**: Replaces webserver, handles both UI and API
- **DAG Processor**: Separate service for parsing DAG files
- **Triggerer**: Dedicated service for deferred tasks

## Modern DAG Patterns

### 1. Context Manager Pattern (Recommended)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Use context manager for cleaner code
with DAG(
    'modern_etl_pipeline',
    default_args={
        'owner': 'data-engineering',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
    },
    description='Modern ETL pipeline with best practices',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'production', 'daily'],
    doc_md=__doc__,
) as dag:
    
    # Tasks defined here inherit the DAG context
    pass
```

### 2. Task Groups for Organization

```python
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

with TaskGroup("data_extraction", tooltip="Extract data from various sources") as extract_group:
    extract_customers = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers_data,
        doc_md="Extract customer data from CRM system",
    )
    
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_data,
        doc_md="Extract order data from e-commerce platform",
    )
    
    extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products_data,
        doc_md="Extract product catalog from inventory system",
    )

with TaskGroup("data_transformation", tooltip="Transform and clean data") as transform_group:
    clean_customers = PythonOperator(
        task_id='clean_customers',
        python_callable=clean_customer_data,
    )
    
    clean_orders = PythonOperator(
        task_id='clean_orders',
        python_callable=clean_order_data,
    )
    
    join_data = PythonOperator(
        task_id='join_customer_orders',
        python_callable=join_customer_order_data,
    )

# Define dependencies between groups
extract_group >> transform_group
```

### 3. Dynamic Task Generation

```python
from airflow.operators.python import PythonOperator

# Dynamic task creation based on configuration
REGIONS = ['us-east', 'us-west', 'eu-central', 'asia-pacific']

def create_region_tasks():
    tasks = []
    for region in REGIONS:
        task = PythonOperator(
            task_id=f'process_{region}',
            python_callable=process_region_data,
            op_kwargs={'region': region},
            doc_md=f"Process data for {region} region",
        )
        tasks.append(task)
    return tasks

# Create tasks dynamically
region_tasks = create_region_tasks()

# Set up dependencies
start_task >> region_tasks >> end_task
```

### 4. Async and Deferred Tasks

```python
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.s3 import S3KeySensor

# Use deferrable sensors for better resource utilization
wait_for_file = FileSensor(
    task_id='wait_for_input_file',
    filepath='/opt/airflow/data/{{ ds }}/input.csv',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=3600,
    mode='reschedule',  # Better resource usage than 'poke'
    deferrable=True,    # New in Airflow 3.x
)

# Async task example
async_processing = PythonOperator(
    task_id='async_data_processing',
    python_callable=async_process_data,
    deferrable=True,
)
```

## Task Design Principles

### 1. Idempotency

Tasks should produce the same result when run multiple times:

```python
def idempotent_data_load(**context):
    """Load data in an idempotent way"""
    execution_date = context['ds']
    
    # Use execution date in file paths and database operations
    output_path = f"/data/processed/{execution_date}/results.parquet"
    
    # Check if already processed
    if os.path.exists(output_path):
        logging.info(f"Data for {execution_date} already processed")
        return output_path
    
    # Process data
    data = extract_data(execution_date)
    processed_data = transform_data(data)
    
    # Save with atomic operation
    temp_path = f"{output_path}.tmp"
    processed_data.to_parquet(temp_path)
    os.rename(temp_path, output_path)  # Atomic operation
    
    return output_path
```

### 2. Atomicity

Tasks should be atomic - either fully succeed or fully fail:

```python
def atomic_database_operation(**context):
    """Perform database operations atomically"""
    import psycopg2
    
    conn = get_database_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                # All operations in a single transaction
                cursor.execute("DELETE FROM staging_table WHERE date = %s", (context['ds'],))
                cursor.execute("INSERT INTO staging_table SELECT * FROM source WHERE date = %s", (context['ds'],))
                cursor.execute("UPDATE summary_table SET last_updated = NOW() WHERE date = %s", (context['ds'],))
                
        logging.info("Database operations completed successfully")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Database operation failed: {e}")
        raise
    finally:
        conn.close()
```

### 3. Proper Resource Management

```python
def resource_managed_task(**context):
    """Properly manage resources in tasks"""
    import tempfile
    import shutil
    
    # Use context managers for resource cleanup
    with tempfile.TemporaryDirectory() as temp_dir:
        # Work with temporary files
        temp_file = os.path.join(temp_dir, 'processing.csv')
        
        try:
            # Process data
            data = extract_large_dataset()
            data.to_csv(temp_file)
            
            # Transform data
            result = process_csv_file(temp_file)
            
            # Save final result
            save_result(result, context['ds'])
            
        except Exception as e:
            logging.error(f"Processing failed: {e}")
            raise
        # Temporary directory automatically cleaned up
```

### 4. Parameterization and Templating

```python
# Use Jinja templating for dynamic values
parameterized_task = BashOperator(
    task_id='parameterized_processing',
    bash_command="""
    python /opt/airflow/scripts/process_data.py \
        --date {{ ds }} \
        --output-path /data/{{ ds }}/processed/ \
        --config-file /config/{{ params.environment }}.json
    """,
    params={'environment': 'production'},
)

# Use Variables for configuration
from airflow.models import Variable

def configurable_task(**context):
    """Use Airflow Variables for configuration"""
    batch_size = Variable.get('batch_size', default_var=1000, deserialize_json=False)
    api_endpoint = Variable.get('api_endpoint')
    
    # Use configuration in task logic
    process_data_in_batches(batch_size=int(batch_size), endpoint=api_endpoint)
```

## Error Handling and Monitoring

### 1. Comprehensive Error Handling

```python
from airflow.exceptions import AirflowException, AirflowSkipException
import logging

def robust_task_function(**context):
    """Task with comprehensive error handling"""
    task_instance = context['task_instance']
    
    try:
        # Validate inputs
        required_params = ['input_file', 'output_path']
        for param in required_params:
            if param not in context['params']:
                raise AirflowException(f"Required parameter '{param}' not provided")
        
        # Main task logic
        result = perform_complex_operation(context['params'])
        
        # Validate outputs
        if not validate_result(result):
            raise AirflowException("Output validation failed")
        
        # Log success with metrics
        logging.info(f"Task completed successfully. Processed {len(result)} records")
        
        # Store metrics for monitoring
        task_instance.xcom_push(key='records_processed', value=len(result))
        
        return result
        
    except FileNotFoundError as e:
        # Handle specific exceptions
        logging.warning(f"Input file not found: {e}")
        raise AirflowSkipException("Skipping task due to missing input file")
        
    except ValidationError as e:
        # Handle validation errors
        logging.error(f"Data validation failed: {e}")
        raise AirflowException(f"Data quality check failed: {e}")
        
    except Exception as e:
        # Handle unexpected errors
        logging.error(f"Unexpected error in task: {str(e)}")
        logging.error(f"Task context: {context}")
        raise AirflowException(f"Task failed with unexpected error: {str(e)}")
```

### 2. Task Callbacks for Monitoring

```python
def task_success_callback(context):
    """Handle successful task completion"""
    task_instance = context['task_instance']
    
    # Log success metrics
    records_processed = task_instance.xcom_pull(key='records_processed')
    logging.info(f"Task {task_instance.task_id} processed {records_processed} records")
    
    # Send success notification if needed
    if context['dag'].dag_id in ['critical_pipeline']:
        send_success_notification(context)

def task_failure_callback(context):
    """Handle task failure"""
    task_instance = context['task_instance']
    
    # Log failure details
    logging.error(f"Task {task_instance.task_id} failed")
    logging.error(f"Exception: {context.get('exception')}")
    
    # Send alert
    send_failure_alert(context)
    
    # Create incident ticket for critical DAGs
    if context['dag'].dag_id in ['critical_pipeline']:
        create_incident_ticket(context)

def dag_failure_callback(context):
    """Handle DAG-level failure"""
    logging.error(f"DAG {context['dag'].dag_id} failed")
    
    # Escalate critical failures
    if context['dag'].tags and 'critical' in context['dag'].tags:
        escalate_failure(context)

# Apply callbacks to tasks
monitored_task = PythonOperator(
    task_id='monitored_task',
    python_callable=robust_task_function,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
)

# Apply to entire DAG
dag.on_failure_callback = dag_failure_callback
```

### 3. Data Quality Checks

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def data_quality_check(**context):
    """Perform data quality checks"""
    data_path = context['task_instance'].xcom_pull(task_ids='extract_data')
    
    # Load data for validation
    data = load_data(data_path)
    
    # Define quality checks
    checks = {
        'row_count': len(data) > 0,
        'null_check': data.isnull().sum().sum() == 0,
        'duplicate_check': data.duplicated().sum() == 0,
        'date_range_check': validate_date_range(data),
    }
    
    # Log check results
    for check_name, passed in checks.items():
        logging.info(f"Quality check '{check_name}': {'PASSED' if passed else 'FAILED'}")
    
    # Determine next step based on checks
    if all(checks.values()):
        return 'quality_check_passed'
    else:
        return 'quality_check_failed'

quality_check = BranchPythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
)

quality_passed = DummyOperator(task_id='quality_check_passed')
quality_failed = DummyOperator(task_id='quality_check_failed')

# Set up branching
extract_data >> quality_check >> [quality_passed, quality_failed]
quality_passed >> continue_processing
quality_failed >> send_quality_alert
```

## Performance Optimization

### 1. Efficient Task Design

```python
def optimized_data_processing(**context):
    """Optimized task for large data processing"""
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    
    # Use chunking for large datasets
    chunk_size = Variable.get('chunk_size', default_var=10000)
    
    def process_chunk(chunk):
        # Process individual chunk
        return transform_data(chunk)
    
    # Process data in chunks with parallel processing
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for chunk in pd.read_csv(input_file, chunksize=int(chunk_size)):
            future = executor.submit(process_chunk, chunk)
            results.append(future.result())
    
    # Combine results
    final_result = pd.concat(results, ignore_index=True)
    return final_result
```

### 2. Memory Management

```python
def memory_efficient_task(**context):
    """Task designed for memory efficiency"""
    import gc
    
    try:
        # Process data in stages to manage memory
        stage1_result = process_stage1()
        
        # Clear intermediate variables
        del stage1_result
        gc.collect()
        
        stage2_result = process_stage2()
        
        # Use generators for large datasets
        for batch in data_generator(batch_size=1000):
            process_batch(batch)
            
    except MemoryError:
        logging.error("Task ran out of memory")
        # Implement fallback strategy
        process_with_smaller_batches()
```

### 3. Connection Pooling

```python
from airflow.hooks.base import BaseHook
from contextlib import contextmanager

@contextmanager
def get_database_connection():
    """Context manager for database connections"""
    conn = None
    try:
        # Use connection pooling
        hook = BaseHook.get_hook('postgres_default')
        conn = hook.get_conn()
        yield conn
    finally:
        if conn:
            conn.close()

def database_task(**context):
    """Task using connection pooling"""
    with get_database_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM large_table WHERE date = %s", (context['ds'],))
        results = cursor.fetchall()
        return results
```

## Testing Strategies

### 1. Unit Testing DAGs

```python
# tests/test_dags.py
import pytest
from datetime import datetime
from airflow.models import DagBag
from airflow.utils.state import State

class TestDAGs:
    def setup_method(self):
        """Setup test environment"""
        self.dag_bag = DagBag()
    
    def test_dag_loaded(self):
        """Test that DAG loads without errors"""
        dag = self.dag_bag.get_dag(dag_id='my_etl_pipeline')
        assert dag is not None
        assert len(dag.tasks) > 0
        assert dag.dag_id == 'my_etl_pipeline'
    
    def test_dag_structure(self):
        """Test DAG structure and dependencies"""
        dag = self.dag_bag.get_dag(dag_id='my_etl_pipeline')
        
        # Test specific tasks exist
        assert dag.has_task('extract_data')
        assert dag.has_task('transform_data')
        assert dag.has_task('load_data')
        
        # Test dependencies
        extract_task = dag.get_task('extract_data')
        transform_task = dag.get_task('transform_data')
        
        assert transform_task in extract_task.downstream_list
    
    def test_dag_configuration(self):
        """Test DAG configuration"""
        dag = self.dag_bag.get_dag(dag_id='my_etl_pipeline')
        
        assert dag.schedule_interval == '@daily'
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert 'etl' in dag.tags
```

### 2. Integration Testing

```python
# tests/test_integration.py
import pytest
from datetime import datetime
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType

class TestIntegration:
    def test_dag_execution(self):
        """Test complete DAG execution"""
        dag = self.dag_bag.get_dag(dag_id='my_etl_pipeline')
        
        # Create test DAG run
        dag_run = dag.create_dagrun(
            run_id='test_run',
            start_date=datetime(2024, 1, 1),
            external_trigger=True,
            state=State.RUNNING
        )
        
        # Execute tasks in order
        for task in dag.topological_sort():
            task_instance = TaskInstance(task, dag_run.execution_date)
            task_instance.run(ignore_dependencies=True)
            assert task_instance.state == State.SUCCESS
    
    def test_task_with_mock_data(self):
        """Test individual task with mock data"""
        from unittest.mock import patch, MagicMock
        
        dag = self.dag_bag.get_dag(dag_id='my_etl_pipeline')
        task = dag.get_task('extract_data')
        
        # Mock external dependencies
        with patch('my_dag.extract_from_api') as mock_extract:
            mock_extract.return_value = {'data': 'test'}
            
            # Execute task
            task_instance = TaskInstance(task, datetime(2024, 1, 1))
            result = task_instance.run()
            
            assert result is not None
            mock_extract.assert_called_once()
```

### 3. Data Testing

```python
# tests/test_data_quality.py
import pandas as pd
from airflow.models import Variable

class TestDataQuality:
    def test_data_schema(self):
        """Test data schema compliance"""
        # Load test data
        test_data = pd.read_csv('tests/fixtures/sample_data.csv')
        
        # Define expected schema
        expected_columns = ['id', 'name', 'email', 'created_at']
        
        assert list(test_data.columns) == expected_columns
        assert test_data['id'].dtype == 'int64'
        assert test_data['email'].str.contains('@').all()
    
    def test_data_transformations(self):
        """Test data transformation logic"""
        from my_dag import transform_customer_data
        
        # Create test input
        input_data = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        })
        
        # Apply transformation
        result = transform_customer_data(input_data)
        
        # Validate results
        assert 'full_name' in result.columns
        assert result['full_name'].iloc[0] == 'John Doe'
        assert len(result) == len(input_data)
```

## Configuration Management

### 1. Environment-Specific Configuration

```python
from airflow.models import Variable
import os

def get_environment_config():
    """Get configuration based on environment"""
    environment = Variable.get('environment', default_var='development')
    
    config = {
        'development': {
            'database_url': 'postgresql://localhost:5432/dev_db',
            'batch_size': 100,
            'enable_alerts': False,
        },
        'staging': {
            'database_url': 'postgresql://staging-db:5432/staging_db',
            'batch_size': 1000,
            'enable_alerts': True,
        },
        'production': {
            'database_url': Variable.get('prod_database_url'),
            'batch_size': 5000,
            'enable_alerts': True,
        }
    }
    
    return config.get(environment, config['development'])

def environment_aware_task(**context):
    """Task that adapts to environment"""
    config = get_environment_config()
    
    # Use environment-specific configuration
    process_data(
        database_url=config['database_url'],
        batch_size=config['batch_size']
    )
    
    if config['enable_alerts']:
        send_completion_alert(context)
```

### 2. Secret Management

```python
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def secure_api_task(**context):
    """Task that securely handles API credentials"""
    # Use Airflow Connections for credentials
    api_conn = BaseHook.get_connection('external_api')
    
    # Access connection details
    api_key = api_conn.password
    api_endpoint = api_conn.host
    
    # Use Variables for non-sensitive configuration
    timeout = Variable.get('api_timeout', default_var=30)
    
    # Make secure API call
    response = make_api_call(
        endpoint=api_endpoint,
        api_key=api_key,
        timeout=int(timeout)
    )
    
    return response
```

### 3. Configuration Validation

```python
def validate_configuration(**context):
    """Validate DAG configuration before execution"""
    required_variables = [
        'database_url',
        'api_endpoint',
        'batch_size'
    ]
    
    required_connections = [
        'postgres_default',
        'external_api'
    ]
    
    # Validate Variables
    for var_name in required_variables:
        try:
            value = Variable.get(var_name)
            if not value:
                raise AirflowException(f"Variable '{var_name}' is empty")
        except KeyError:
            raise AirflowException(f"Required variable '{var_name}' not found")
    
    # Validate Connections
    for conn_id in required_connections:
        try:
            conn = BaseHook.get_connection(conn_id)
            if not conn:
                raise AirflowException(f"Connection '{conn_id}' not found")
        except Exception as e:
            raise AirflowException(f"Connection '{conn_id}' validation failed: {e}")
    
    logging.info("Configuration validation passed")

# Add validation as first task
config_validation = PythonOperator(
    task_id='validate_configuration',
    python_callable=validate_configuration,
)

config_validation >> main_processing_tasks
```

## Security Best Practices

### 1. Secure Credential Handling

```python
# Never hardcode credentials
# ❌ Bad
API_KEY = "sk-1234567890abcdef"

# ✅ Good - Use Airflow Connections
def get_api_credentials():
    conn = BaseHook.get_connection('secure_api')
    return {
        'api_key': conn.password,
        'endpoint': conn.host,
        'username': conn.login
    }
```

### 2. Input Validation and Sanitization

```python
import re
from airflow.exceptions import AirflowException

def validate_and_sanitize_input(**context):
    """Validate and sanitize user inputs"""
    # Get input from XCom or parameters
    user_input = context['params'].get('user_query', '')
    
    # Validate input format
    if not re.match(r'^[a-zA-Z0-9_\-\s]+$', user_input):
        raise AirflowException("Invalid characters in user input")
    
    # Sanitize input
    sanitized_input = user_input.strip().lower()
    
    # Length validation
    if len(sanitized_input) > 100:
        raise AirflowException("Input too long")
    
    return sanitized_input
```

### 3. Secure File Handling

```python
import os
import tempfile
from pathlib import Path

def secure_file_processing(**context):
    """Securely handle file operations"""
    # Use secure temporary directories
    with tempfile.TemporaryDirectory() as temp_dir:
        # Validate file paths
        input_file = context['params']['input_file']
        
        # Prevent path traversal attacks
        if '..' in input_file or input_file.startswith('/'):
            raise AirflowException("Invalid file path")
        
        # Use Path for secure path handling
        safe_path = Path(temp_dir) / input_file
        
        # Ensure file is within allowed directory
        if not str(safe_path).startswith(temp_dir):
            raise AirflowException("File path outside allowed directory")
        
        # Process file securely
        process_file(safe_path)
```

### 4. Audit Logging

```python
import json
from datetime import datetime

def audit_log_task(**context):
    """Task with comprehensive audit logging"""
    audit_info = {
        'timestamp': datetime.utcnow().isoformat(),
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
        'execution_date': context['ds'],
        'user': context.get('user', 'system'),
        'action': 'data_processing',
        'parameters': context.get('params', {}),
    }
    
    # Log audit information
    logging.info(f"AUDIT: {json.dumps(audit_info)}")
    
    try:
        # Perform main task logic
        result = perform_sensitive_operation()
        
        # Log success
        audit_info.update({
            'status': 'success',
            'records_processed': len(result)
        })
        logging.info(f"AUDIT: {json.dumps(audit_info)}")
        
        return result
        
    except Exception as e:
        # Log failure
        audit_info.update({
            'status': 'failure',
            'error': str(e)
        })
        logging.error(f"AUDIT: {json.dumps(audit_info)}")
        raise
```

This comprehensive guide provides the foundation for developing robust, maintainable, and secure DAGs in Airflow 3.1.0. Follow these patterns and practices to build production-ready data pipelines.