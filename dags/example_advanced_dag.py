"""
Advanced Example DAG for Airflow 3.1.0

This DAG demonstrates advanced Airflow 3.1.0 features including:
- TaskFlow API (@task decorator)
- XCom usage for data passing between tasks
- Conditional branching with BranchPythonOperator
- Task groups for organization
- Sensors and external task dependencies
- Error handling and retries

Requirements covered: 1.4, 3.4
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
import json


# Default arguments
default_args = {
    'owner': 'airflow-developer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    'example_advanced_dag',
    default_args=default_args,
    description='Advanced example DAG showcasing Airflow 3.1.0 features',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'advanced', 'taskflow', 'airflow-3.1.0'],
    doc_md=__doc__,
)


@task(dag=dag)
def extract_data() -> Dict[str, Any]:
    """
    Extract data using TaskFlow API
    
    Returns:
        Dict containing extracted data
    """
    print("Extracting data from source...")
    
    # Simulate data extraction
    data = {
        'records': [
            {'id': 1, 'name': 'Alice', 'value': 100},
            {'id': 2, 'name': 'Bob', 'value': 200},
            {'id': 3, 'name': 'Charlie', 'value': 150},
        ],
        'extraction_time': str(datetime.now()),
        'total_records': 3
    }
    
    print(f"Extracted {data['total_records']} records")
    return data


@task(dag=dag)
def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform the extracted data
    
    Args:
        raw_data: Data from extract_data task
        
    Returns:
        Dict containing transformed data
    """
    print("Transforming data...")
    
    records = raw_data['records']
    
    # Transform data: calculate average value and add status
    total_value = sum(record['value'] for record in records)
    avg_value = total_value / len(records)
    
    transformed_records = []
    for record in records:
        transformed_record = record.copy()
        transformed_record['status'] = 'high' if record['value'] > avg_value else 'low'
        transformed_records.append(transformed_record)
    
    transformed_data = {
        'records': transformed_records,
        'total_value': total_value,
        'average_value': avg_value,
        'transformation_time': str(datetime.now())
    }
    
    print(f"Transformed {len(transformed_records)} records, avg value: {avg_value}")
    return transformed_data


def decide_processing_branch(**context) -> str:
    """
    Decide which processing branch to take based on data
    
    Returns:
        Task ID of the next task to execute
    """
    # Get data from previous task using XCom
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    avg_value = transformed_data['average_value']
    
    print(f"Average value: {avg_value}")
    
    if avg_value > 150:
        print("Taking high_value_processing branch")
        return 'high_value_processing'
    else:
        print("Taking low_value_processing branch")
        return 'low_value_processing'


# Branching task
branch_task = BranchPythonOperator(
    task_id='decide_processing_branch',
    python_callable=decide_processing_branch,

    dag=dag,
    doc_md="""
    ### Branching Task
    Demonstrates conditional branching based on data from previous tasks.
    Uses XCom to pull data and make decisions about workflow execution.
    """,
)


@task(dag=dag)
def high_value_processing(transformed_data: Dict[str, Any]) -> str:
    """
    Process high-value records
    """
    print("Processing high-value records...")
    high_value_records = [r for r in transformed_data['records'] if r['status'] == 'high']
    print(f"Found {len(high_value_records)} high-value records")
    return f"Processed {len(high_value_records)} high-value records"


@task(dag=dag)
def low_value_processing(transformed_data: Dict[str, Any]) -> str:
    """
    Process low-value records
    """
    print("Processing low-value records...")
    low_value_records = [r for r in transformed_data['records'] if r['status'] == 'low']
    print(f"Found {len(low_value_records)} low-value records")
    return f"Processed {len(low_value_records)} low-value records"


@task_group(group_id='data_quality_checks', dag=dag)
def data_quality_group():
    """
    Task group for data quality checks
    Demonstrates task grouping in Airflow 3.1.0
    """
    
    @task
    def check_record_count(transformed_data: Dict[str, Any]) -> bool:
        """Check if we have minimum required records"""
        record_count = len(transformed_data['records'])
        min_required = 1
        
        is_valid = record_count >= min_required
        print(f"Record count check: {record_count} >= {min_required} = {is_valid}")
        return is_valid
    
    @task
    def check_data_completeness(transformed_data: Dict[str, Any]) -> bool:
        """Check if all records have required fields"""
        required_fields = ['id', 'name', 'value', 'status']
        
        for record in transformed_data['records']:
            for field in required_fields:
                if field not in record:
                    print(f"Missing field {field} in record {record}")
                    return False
        
        print("Data completeness check passed")
        return True
    
    @task
    def validate_data_types(transformed_data: Dict[str, Any]) -> bool:
        """Validate data types in records"""
        for record in transformed_data['records']:
            if not isinstance(record['id'], int):
                print(f"Invalid id type in record {record}")
                return False
            if not isinstance(record['value'], (int, float)):
                print(f"Invalid value type in record {record}")
                return False
        
        print("Data type validation passed")
        return True
    
    # Return tasks to be used in the main DAG
    return [check_record_count, check_data_completeness, validate_data_types]


# File sensor to wait for input file (demonstrates sensors)
wait_for_file = FileSensor(
    task_id='wait_for_input_file',
    filepath='/tmp/airflow_input.txt',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
    doc_md="""
    ### File Sensor
    Demonstrates sensor usage in Airflow 3.1.0.
    Waits for a file to appear before proceeding with the workflow.
    """,
)

# Create input file task
create_input_file = BashOperator(
    task_id='create_input_file',
    bash_command='echo "Input data ready" > /tmp/airflow_input.txt',
    dag=dag,
)

# Final aggregation task that runs regardless of branch taken
@task(dag=dag, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def final_aggregation(**context) -> str:
    """
    Final task that aggregates results from both branches
    Uses trigger rule to run even if only one branch executed
    """
    print("Performing final aggregation...")
    
    # Try to get results from both processing branches
    high_result = None
    low_result = None
    
    try:
        high_result = context['task_instance'].xcom_pull(task_ids='high_value_processing')
    except:
        print("No high value processing result")
    
    try:
        low_result = context['task_instance'].xcom_pull(task_ids='low_value_processing')
    except:
        print("No low value processing result")
    
    result = f"Final aggregation complete. High: {high_result}, Low: {low_result}"
    print(result)
    return result


# Instantiate tasks
extract_task = extract_data()
transform_task = transform_data(extract_task)
quality_checks = data_quality_group()

# Get individual quality check tasks
check_count, check_completeness, check_types = quality_checks

high_processing = high_value_processing(transform_task)
low_processing = low_value_processing(transform_task)
final_task = final_aggregation()

# Define dependencies
create_input_file >> wait_for_file >> extract_task
extract_task >> transform_task
transform_task >> [check_count(transform_task), check_completeness(transform_task), check_types(transform_task)]
[check_count(transform_task), check_completeness(transform_task), check_types(transform_task)] >> branch_task

# Branch dependencies
branch_task >> [high_processing, low_processing]
[high_processing, low_processing] >> final_task