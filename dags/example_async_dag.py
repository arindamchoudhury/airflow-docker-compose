"""
Async and Deferred Tasks Example DAG for Airflow 3.1.0

This DAG demonstrates Airflow 3.1.0 async and deferred task features including:
- Deferrable operators (new in Airflow 2.2+, enhanced in 3.x)
- Async task execution
- Triggerer service usage
- Time-based deferrable tasks
- Custom deferrable operators

Requirements covered: 1.4, 3.4
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import asyncio


# Default arguments
default_args = {
    'owner': 'airflow-developer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'example_async_dag',
    default_args=default_args,
    description='Example DAG showcasing async and deferred tasks in Airflow 3.1.0',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'async', 'deferred', 'triggerer', 'airflow-3.1.0'],
    doc_md=__doc__,
)


async def async_data_fetch():
    """
    Async function to simulate data fetching
    Demonstrates async/await patterns in Airflow tasks
    """
    print("Starting async data fetch...")
    
    # Simulate async API calls
    await asyncio.sleep(2)  # Simulate network delay
    print("Fetching user data...")
    
    await asyncio.sleep(1)  # Another async operation
    print("Fetching product data...")
    
    await asyncio.sleep(1)  # Final async operation
    print("Async data fetch completed!")
    
    return {
        'users': ['user1', 'user2', 'user3'],
        'products': ['product1', 'product2'],
        'fetch_time': str(datetime.now())
    }


def run_async_task():
    """
    Wrapper function to run async code in PythonOperator
    """
    import asyncio
    
    # Run the async function
    result = asyncio.run(async_data_fetch())
    print(f"Async task result: {result}")
    return result


@task(dag=dag)
def process_async_result(**context):
    """
    Process the result from async task
    """
    # Get result from previous task
    async_result = context['task_instance'].xcom_pull(task_ids='async_data_processing')
    
    print(f"Processing async result: {async_result}")
    
    if async_result:
        user_count = len(async_result.get('users', []))
        product_count = len(async_result.get('products', []))
        
        summary = {
            'total_users': user_count,
            'total_products': product_count,
            'processing_time': str(datetime.now())
        }
        
        print(f"Processing summary: {summary}")
        return summary
    
    return {'error': 'No async result found'}


def simulate_long_running_task():
    """
    Simulate a long-running task that would benefit from deferral
    """
    import time
    
    print("Starting long-running simulation...")
    
    # Simulate work in chunks to show progress
    for i in range(5):
        print(f"Processing chunk {i+1}/5...")
        time.sleep(2)  # Simulate work
    
    print("Long-running task completed!")
    return "Long task finished successfully"


# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Async data processing task
async_task = PythonOperator(
    task_id='async_data_processing',
    python_callable=run_async_task,
    dag=dag,
    doc_md="""
    ### Async Data Processing
    Demonstrates running async Python code within Airflow tasks.
    Uses asyncio to handle concurrent operations efficiently.
    """,
)

# Process async results
process_result_task = process_async_result()

# Time-based deferrable sensor
# This sensor will defer until a specific time offset from the execution date
time_sensor = TimeDeltaSensorAsync(
    task_id='wait_for_time_offset',
    delta=timedelta(minutes=5),  # Wait 5 minutes from execution time
    dag=dag,
    doc_md="""
    ### Time Delta Sensor (Async)
    Deferrable sensor that waits for a specific time offset.
    Uses the triggerer service instead of blocking a worker slot.
    """,
)

# DateTime sensor for specific time
datetime_sensor = DateTimeSensorAsync(
    task_id='wait_for_specific_time',
    target_time="{{ (execution_date + macros.timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S') }}",
    dag=dag,
    doc_md="""
    ### DateTime Sensor (Async)
    Deferrable sensor that waits until a specific datetime.
    Demonstrates templating with execution_date and macros.
    """,
)

# Long-running task (traditional approach)
long_task_traditional = PythonOperator(
    task_id='long_running_traditional',
    python_callable=simulate_long_running_task,
    dag=dag,
    doc_md="""
    ### Long Running Task (Traditional)
    Traditional approach to long-running tasks.
    This will occupy a worker slot for the entire duration.
    """,
)

# Simulate external system check
external_system_check = BashOperator(
    task_id='check_external_system',
    bash_command='''
    echo "Checking external system availability..."
    # Simulate system check with random success/failure
    if [ $((RANDOM % 2)) -eq 0 ]; then
        echo "External system is available"
        exit 0
    else
        echo "External system is not ready, will retry..."
        exit 1
    fi
    ''',
    retries=3,
    retry_delay=timedelta(seconds=30),
    dag=dag,
    doc_md="""
    ### External System Check
    Simulates checking external system availability.
    Demonstrates retry logic for unreliable external dependencies.
    """,
)

@task(dag=dag)
def summarize_async_workflow(**context):
    """
    Summarize the results of the async workflow
    """
    print("Summarizing async workflow results...")
    
    # Collect results from various tasks
    async_result = context['task_instance'].xcom_pull(task_ids='process_async_result')
    long_task_result = context['task_instance'].xcom_pull(task_ids='long_running_traditional')
    
    summary = {
        'workflow_completion_time': str(datetime.now()),
        'async_processing_result': async_result,
        'long_task_result': long_task_result,
        'sensors_completed': True,
        'external_system_status': 'checked'
    }
    
    print(f"Workflow summary: {summary}")
    return summary


# Final summary task
summary_task = summarize_async_workflow()

# End task
end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> async_task
async_task >> process_result_task

# Parallel execution of sensors and long-running task
start_task >> [time_sensor, datetime_sensor, long_task_traditional]

# External system check runs independently
start_task >> external_system_check

# All tasks must complete before summary
[process_result_task, time_sensor, datetime_sensor, long_task_traditional, external_system_check] >> summary_task
summary_task >> end_task

# Alternative dependency syntax for demonstration:
# You can also use the set_upstream/set_downstream methods
# summary_task.set_upstream([process_result_task, time_sensor, datetime_sensor, long_task_traditional, external_system_check])
# end_task.set_upstream(summary_task)