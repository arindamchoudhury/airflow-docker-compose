"""
Example Basic DAG for Airflow 3.1.0

This DAG demonstrates basic Airflow 3.1.0 features including:
- New DAG definition syntax
- Basic operators (BashOperator, PythonOperator)
- Task dependencies
- Documentation and logging best practices

Requirements covered: 1.4, 3.4
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def print_hello_world():
    """
    Simple Python function to demonstrate PythonOperator
    """
    print("Hello World from Airflow 3.1.0!")
    return "Hello World task completed successfully"


def process_data(**context):
    """
    Example function that demonstrates context usage and data processing

    Args:
        **context: Airflow context containing logical_date (formerly execution_date),
                  task instance, data interval, and other metadata.
    """
    logical_date = context[
        "logical_date"
    ]  # This replaces execution_date in Airflow 3.x
    task_instance = context["task_instance"]
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    print(f"Processing data for logical date: {logical_date}")
    print(f"Task instance: {task_instance.task_id}")
    print(f"Data interval: {data_interval_start} to {data_interval_end}")

    # Simulate some data processing
    data = {
        "processed_records": 100,
        "logical_date": str(logical_date),
        "data_interval": {
            "start": str(data_interval_start),
            "end": str(data_interval_end),
        },
    }

    # Return data that can be used by downstream tasks
    return data


# Default arguments for the DAG
default_args = {
    "owner": "airflow-developer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition using Airflow 3.1.0 syntax
dag = DAG(
    "example_basic_dag",
    default_args=default_args,
    description="A basic example DAG for Airflow 3.1.0",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "basic", "airflow-3.1.0"],
    doc_md=__doc__,
)

# Task 1: Start task (EmptyOperator)
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
    doc_md="""
    ### Start Task
    This is an empty task that marks the beginning of the DAG execution.
    EmptyOperator is useful for creating logical groupings and dependencies.
    """,
)

# Task 2: Hello World Python task
hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=print_hello_world,
    dag=dag,
    doc_md="""
    ### Hello World Task
    Demonstrates the PythonOperator with a simple function.
    This task executes a Python function and returns a value.
    """,
)

# Task 3: Data processing task
process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
    doc_md="""
    ### Process Data Task
    Demonstrates context usage in PythonOperator.
    Shows how to access execution_date, task_instance, and other context variables.
    """,
)

# Task 4: System information task using BashOperator
system_info_task = BashOperator(
    task_id="system_info",
    bash_command='echo "System: $(uname -a)" && echo "Date: $(date)" && echo "User: $(whoami)"',
    dag=dag,
    doc_md="""
    ### System Info Task
    Demonstrates BashOperator for executing shell commands.
    Useful for system operations, file manipulation, and external command execution.
    """,
)

# Task 5: End task
end_task = EmptyOperator(
    task_id="end",
    dag=dag,
    doc_md="""
    ### End Task
    Marks the completion of the DAG execution.
    All upstream tasks must complete successfully before this task runs.
    """,
)

# Define task dependencies
# Method 1: Using >> operator (recommended in Airflow 3.x)
start_task >> [hello_world_task, system_info_task]
hello_world_task >> process_data_task
system_info_task >> process_data_task
process_data_task >> end_task

# Alternative dependency definition methods:
# Method 2: Using set_upstream/set_downstream
# start_task.set_downstream([hello_world_task, system_info_task])
# process_data_task.set_upstream([hello_world_task, system_info_task])
# end_task.set_upstream(process_data_task)

# Method 3: Using depends_on_past in task definition (already shown in default_args)
