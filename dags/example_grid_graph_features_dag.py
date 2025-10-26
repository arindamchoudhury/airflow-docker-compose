"""
Grid and Graph View Features Example DAG for Airflow 3.1.0

This DAG demonstrates Airflow 3.1.0's enhanced Grid and Graph views:
- Task groups with nested structures
- Rich task metadata and documentation for Grid view
- Task state management and retry patterns

Requirements covered: 1.4, 5.2, 5.4
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import json
import random

# Default arguments with rich metadata
default_args = {
    "owner": "data-visualization-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# DAG definition optimized for Grid/Graph view demonstration
dag = DAG(
    "example_grid_graph_features_dag",
    default_args=default_args,
    description="Demonstrates Airflow 3.1.0 Grid and Graph view features",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "grid-view", "graph-view"],
    doc_md=__doc__,
)


# Task with rich documentation and metadata
@task(dag=dag)
def initialize_pipeline(**context) -> Dict[str, Any]:
    """Initialize the data processing pipeline"""
    print("Initializing pipeline...")

    config = {
        "pipeline_id": f"pipeline_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}",
        "environment": "development",
        "regions": ["us-east-1", "us-west-2"],
        "batch_size": 1000,
        "initialization_time": str(datetime.now()),
    }

    print(f"Pipeline initialized with config: {json.dumps(config, indent=2)}")
    return config


# Task group for data extraction with parallel tasks
@task_group(group_id="data_extraction_group", dag=dag)
def data_extraction_group():
    """Task group for data extraction"""

    # Get the config from the upstream task
    @task
    def extract_us_east(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from US East region"""
        print("Extracting data from US East region...")
        return {
            "region": "us-east-1",
            "records_extracted": random.randint(800, 1200),
            "status": "success",
        }

    @task
    def extract_us_west(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from US West region"""
        print("Extracting data from US West region...")
        return {
            "region": "us-west-2",
            "records_extracted": random.randint(700, 1100),
            "status": "success",
        }

    # Create the task instances with the config from upstream
    extract_us_east_task = extract_us_east(initialize_pipeline())
    extract_us_west_task = extract_us_west(initialize_pipeline())

    return [extract_us_east_task, extract_us_west_task]


def decide_output_format(**context) -> str:
    """Decide output format based on data volume"""
    # Get extraction results from both regions
    try:
        us_east = context["task_instance"].xcom_pull(
            task_ids="data_extraction_group.extract_us_east"
        )
        us_west = context["task_instance"].xcom_pull(
            task_ids="data_extraction_group.extract_us_west"
        )

        total_records = us_east.get("records_extracted", 0) + us_west.get(
            "records_extracted", 0
        )

        if total_records > 2000:
            print(f"High volume ({total_records} records) - using parquet format")
            return "output_parquet"
        else:
            print(f"Standard volume ({total_records} records) - using JSON format")
            return "output_json"
    except:
        return "output_json"


@task(dag=dag)
def output_json(**context) -> str:
    """Export data in JSON format"""
    print("Exporting data in JSON format...")
    result = {
        "format": "JSON",
        "file_size_mb": random.uniform(10, 50),
        "export_time": str(datetime.now()),
    }
    print(f"JSON export complete: {result}")
    return json.dumps(result)


@task(dag=dag)
def output_parquet(**context) -> str:
    """Export data in Parquet format"""
    print("Exporting data in Parquet format...")
    result = {
        "format": "Parquet",
        "file_size_mb": random.uniform(5, 25),
        "export_time": str(datetime.now()),
    }
    print(f"Parquet export complete: {result}")
    return json.dumps(result)


@task(dag=dag, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def generate_summary(**context) -> Dict[str, Any]:
    """Generate final pipeline summary"""
    print("Generating pipeline summary...")

    # Collect data from various completed tasks
    config = context["task_instance"].xcom_pull(task_ids="initialize_pipeline")

    # Try to get output format results
    json_result = None
    parquet_result = None

    try:
        json_result = context["task_instance"].xcom_pull(task_ids="output_json")
    except:
        pass

    try:
        parquet_result = context["task_instance"].xcom_pull(task_ids="output_parquet")
    except:
        pass

    summary = {
        "pipeline_id": config.get("pipeline_id", "unknown") if config else "unknown",
        "execution_summary": {
            "start_time": str(context["execution_date"]),
            "end_time": str(datetime.now()),
            "output_format": (
                "JSON" if json_result else "Parquet" if parquet_result else "Unknown"
            ),
        },
        "status": "COMPLETED",
    }

    print(f"Pipeline summary: {json.dumps(summary, indent=2)}")
    return summary


# Instantiate tasks
init_task = initialize_pipeline()
extraction_group = data_extraction_group()
json_output = output_json()
parquet_output = output_parquet()
summary = generate_summary()

# Start and end markers
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# Define dependencies
start >> init_task >> extraction_group

# Branching operator
branch_output_format = BranchPythonOperator(
    task_id="decide_output_format", python_callable=decide_output_format, dag=dag
)

# Branching based on data volume
extraction_group >> branch_output_format
branch_output_format >> [json_output, parquet_output]

# Summary after outputs
[json_output, parquet_output] >> summary >> end
