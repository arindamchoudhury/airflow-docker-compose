"""
Grid and Graph View Features Example DAG for Airflow 3.1.0

This DAG is specifically designed to showcase Airflow 3.1.0's enhanced Grid and Graph views:
- Complex task dependencies for Graph view visualization
- Task groups with nested structures
- Dynamic task mapping and parallel execution
- Rich task metadata and documentation for Grid view
- Task state management and retry patterns
- Custom task colors and visual organization

Requirements covered: 1.4, 5.2, 5.4
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
import json
import random


# Default arguments with rich metadata
default_args = {
    'owner': 'data-visualization-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 1,
}

# DAG definition optimized for Grid/Graph view demonstration
dag = DAG(
    'example_grid_graph_features_dag',
    default_args=default_args,
    description='Demonstrates Airflow 3.1.0 Grid and Graph view features with complex workflows',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=['example', 'grid-view', 'graph-view', 'visualization', 'airflow-3.1.0'],
    doc_md=__doc__,
    # Enhanced DAG-level documentation for Grid view
    params={
        'environment': 'development',
        'data_source': 'multi-region',
        'processing_mode': 'batch'
    }
)


# Task with rich documentation and metadata
@task(dag=dag, 
      doc_md="""
      ### Data Initialization Task
      
      **Purpose**: Initialize the data processing pipeline with configuration and setup.
      
      **Inputs**: None (uses DAG parameters)
      **Outputs**: Configuration dictionary with processing parameters
      
      **Grid View Features Demonstrated**:
      - Rich task documentation
      - Parameter usage
      - XCom data preview
      """,
      task_display_name="🚀 Initialize Pipeline")
def initialize_pipeline(**context) -> Dict[str, Any]:
    """
    Initialize the data processing pipeline
    Demonstrates rich task metadata for Grid view
    """
    dag_params = context.get('params', {})
    
    config = {
        'pipeline_id': f"pipeline_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}",
        'environment': dag_params.get('environment', 'development'),
        'data_source': dag_params.get('data_source', 'single-region'),
        'processing_mode': dag_params.get('processing_mode', 'batch'),
        'regions': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'],
        'batch_size': 1000,
        'max_parallel_tasks': 4,
        'initialization_time': str(datetime.now())
    }
    
    print(f"Pipeline initialized with config: {json.dumps(config, indent=2)}")
    return config


# Task group for data extraction with parallel tasks
@task_group(group_id='data_extraction_group', 
           tooltip="Extract data from multiple regions in parallel",
           dag=dag)
def data_extraction_group():
    """
    Task group demonstrating parallel data extraction
    Showcases task grouping in Graph view
    """
    
    @task(task_display_name="📥 Extract US East")
    def extract_us_east(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from US East region"""
        print("Extracting data from US East region...")
        
        # Simulate variable processing time for Grid view demonstration
        import time
        time.sleep(random.uniform(1, 3))
        
        return {
            'region': 'us-east-1',
            'records_extracted': random.randint(800, 1200),
            'extraction_time': str(datetime.now()),
            'status': 'success'
        }
    
    @task(task_display_name="📥 Extract US West")
    def extract_us_west(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from US West region"""
        print("Extracting data from US West region...")
        
        import time
        time.sleep(random.uniform(1, 3))
        
        return {
            'region': 'us-west-2',
            'records_extracted': random.randint(700, 1100),
            'extraction_time': str(datetime.now()),
            'status': 'success'
        }
    
    @task(task_display_name="📥 Extract Europe")
    def extract_europe(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from Europe region"""
        print("Extracting data from Europe region...")
        
        import time
        time.sleep(random.uniform(1, 3))
        
        # Occasionally simulate failure for retry demonstration
        if random.random() < 0.2:  # 20% chance of failure
            raise Exception("Simulated network timeout in Europe region")
        
        return {
            'region': 'eu-west-1',
            'records_extracted': random.randint(600, 1000),
            'extraction_time': str(datetime.now()),
            'status': 'success'
        }
    
    @task(task_display_name="📥 Extract Asia Pacific")
    def extract_asia_pacific(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from Asia Pacific region"""
        print("Extracting data from Asia Pacific region...")
        
        import time
        time.sleep(random.uniform(1, 3))
        
        return {
            'region': 'ap-southeast-1',
            'records_extracted': random.randint(500, 900),
            'extraction_time': str(datetime.now()),
            'status': 'success'
        }
    
    return [extract_us_east, extract_us_west, extract_europe, extract_asia_pacific]


# Complex task group with nested structure
@task_group(group_id='data_processing_group',
           tooltip="Multi-stage data processing with validation",
           dag=dag)
def data_processing_group():
    """
    Nested task group for complex data processing
    Demonstrates nested grouping in Graph view
    """
    
    # Validation sub-group
    with TaskGroup(group_id='validation_subgroup', tooltip="Data validation tasks") as validation_group:
        
        @task(task_display_name="✅ Validate Schema")
        def validate_schema(extraction_results: List[Dict[str, Any]]) -> Dict[str, Any]:
            """Validate data schema across all regions"""
            print("Validating data schema...")
            
            total_records = sum(result['records_extracted'] for result in extraction_results)
            
            validation_result = {
                'total_records': total_records,
                'regions_validated': len(extraction_results),
                'schema_valid': True,
                'validation_time': str(datetime.now())
            }
            
            print(f"Schema validation complete: {validation_result}")
            return validation_result
        
        @task(task_display_name="🔍 Check Data Quality")
        def check_data_quality(extraction_results: List[Dict[str, Any]]) -> Dict[str, Any]:
            """Check data quality metrics"""
            print("Checking data quality...")
            
            quality_score = random.uniform(0.85, 0.98)  # Simulate quality score
            
            quality_result = {
                'quality_score': quality_score,
                'quality_status': 'PASS' if quality_score > 0.9 else 'WARNING',
                'regions_checked': len(extraction_results),
                'check_time': str(datetime.now())
            }
            
            print(f"Data quality check complete: {quality_result}")
            return quality_result
    
    # Transformation sub-group
    with TaskGroup(group_id='transformation_subgroup', tooltip="Data transformation tasks") as transformation_group:
        
        @task(task_display_name="🔄 Transform Data")
        def transform_data(validation_result: Dict[str, Any], quality_result: Dict[str, Any]) -> Dict[str, Any]:
            """Transform and normalize data"""
            print("Transforming data...")
            
            import time
            time.sleep(2)  # Simulate processing time
            
            transformation_result = {
                'records_transformed': validation_result['total_records'],
                'transformation_type': 'normalize_and_enrich',
                'quality_maintained': quality_result['quality_score'] > 0.9,
                'transformation_time': str(datetime.now())
            }
            
            print(f"Data transformation complete: {transformation_result}")
            return transformation_result
        
        @task(task_display_name="📊 Generate Aggregates")
        def generate_aggregates(transformation_result: Dict[str, Any]) -> Dict[str, Any]:
            """Generate data aggregates and summaries"""
            print("Generating aggregates...")
            
            aggregates = {
                'total_records_processed': transformation_result['records_transformed'],
                'aggregation_types': ['daily_summary', 'regional_totals', 'trend_analysis'],
                'aggregates_generated': random.randint(50, 100),
                'aggregation_time': str(datetime.now())
            }
            
            print(f"Aggregates generated: {aggregates}")
            return aggregates
    
    return [validation_group, transformation_group]


# Conditional branching for Graph view complexity
def decide_output_format(**context) -> str:
    """
    Decide output format based on data volume
    Demonstrates branching in Graph view
    """
    # Get transformation results
    transformation_result = context['task_instance'].xcom_pull(
        task_ids='data_processing_group.transformation_subgroup.transform_data'
    )
    
    if transformation_result:
        record_count = transformation_result.get('records_transformed', 0)
        
        if record_count > 3000:
            print(f"High volume ({record_count} records) - using parquet format")
            return 'output_parquet'
        else:
            print(f"Standard volume ({record_count} records) - using JSON format")
            return 'output_json'
    
    return 'output_json'  # Default


# Branching operator
from airflow.operators.python import BranchPythonOperator

branch_output_format = BranchPythonOperator(
    task_id='decide_output_format',
    python_callable=decide_output_format,

    dag=dag,
    doc_md="""
    ### Output Format Decision
    
    Demonstrates conditional branching based on data volume:
    - High volume (>3000 records): Parquet format
    - Standard volume: JSON format
    
    **Graph View Feature**: Shows branching paths clearly
    """,
)

# Output format tasks
@task(dag=dag, task_display_name="📄 Export JSON")
def output_json(**context) -> str:
    """Export data in JSON format"""
    print("Exporting data in JSON format...")
    
    import time
    time.sleep(1)
    
    result = {
        'format': 'JSON',
        'file_size_mb': random.uniform(10, 50),
        'export_time': str(datetime.now())
    }
    
    print(f"JSON export complete: {result}")
    return json.dumps(result)


@task(dag=dag, task_display_name="📊 Export Parquet")
def output_parquet(**context) -> str:
    """Export data in Parquet format"""
    print("Exporting data in Parquet format...")
    
    import time
    time.sleep(2)  # Parquet takes longer
    
    result = {
        'format': 'Parquet',
        'file_size_mb': random.uniform(5, 25),  # More compressed
        'compression_ratio': random.uniform(0.3, 0.6),
        'export_time': str(datetime.now())
    }
    
    print(f"Parquet export complete: {result}")
    return json.dumps(result)


# Monitoring and alerting task group
@task_group(group_id='monitoring_group',
           tooltip="Pipeline monitoring and alerting",
           dag=dag)
def monitoring_group():
    """
    Monitoring task group that runs regardless of output format
    Demonstrates trigger rules in Graph view
    """
    
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
          task_display_name="📈 Generate Metrics")
    def generate_pipeline_metrics(**context) -> Dict[str, Any]:
        """Generate pipeline execution metrics"""
        print("Generating pipeline metrics...")
        
        # Collect metrics from various tasks
        config = context['task_instance'].xcom_pull(task_ids='initialize_pipeline')
        
        metrics = {
            'pipeline_id': config.get('pipeline_id', 'unknown') if config else 'unknown',
            'execution_date': str(context['execution_date']),
            'total_duration_minutes': random.uniform(5, 15),
            'success_rate': random.uniform(0.95, 1.0),
            'data_processed_gb': random.uniform(1, 10),
            'metrics_time': str(datetime.now())
        }
        
        print(f"Pipeline metrics: {json.dumps(metrics, indent=2)}")
        return metrics
    
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
          task_display_name="🚨 Check Alerts")
    def check_alerts(metrics: Dict[str, Any]) -> str:
        """Check for any alerts or issues"""
        print("Checking for alerts...")
        
        alerts = []
        
        if metrics.get('success_rate', 1.0) < 0.98:
            alerts.append("Low success rate detected")
        
        if metrics.get('total_duration_minutes', 0) > 12:
            alerts.append("Pipeline duration exceeded threshold")
        
        alert_status = "ALERTS_FOUND" if alerts else "NO_ALERTS"
        
        print(f"Alert check complete: {alert_status}")
        if alerts:
            print(f"Alerts: {alerts}")
        
        return alert_status
    
    return [generate_pipeline_metrics, check_alerts]


# Cleanup and finalization
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
    echo "Cleaning up temporary files..."
    # Simulate cleanup
    echo "Removed temporary processing files"
    echo "Cleanup completed at $(date)"
    ''',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
    doc_md="""
    ### Cleanup Task
    
    Runs regardless of upstream task success/failure.
    
    **Grid View Feature**: Shows trigger rule behavior
    **Graph View Feature**: Demonstrates ALL_DONE trigger rule
    """,
)

# Final summary task
@task(dag=dag, 
      trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
      task_display_name="📋 Pipeline Summary")
def pipeline_summary(**context) -> Dict[str, Any]:
    """
    Generate final pipeline summary
    Demonstrates complex dependency resolution in Grid view
    """
    print("Generating pipeline summary...")
    
    # Collect data from various completed tasks
    config = context['task_instance'].xcom_pull(task_ids='initialize_pipeline')
    metrics = context['task_instance'].xcom_pull(task_ids='monitoring_group.generate_pipeline_metrics')
    
    # Try to get output format results
    json_result = None
    parquet_result = None
    
    try:
        json_result = context['task_instance'].xcom_pull(task_ids='output_json')
    except:
        pass
    
    try:
        parquet_result = context['task_instance'].xcom_pull(task_ids='output_parquet')
    except:
        pass
    
    summary = {
        'pipeline_id': config.get('pipeline_id', 'unknown') if config else 'unknown',
        'execution_summary': {
            'start_time': str(context['execution_date']),
            'end_time': str(datetime.now()),
            'output_format': 'JSON' if json_result else 'Parquet' if parquet_result else 'Unknown'
        },
        'performance_metrics': metrics if metrics else {},
        'status': 'COMPLETED',
        'summary_time': str(datetime.now())
    }
    
    print(f"Pipeline summary: {json.dumps(summary, indent=2)}")
    return summary


# Instantiate tasks and task groups
init_task = initialize_pipeline()
extraction_tasks = data_extraction_group()
processing_tasks = data_processing_group()
monitoring_tasks = monitoring_group()
json_output = output_json()
parquet_output = output_parquet()
summary = pipeline_summary()

# Start and end markers for clear Graph view
start = EmptyOperator(task_id='start', dag=dag, doc_md="### Pipeline Start\nMarks the beginning of the data processing pipeline")
end = EmptyOperator(task_id='end', dag=dag, doc_md="### Pipeline End\nMarks the completion of the data processing pipeline")

# Define complex dependencies for Graph view demonstration
start >> init_task

# Parallel extraction
init_task >> extraction_tasks

# Sequential processing with task group dependencies
extraction_tasks >> processing_tasks

# Branching based on processing results
processing_tasks >> branch_output_format

# Conditional outputs
branch_output_format >> [json_output, parquet_output]

# Monitoring runs in parallel with outputs
processing_tasks >> monitoring_tasks

# Final summary waits for outputs and monitoring
[json_output, parquet_output, monitoring_tasks] >> summary

# Cleanup runs after everything
[summary, monitoring_tasks] >> cleanup_task

# End after cleanup and summary
[cleanup_task, summary] >> end