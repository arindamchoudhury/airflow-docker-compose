"""
Advanced Sensors and Custom Operators Example DAG for Airflow 3.1.0

This DAG demonstrates advanced Airflow 3.1.0 features including:
- Custom deferrable sensors
- HTTP sensors and operators
- File system monitoring with advanced patterns
- Custom operators with async capabilities
- Dynamic task generation based on sensor results
- Advanced sensor configurations and patterns

Requirements covered: 1.4, 5.2, 5.4
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.configuration import conf
import json
import os
import glob
import requests


# Custom Deferrable Sensor
class CustomApiSensor(BaseSensorOperator):
    """
    Custom sensor that monitors API endpoint status
    Demonstrates creating custom deferrable sensors in Airflow 3.1.0
    """
    
    def __init__(self, endpoint: str, expected_status: int = 200, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.expected_status = expected_status
    
    def poke(self, context: Context) -> bool:
        """
        Check if the API endpoint returns expected status
        """
        try:
            self.log.info(f"Checking API endpoint: {self.endpoint}")
            response = requests.get(self.endpoint, timeout=10)
            
            if response.status_code == self.expected_status:
                self.log.info(f"API endpoint returned expected status {self.expected_status}")
                return True
            else:
                self.log.info(f"API endpoint returned status {response.status_code}, expected {self.expected_status}")
                return False
                
        except Exception as e:
            self.log.warning(f"Error checking API endpoint: {str(e)}")
            return False


class MultiFileSensor(BaseSensorOperator):
    """
    Custom sensor that waits for multiple files to be present
    Demonstrates pattern matching and multiple file monitoring
    """
    
    def __init__(self, file_patterns: List[str], base_path: str = "/tmp", **kwargs):
        super().__init__(**kwargs)
        self.file_patterns = file_patterns
        self.base_path = base_path
    
    def poke(self, context: Context) -> bool:
        """
        Check if all file patterns have matching files
        """
        found_files = {}
        
        for pattern in self.file_patterns:
            full_pattern = os.path.join(self.base_path, pattern)
            matching_files = glob.glob(full_pattern)
            
            if matching_files:
                found_files[pattern] = matching_files
                self.log.info(f"Found files for pattern '{pattern}': {matching_files}")
            else:
                self.log.info(f"No files found for pattern '{pattern}'")
                return False
        
        # Store found files in XCom for downstream tasks
        context['task_instance'].xcom_push(key='found_files', value=found_files)
        self.log.info(f"All file patterns satisfied: {found_files}")
        return True


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
    'example_advanced_sensors_dag',
    default_args=default_args,
    description='Advanced sensors and custom operators example for Airflow 3.1.0',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'advanced', 'sensors', 'custom-operators', 'airflow-3.1.0'],
    doc_md=__doc__,
)


# Create test files for sensors
def create_test_files():
    """
    Create test files for sensor demonstration
    """
    import os
    
    test_dir = "/tmp/sensor_test"
    os.makedirs(test_dir, exist_ok=True)
    
    # Create various test files
    files_to_create = [
        "data_2024_01_01.csv",
        "data_2024_01_02.csv", 
        "config.json",
        "metadata.xml",
        "processed_batch_001.txt"
    ]
    
    for filename in files_to_create:
        filepath = os.path.join(test_dir, filename)
        with open(filepath, 'w') as f:
            f.write(f"Test content for {filename}\nCreated at: {datetime.now()}")
    
    print(f"Created test files in {test_dir}: {files_to_create}")
    return f"Created {len(files_to_create)} test files"


create_files_task = PythonOperator(
    task_id='create_test_files',
    python_callable=create_test_files,
    dag=dag,
    doc_md="""
    ### Create Test Files
    Creates test files for sensor demonstrations.
    This simulates external systems creating files that our sensors will monitor.
    """,
)

# Basic file sensor
basic_file_sensor = FileSensor(
    task_id='wait_for_config_file',
    filepath='/tmp/sensor_test/config.json',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
    doc_md="""
    ### Basic File Sensor
    Waits for a specific configuration file to be created.
    Demonstrates basic file system monitoring in Airflow 3.1.0.
    """,
)

# Custom multi-file sensor
multi_file_sensor = MultiFileSensor(
    task_id='wait_for_data_files',
    file_patterns=['data_*.csv', 'metadata.xml'],
    base_path='/tmp/sensor_test',
    poke_interval=30,
    timeout=300,
    dag=dag,
    doc_md="""
    ### Multi-File Sensor
    Custom sensor that waits for multiple file patterns.
    Demonstrates pattern matching and custom sensor implementation.
    """,
)

# HTTP sensor for external API
http_sensor = HttpSensor(
    task_id='wait_for_api_availability',
    http_conn_id='http_default',
    endpoint='httpbin.org/status/200',
    poke_interval=60,
    timeout=300,
    dag=dag,
    doc_md="""
    ### HTTP Sensor
    Monitors external API availability.
    Uses httpbin.org as a test endpoint that always returns 200.
    """,
)

# Custom API sensor
custom_api_sensor = CustomApiSensor(
    task_id='monitor_custom_api',
    endpoint='https://httpbin.org/json',
    expected_status=200,
    poke_interval=45,
    timeout=300,
    dag=dag,
    doc_md="""
    ### Custom API Sensor
    Custom implementation of API monitoring sensor.
    Demonstrates creating custom sensors with specific logic.
    """,
)


@task(dag=dag)
def process_sensor_results(**context) -> Dict[str, Any]:
    """
    Process results from various sensors
    Demonstrates accessing sensor data in downstream tasks
    """
    print("Processing sensor results...")
    
    # Get results from multi-file sensor
    found_files = context['task_instance'].xcom_pull(
        task_ids='wait_for_data_files', 
        key='found_files'
    )
    
    results = {
        'processing_time': str(datetime.now()),
        'found_files': found_files or {},
        'sensors_completed': True
    }
    
    print(f"Sensor results processed: {results}")
    return results


# HTTP operator to fetch data after sensors complete
fetch_api_data = HttpOperator(
    task_id='fetch_external_data',
    http_conn_id='http_default',
    endpoint='httpbin.org/json',
    method='GET',
    headers={'Content-Type': 'application/json'},

    dag=dag,
    doc_md="""
    ### HTTP Operator
    Fetches data from external API after sensors confirm availability.
    Demonstrates HTTP operations in Airflow 3.1.0.
    """,
)


@task(dag=dag)
def analyze_fetched_data(**context) -> Dict[str, Any]:
    """
    Analyze data fetched from HTTP operator
    """
    print("Analyzing fetched data...")
    
    # Get data from HTTP operator
    api_response = context['task_instance'].xcom_pull(task_ids='fetch_external_data')
    
    if api_response:
        try:
            # Parse JSON response if it's a string
            if isinstance(api_response, str):
                data = json.loads(api_response)
            else:
                data = api_response
            
            analysis = {
                'data_type': type(data).__name__,
                'data_keys': list(data.keys()) if isinstance(data, dict) else [],
                'data_size': len(str(data)),
                'analysis_time': str(datetime.now())
            }
            
        except Exception as e:
            analysis = {
                'error': str(e),
                'raw_response': str(api_response)[:200],  # First 200 chars
                'analysis_time': str(datetime.now())
            }
    else:
        analysis = {
            'error': 'No data received from HTTP operator',
            'analysis_time': str(datetime.now())
        }
    
    print(f"Data analysis complete: {analysis}")
    return analysis


# Dynamic task generation based on sensor results
@task(dag=dag)
def generate_processing_tasks(**context) -> List[str]:
    """
    Generate dynamic processing tasks based on sensor results
    Demonstrates dynamic task generation in Airflow 3.1.0
    """
    print("Generating dynamic processing tasks...")
    
    # Get sensor results
    sensor_results = context['task_instance'].xcom_pull(task_ids='process_sensor_results')
    found_files = sensor_results.get('found_files', {}) if sensor_results else {}
    
    processing_tasks = []
    
    for pattern, files in found_files.items():
        for file_path in files:
            task_name = f"process_{os.path.basename(file_path).replace('.', '_')}"
            processing_tasks.append(task_name)
    
    print(f"Generated {len(processing_tasks)} processing tasks: {processing_tasks}")
    return processing_tasks


def process_individual_file(file_info: str) -> str:
    """
    Process an individual file
    This would be called dynamically for each file found by sensors
    """
    print(f"Processing file: {file_info}")
    
    # Simulate file processing
    import time
    time.sleep(1)
    
    result = f"Processed {file_info} at {datetime.now()}"
    print(result)
    return result


# Create dynamic processing tasks
# Note: In a real implementation, you might use the TaskFlow API's dynamic task mapping
# or create tasks programmatically based on sensor results

process_csv_files = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_individual_file,
    op_args=['CSV files from sensor'],
    dag=dag,
    doc_md="""
    ### Process CSV Files
    Processes CSV files detected by sensors.
    In a real scenario, this would be dynamically generated.
    """,
)

process_xml_files = PythonOperator(
    task_id='process_xml_files', 
    python_callable=process_individual_file,
    op_args=['XML files from sensor'],
    dag=dag,
    doc_md="""
    ### Process XML Files
    Processes XML files detected by sensors.
    Demonstrates file-type specific processing.
    """,
)


@task(dag=dag)
def consolidate_processing_results(**context) -> Dict[str, Any]:
    """
    Consolidate results from all processing tasks
    """
    print("Consolidating processing results...")
    
    # Collect results from various processing tasks
    csv_result = context['task_instance'].xcom_pull(task_ids='process_csv_files')
    xml_result = context['task_instance'].xcom_pull(task_ids='process_xml_files')
    api_analysis = context['task_instance'].xcom_pull(task_ids='analyze_fetched_data')
    
    consolidated_results = {
        'csv_processing': csv_result,
        'xml_processing': xml_result,
        'api_analysis': api_analysis,
        'consolidation_time': str(datetime.now()),
        'total_tasks_completed': 3
    }
    
    print(f"Consolidation complete: {consolidated_results}")
    return consolidated_results


# Final tasks
process_results = process_sensor_results()
analyze_data = analyze_fetched_data()
generate_tasks = generate_processing_tasks()
consolidate_results = consolidate_processing_results()

# Start and end markers
start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >> create_files_task

# Sensor dependencies - all sensors can run in parallel after files are created
create_files_task >> [basic_file_sensor, multi_file_sensor, http_sensor, custom_api_sensor]

# Process sensor results after basic sensors complete
[basic_file_sensor, multi_file_sensor] >> process_results

# HTTP operations after HTTP sensors complete
[http_sensor, custom_api_sensor] >> fetch_api_data
fetch_api_data >> analyze_data

# Generate dynamic tasks based on sensor results
process_results >> generate_tasks

# File processing tasks (in real scenario, these would be dynamically created)
process_results >> [process_csv_files, process_xml_files]

# Consolidate all results
[process_csv_files, process_xml_files, analyze_data, generate_tasks] >> consolidate_results

# End the workflow
consolidate_results >> end_task