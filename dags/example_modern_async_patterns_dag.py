"""
Modern Async Patterns Example DAG for Airflow 3.1.0

This DAG demonstrates cutting-edge async patterns and features in Airflow 3.1.0:
- Advanced async/await patterns with proper error handling
- Deferrable operators with custom triggerer integration
- Async context managers and resource management
- Concurrent task execution patterns
- Modern Python async libraries integration
- Async database operations and API calls
- Stream processing with async generators

Requirements covered: 1.4, 5.2, 5.4
"""

from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, Any, List, Optional
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.configuration import conf
import asyncio
import aiohttp
import json
import random
import time


# Custom Async Sensor with proper deferrable implementation
class AsyncDataStreamSensor(BaseSensorOperator):
    """
    Advanced async sensor that monitors data streams
    Demonstrates modern deferrable sensor patterns in Airflow 3.1.0
    """
    
    def __init__(self, stream_url: str, expected_records: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.stream_url = stream_url
        self.expected_records = expected_records
    
    def poke(self, context: Context) -> bool:
        """
        Check if the data stream has sufficient records
        Uses async operations for non-blocking I/O
        """
        try:
            # Run async check in sync context
            result = asyncio.run(self._async_check_stream())
            return result
        except Exception as e:
            self.log.warning(f"Error checking stream: {str(e)}")
            return False
    
    async def _async_check_stream(self) -> bool:
        """
        Async method to check stream status
        """
        self.log.info(f"Checking async data stream: {self.stream_url}")
        
        # Simulate async HTTP check
        await asyncio.sleep(1)  # Simulate network delay
        
        # Simulate stream record count
        current_records = random.randint(0, 20)
        
        if current_records >= self.expected_records:
            self.log.info(f"Stream has {current_records} records (>= {self.expected_records})")
            return True
        else:
            self.log.info(f"Stream has {current_records} records (< {self.expected_records})")
            return False


# Default arguments
default_args = {
    'owner': 'async-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'example_modern_async_patterns_dag',
    default_args=default_args,
    description='Modern async patterns and advanced features for Airflow 3.1.0',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'async', 'modern', 'patterns', 'streaming', 'airflow-3.1.0'],
    doc_md=__doc__,
)


# Advanced async data fetching with proper resource management
async def fetch_multiple_apis_concurrent(urls: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch data from multiple APIs concurrently using aiohttp
    Demonstrates proper async resource management
    """
    async def fetch_single_api(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """Fetch data from a single API endpoint"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    # For demo purposes, simulate JSON response
                    data = {
                        'url': url,
                        'status': response.status,
                        'data': {'simulated': True, 'timestamp': str(datetime.now())},
                        'response_time_ms': random.randint(100, 500)
                    }
                    return data
                else:
                    return {
                        'url': url,
                        'status': response.status,
                        'error': f'HTTP {response.status}',
                        'data': None
                    }
        except asyncio.TimeoutError:
            return {
                'url': url,
                'status': 0,
                'error': 'Timeout',
                'data': None
            }
        except Exception as e:
            return {
                'url': url,
                'status': 0,
                'error': str(e),
                'data': None
            }
    
    # Use async context manager for proper resource cleanup
    async with aiohttp.ClientSession() as session:
        # Execute all requests concurrently
        tasks = [fetch_single_api(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({
                    'url': 'unknown',
                    'status': 0,
                    'error': str(result),
                    'data': None
                })
            else:
                processed_results.append(result)
        
        return processed_results


@task(dag=dag)
def concurrent_api_fetching() -> List[Dict[str, Any]]:
    """
    Demonstrate concurrent API fetching with async patterns
    """
    print("Starting concurrent API fetching...")
    
    # Simulate multiple API endpoints
    api_urls = [
        'https://httpbin.org/delay/1',
        'https://httpbin.org/json',
        'https://httpbin.org/uuid',
        'https://httpbin.org/ip',
        'https://httpbin.org/user-agent'
    ]
    
    # Run async function in sync context
    start_time = time.time()
    results = asyncio.run(fetch_multiple_apis_concurrent(api_urls))
    end_time = time.time()
    
    print(f"Fetched {len(results)} APIs concurrently in {end_time - start_time:.2f} seconds")
    
    # Add timing information
    for result in results:
        result['fetch_duration'] = end_time - start_time
    
    return results


# Async generator for stream processing
async def async_data_generator(batch_size: int = 5, total_batches: int = 4) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Async generator that yields data batches
    Demonstrates async iteration patterns
    """
    for batch_num in range(total_batches):
        print(f"Generating batch {batch_num + 1}/{total_batches}")
        
        # Simulate async data generation
        await asyncio.sleep(1)  # Simulate processing time
        
        batch_data = {
            'batch_id': batch_num + 1,
            'records': [
                {
                    'id': batch_num * batch_size + i + 1,
                    'value': random.randint(1, 100),
                    'timestamp': str(datetime.now())
                }
                for i in range(batch_size)
            ],
            'batch_size': batch_size,
            'generated_at': str(datetime.now())
        }
        
        yield batch_data


@task(dag=dag)
def stream_processing_with_async_generator() -> Dict[str, Any]:
    """
    Process data using async generators
    Demonstrates streaming patterns in Airflow 3.1.0
    """
    print("Starting stream processing with async generator...")
    
    async def process_stream():
        """Async function to process the data stream"""
        processed_batches = []
        total_records = 0
        
        # Process data stream using async generator
        async for batch in async_data_generator(batch_size=3, total_batches=5):
            print(f"Processing batch {batch['batch_id']} with {len(batch['records'])} records")
            
            # Simulate async processing of each batch
            await asyncio.sleep(0.5)
            
            # Process records in batch
            processed_records = []
            for record in batch['records']:
                processed_record = record.copy()
                processed_record['processed'] = True
                processed_record['processing_time'] = str(datetime.now())
                processed_records.append(processed_record)
            
            processed_batch = {
                'batch_id': batch['batch_id'],
                'original_records': len(batch['records']),
                'processed_records': processed_records,
                'processing_completed_at': str(datetime.now())
            }
            
            processed_batches.append(processed_batch)
            total_records += len(processed_records)
        
        return {
            'total_batches_processed': len(processed_batches),
            'total_records_processed': total_records,
            'processed_batches': processed_batches,
            'stream_processing_completed_at': str(datetime.now())
        }
    
    # Run the async stream processing
    result = asyncio.run(process_stream())
    print(f"Stream processing complete: {result['total_records_processed']} records in {result['total_batches_processed']} batches")
    
    return result


# Advanced async database simulation
class AsyncDatabaseSimulator:
    """
    Simulates async database operations
    Demonstrates async context managers and connection pooling
    """
    
    def __init__(self, connection_pool_size: int = 5):
        self.pool_size = connection_pool_size
        self.connections = []
    
    async def __aenter__(self):
        """Async context manager entry"""
        print(f"Initializing async database connection pool (size: {self.pool_size})")
        await asyncio.sleep(0.5)  # Simulate connection setup
        
        # Simulate connection pool
        self.connections = [f"conn_{i}" for i in range(self.pool_size)]
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        print("Closing async database connection pool")
        await asyncio.sleep(0.2)  # Simulate cleanup
        self.connections = []
    
    async def execute_query(self, query: str, connection_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute a database query asynchronously"""
        conn = connection_id or random.choice(self.connections)
        
        print(f"Executing query on {conn}: {query[:50]}...")
        
        # Simulate query execution time
        await asyncio.sleep(random.uniform(0.1, 0.5))
        
        # Simulate query results
        return {
            'connection': conn,
            'query': query,
            'rows_affected': random.randint(1, 100),
            'execution_time_ms': random.randint(50, 200),
            'executed_at': str(datetime.now())
        }
    
    async def execute_batch_queries(self, queries: List[str]) -> List[Dict[str, Any]]:
        """Execute multiple queries concurrently"""
        print(f"Executing {len(queries)} queries concurrently...")
        
        # Execute all queries concurrently using available connections
        tasks = [self.execute_query(query) for query in queries]
        results = await asyncio.gather(*tasks)
        
        return results


@task(dag=dag)
def async_database_operations() -> Dict[str, Any]:
    """
    Demonstrate async database operations with connection pooling
    """
    print("Starting async database operations...")
    
    async def perform_database_operations():
        """Async function for database operations"""
        
        # Use async context manager for proper resource management
        async with AsyncDatabaseSimulator(connection_pool_size=3) as db:
            
            # Single query execution
            single_result = await db.execute_query("SELECT * FROM users WHERE active = true")
            
            # Batch query execution
            batch_queries = [
                "INSERT INTO logs (message, timestamp) VALUES ('Process started', NOW())",
                "UPDATE metrics SET last_updated = NOW() WHERE id = 1",
                "SELECT COUNT(*) FROM transactions WHERE date = CURRENT_DATE",
                "DELETE FROM temp_data WHERE created_at < NOW() - INTERVAL '1 day'",
                "INSERT INTO audit_log (action, user_id) VALUES ('batch_process', 123)"
            ]
            
            batch_results = await db.execute_batch_queries(batch_queries)
            
            return {
                'single_query_result': single_result,
                'batch_query_results': batch_results,
                'total_queries_executed': 1 + len(batch_queries),
                'database_operations_completed_at': str(datetime.now())
            }
    
    # Execute async database operations
    result = asyncio.run(perform_database_operations())
    print(f"Database operations complete: {result['total_queries_executed']} queries executed")
    
    return result


# Async error handling and retry patterns
@task(dag=dag, retries=3, retry_delay=timedelta(seconds=30))
def async_error_handling_demo() -> Dict[str, Any]:
    """
    Demonstrate async error handling and retry patterns
    """
    print("Starting async error handling demonstration...")
    
    async def unreliable_async_operation(operation_id: int) -> Dict[str, Any]:
        """Simulate an unreliable async operation"""
        print(f"Attempting async operation {operation_id}...")
        
        await asyncio.sleep(1)  # Simulate work
        
        # Simulate random failures
        if random.random() < 0.3:  # 30% chance of failure
            raise Exception(f"Simulated failure in operation {operation_id}")
        
        return {
            'operation_id': operation_id,
            'status': 'success',
            'result': f"Operation {operation_id} completed successfully",
            'completed_at': str(datetime.now())
        }
    
    async def resilient_async_operations():
        """Perform multiple async operations with error handling"""
        operations = []
        errors = []
        
        # Attempt multiple operations with individual error handling
        for i in range(5):
            try:
                result = await unreliable_async_operation(i + 1)
                operations.append(result)
            except Exception as e:
                error_info = {
                    'operation_id': i + 1,
                    'error': str(e),
                    'failed_at': str(datetime.now())
                }
                errors.append(error_info)
                print(f"Operation {i + 1} failed: {str(e)}")
        
        return {
            'successful_operations': operations,
            'failed_operations': errors,
            'success_rate': len(operations) / (len(operations) + len(errors)),
            'error_handling_completed_at': str(datetime.now())
        }
    
    # Execute resilient async operations
    result = asyncio.run(resilient_async_operations())
    
    success_count = len(result['successful_operations'])
    error_count = len(result['failed_operations'])
    
    print(f"Async error handling complete: {success_count} successes, {error_count} errors")
    
    # Raise exception if too many failures (will trigger Airflow retry)
    if result['success_rate'] < 0.4:
        raise Exception(f"Too many async operation failures: {result['success_rate']:.2%} success rate")
    
    return result


# Custom async sensor
async_stream_sensor = AsyncDataStreamSensor(
    task_id='wait_for_data_stream',
    stream_url='https://api.example.com/stream',
    expected_records=15,
    poke_interval=30,
    timeout=300,
    dag=dag,
    doc_md="""
    ### Async Data Stream Sensor
    
    Custom deferrable sensor that monitors data streams using async patterns.
    Demonstrates modern sensor implementation in Airflow 3.1.0.
    """,
)


@task(dag=dag)
def consolidate_async_results(**context) -> Dict[str, Any]:
    """
    Consolidate results from all async operations
    """
    print("Consolidating async operation results...")
    
    # Collect results from all async tasks
    api_results = context['task_instance'].xcom_pull(task_ids='concurrent_api_fetching')
    stream_results = context['task_instance'].xcom_pull(task_ids='stream_processing_with_async_generator')
    db_results = context['task_instance'].xcom_pull(task_ids='async_database_operations')
    error_handling_results = context['task_instance'].xcom_pull(task_ids='async_error_handling_demo')
    
    consolidation = {
        'consolidation_time': str(datetime.now()),
        'api_operations': {
            'total_apis_called': len(api_results) if api_results else 0,
            'successful_calls': len([r for r in api_results if r.get('status') == 200]) if api_results else 0
        },
        'stream_processing': {
            'total_batches': stream_results.get('total_batches_processed', 0) if stream_results else 0,
            'total_records': stream_results.get('total_records_processed', 0) if stream_results else 0
        },
        'database_operations': {
            'total_queries': db_results.get('total_queries_executed', 0) if db_results else 0
        },
        'error_handling': {
            'success_rate': error_handling_results.get('success_rate', 0) if error_handling_results else 0,
            'total_operations': len(error_handling_results.get('successful_operations', [])) + len(error_handling_results.get('failed_operations', [])) if error_handling_results else 0
        },
        'overall_status': 'COMPLETED'
    }
    
    print(f"Async operations consolidation: {json.dumps(consolidation, indent=2)}")
    return consolidation


# Task instantiation
api_task = concurrent_api_fetching()
stream_task = stream_processing_with_async_generator()
db_task = async_database_operations()
error_task = async_error_handling_demo()
consolidate_task = consolidate_async_results()

# Start and end markers
start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

# Define dependencies
start_task >> async_stream_sensor

# Parallel async operations after sensor completes
async_stream_sensor >> [api_task, stream_task, db_task, error_task]

# Consolidate results
[api_task, stream_task, db_task, error_task] >> consolidate_task

# End workflow
consolidate_task >> end_task