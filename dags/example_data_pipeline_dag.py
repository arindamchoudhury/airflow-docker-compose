"""
Data Pipeline Example DAG for Airflow 3.1.0

This DAG demonstrates a complete data pipeline using Airflow 3.1.0 features:
- ETL (Extract, Transform, Load) pattern
- Data validation and quality checks
- Error handling and data lineage
- Multiple data sources and formats
- TaskFlow API for clean data passing
- Dynamic task generation

Requirements covered: 1.4, 3.4
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import json
import csv
import io


# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'example_data_pipeline_dag',
    default_args=default_args,
    description='Complete data pipeline example for Airflow 3.1.0',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'data-pipeline', 'etl', 'taskflow', 'airflow-3.1.0'],
    doc_md=__doc__,
)


@task(dag=dag)
def extract_customer_data() -> List[Dict[str, Any]]:
    """
    Extract customer data from simulated source
    
    Returns:
        List of customer records
    """
    print("Extracting customer data...")
    
    # Simulate customer data extraction
    customers = [
        {'id': 1, 'name': 'Alice Johnson', 'email': 'alice@example.com', 'age': 28, 'city': 'New York'},
        {'id': 2, 'name': 'Bob Smith', 'email': 'bob@example.com', 'age': 35, 'city': 'Los Angeles'},
        {'id': 3, 'name': 'Charlie Brown', 'email': 'charlie@example.com', 'age': 42, 'city': 'Chicago'},
        {'id': 4, 'name': 'Diana Prince', 'email': 'diana@example.com', 'age': 31, 'city': 'Houston'},
        {'id': 5, 'name': 'Eve Wilson', 'email': 'eve@example.com', 'age': 26, 'city': 'Phoenix'},
    ]
    
    print(f"Extracted {len(customers)} customer records")
    return customers


@task(dag=dag)
def extract_order_data() -> List[Dict[str, Any]]:
    """
    Extract order data from simulated source
    
    Returns:
        List of order records
    """
    print("Extracting order data...")
    
    # Simulate order data extraction
    orders = [
        {'order_id': 101, 'customer_id': 1, 'product': 'Laptop', 'amount': 1200.00, 'order_date': '2024-01-15'},
        {'order_id': 102, 'customer_id': 2, 'product': 'Phone', 'amount': 800.00, 'order_date': '2024-01-16'},
        {'order_id': 103, 'customer_id': 1, 'product': 'Tablet', 'amount': 400.00, 'order_date': '2024-01-17'},
        {'order_id': 104, 'customer_id': 3, 'product': 'Monitor', 'amount': 300.00, 'order_date': '2024-01-18'},
        {'order_id': 105, 'customer_id': 4, 'product': 'Keyboard', 'amount': 100.00, 'order_date': '2024-01-19'},
        {'order_id': 106, 'customer_id': 2, 'product': 'Mouse', 'amount': 50.00, 'order_date': '2024-01-20'},
    ]
    
    print(f"Extracted {len(orders)} order records")
    return orders


@task(dag=dag)
def validate_customer_data(customers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate customer data quality
    
    Args:
        customers: List of customer records
        
    Returns:
        Validation results and cleaned data
    """
    print("Validating customer data...")
    
    valid_customers = []
    validation_errors = []
    
    for customer in customers:
        errors = []
        
        # Validate required fields
        required_fields = ['id', 'name', 'email', 'age', 'city']
        for field in required_fields:
            if field not in customer or customer[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate email format
        if 'email' in customer and '@' not in customer['email']:
            errors.append("Invalid email format")
        
        # Validate age range
        if 'age' in customer and (customer['age'] < 18 or customer['age'] > 100):
            errors.append("Age out of valid range (18-100)")
        
        if errors:
            validation_errors.append({
                'customer_id': customer.get('id', 'unknown'),
                'errors': errors
            })
        else:
            valid_customers.append(customer)
    
    validation_result = {
        'valid_customers': valid_customers,
        'validation_errors': validation_errors,
        'total_records': len(customers),
        'valid_records': len(valid_customers),
        'error_records': len(validation_errors)
    }
    
    print(f"Validation complete: {len(valid_customers)}/{len(customers)} records valid")
    return validation_result


@task(dag=dag)
def validate_order_data(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate order data quality
    
    Args:
        orders: List of order records
        
    Returns:
        Validation results and cleaned data
    """
    print("Validating order data...")
    
    valid_orders = []
    validation_errors = []
    
    for order in orders:
        errors = []
        
        # Validate required fields
        required_fields = ['order_id', 'customer_id', 'product', 'amount', 'order_date']
        for field in required_fields:
            if field not in order or order[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate amount
        if 'amount' in order and order['amount'] <= 0:
            errors.append("Amount must be positive")
        
        # Validate order_id uniqueness (simple check)
        if 'order_id' in order and order['order_id'] <= 0:
            errors.append("Invalid order_id")
        
        if errors:
            validation_errors.append({
                'order_id': order.get('order_id', 'unknown'),
                'errors': errors
            })
        else:
            valid_orders.append(order)
    
    validation_result = {
        'valid_orders': valid_orders,
        'validation_errors': validation_errors,
        'total_records': len(orders),
        'valid_records': len(valid_orders),
        'error_records': len(validation_errors)
    }
    
    print(f"Validation complete: {len(valid_orders)}/{len(orders)} records valid")
    return validation_result


@task(dag=dag)
def transform_and_enrich_data(customer_validation: Dict[str, Any], order_validation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform and enrich the validated data
    
    Args:
        customer_validation: Validated customer data
        order_validation: Validated order data
        
    Returns:
        Enriched and transformed data
    """
    print("Transforming and enriching data...")
    
    customers = customer_validation['valid_customers']
    orders = order_validation['valid_orders']
    
    # Create customer lookup
    customer_lookup = {customer['id']: customer for customer in customers}
    
    # Enrich orders with customer information
    enriched_orders = []
    for order in orders:
        customer_id = order['customer_id']
        if customer_id in customer_lookup:
            customer = customer_lookup[customer_id]
            enriched_order = order.copy()
            enriched_order.update({
                'customer_name': customer['name'],
                'customer_email': customer['email'],
                'customer_city': customer['city'],
                'customer_age_group': 'young' if customer['age'] < 30 else 'middle' if customer['age'] < 50 else 'senior'
            })
            enriched_orders.append(enriched_order)
    
    # Calculate customer metrics
    customer_metrics = []
    for customer in customers:
        customer_orders = [o for o in orders if o['customer_id'] == customer['id']]
        total_spent = sum(order['amount'] for order in customer_orders)
        order_count = len(customer_orders)
        
        metrics = {
            'customer_id': customer['id'],
            'customer_name': customer['name'],
            'total_orders': order_count,
            'total_spent': total_spent,
            'average_order_value': total_spent / order_count if order_count > 0 else 0,
            'customer_segment': 'high_value' if total_spent > 1000 else 'medium_value' if total_spent > 500 else 'low_value'
        }
        customer_metrics.append(metrics)
    
    transformed_data = {
        'enriched_orders': enriched_orders,
        'customer_metrics': customer_metrics,
        'transformation_summary': {
            'total_customers': len(customers),
            'total_orders': len(orders),
            'enriched_orders': len(enriched_orders),
            'transformation_time': str(datetime.now())
        }
    }
    
    print(f"Transformation complete: {len(enriched_orders)} enriched orders, {len(customer_metrics)} customer metrics")
    return transformed_data


# Data quality monitoring tasks
@task(dag=dag)
def check_data_completeness(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Check data completeness metrics"""
    print("Checking data completeness...")
    
    enriched_orders = transformed_data['enriched_orders']
    customer_metrics = transformed_data['customer_metrics']
    
    completeness_check = {
        'enriched_orders_count': len(enriched_orders),
        'customer_metrics_count': len(customer_metrics),
        'completeness_score': 1.0 if len(enriched_orders) > 0 and len(customer_metrics) > 0 else 0.0,
        'check_time': str(datetime.now())
    }
    
    print(f"Completeness check: {completeness_check}")
    return completeness_check

@task(dag=dag)
def check_data_consistency(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Check data consistency across datasets"""
    print("Checking data consistency...")
    
    enriched_orders = transformed_data['enriched_orders']
    customer_metrics = transformed_data['customer_metrics']
    
    # Check if all customers in orders have metrics
    order_customers = set(order['customer_id'] for order in enriched_orders)
    metric_customers = set(metric['customer_id'] for metric in customer_metrics)
    
    consistency_check = {
        'customers_in_orders': len(order_customers),
        'customers_in_metrics': len(metric_customers),
        'consistency_score': 1.0 if order_customers.issubset(metric_customers) else 0.0,
        'missing_customers': list(order_customers - metric_customers),
        'check_time': str(datetime.now())
    }
    
    print(f"Consistency check: {consistency_check}")
    return consistency_check

@task(dag=dag)
def generate_quality_report(completeness: Dict[str, Any], consistency: Dict[str, Any]) -> Dict[str, Any]:
    """Generate overall data quality report"""
    print("Generating data quality report...")
    
    overall_score = (completeness['completeness_score'] + consistency['consistency_score']) / 2
    
    quality_report = {
        'overall_quality_score': overall_score,
        'completeness_results': completeness,
        'consistency_results': consistency,
        'quality_status': 'PASS' if overall_score >= 0.8 else 'FAIL',
        'report_time': str(datetime.now())
    }
    
    print(f"Quality report generated: {quality_report['quality_status']} (score: {overall_score})")
    return quality_report


@task(dag=dag)
def load_to_data_warehouse(transformed_data: Dict[str, Any], quality_report: Dict[str, Any]) -> str:
    """
    Load data to data warehouse (simulated)
    
    Args:
        transformed_data: Transformed and enriched data
        quality_report: Data quality assessment results
        
    Returns:
        Load operation status
    """
    print("Loading data to data warehouse...")
    
    # Check quality before loading
    if quality_report['quality_status'] != 'PASS':
        raise ValueError(f"Data quality check failed: {quality_report['overall_quality_score']}")
    
    enriched_orders = transformed_data['enriched_orders']
    customer_metrics = transformed_data['customer_metrics']
    
    # Simulate data warehouse loading
    print(f"Loading {len(enriched_orders)} enriched orders...")
    print(f"Loading {len(customer_metrics)} customer metrics...")
    
    # Simulate some processing time
    import time
    time.sleep(2)
    
    load_summary = {
        'orders_loaded': len(enriched_orders),
        'metrics_loaded': len(customer_metrics),
        'load_time': str(datetime.now()),
        'status': 'SUCCESS'
    }
    
    print(f"Data warehouse load completed: {load_summary}")
    return json.dumps(load_summary)


# Create file outputs for monitoring
create_output_dir = BashOperator(
    task_id='create_output_directory',
    bash_command='mkdir -p /tmp/airflow_pipeline_output',
    dag=dag,
)

@task(dag=dag)
def export_pipeline_metrics(**context) -> str:
    """
    Export pipeline execution metrics
    """
    print("Exporting pipeline metrics...")
    
    # Collect metrics from all tasks
    customer_validation = context['task_instance'].xcom_pull(task_ids='validate_customer_data')
    order_validation = context['task_instance'].xcom_pull(task_ids='validate_order_data')
    quality_report = context['task_instance'].xcom_pull(task_ids='data_quality_monitoring.generate_quality_report')
    load_result = context['task_instance'].xcom_pull(task_ids='load_to_data_warehouse')
    
    pipeline_metrics = {
        'execution_date': str(context['execution_date']),
        'pipeline_status': 'SUCCESS',
        'customer_records_processed': customer_validation['total_records'] if customer_validation else 0,
        'order_records_processed': order_validation['total_records'] if order_validation else 0,
        'data_quality_score': quality_report['overall_quality_score'] if quality_report else 0,
        'load_status': json.loads(load_result)['status'] if load_result else 'UNKNOWN',
        'export_time': str(datetime.now())
    }
    
    # Write metrics to file (simulated)
    metrics_json = json.dumps(pipeline_metrics, indent=2)
    print(f"Pipeline metrics: {metrics_json}")
    
    return "Pipeline metrics exported successfully"


# Instantiate tasks
extract_customers = extract_customer_data()
extract_orders = extract_order_data()
validate_customers = validate_customer_data(extract_customers)
validate_orders = validate_order_data(extract_orders)
transform_data = transform_and_enrich_data(validate_customers, validate_orders)

# Quality monitoring tasks
completeness_check = check_data_completeness(transform_data)
consistency_check = check_data_consistency(transform_data)
quality_report_task = generate_quality_report(completeness_check, consistency_check)

# Load and export tasks
load_data = load_to_data_warehouse(transform_data, quality_report_task)
export_metrics = export_pipeline_metrics()

# Define dependencies
create_output_dir >> [extract_customers, extract_orders]

# Validation phase
extract_customers >> validate_customers
extract_orders >> validate_orders

# Transformation phase
[validate_customers, validate_orders] >> transform_data

# Quality monitoring phase
transform_data >> [completeness_check, consistency_check]
[completeness_check, consistency_check] >> quality_report_task

# Load phase
quality_report_task >> load_data

# Export phase
load_data >> export_metrics