# Airflow 3.1.0 Example DAGs

This directory contains example DAGs that demonstrate various features and capabilities of Apache Airflow 3.1.0. These DAGs are designed for learning and development purposes.

## DAG Overview

### 1. `example_basic_dag.py`
**Purpose**: Introduction to basic Airflow concepts and operators

**Features Demonstrated**:
- Basic DAG structure and configuration
- PythonOperator with simple functions
- BashOperator for shell commands
- DummyOperator for workflow control
- Task dependencies using `>>` operator
- Context usage in Python functions
- Documentation and logging best practices

**Use Case**: Perfect for beginners learning Airflow fundamentals

**Schedule**: Daily (`timedelta(days=1)`)

### 2. `example_advanced_dag.py`
**Purpose**: Advanced Airflow features and patterns

**Features Demonstrated**:
- TaskFlow API with `@task` decorator
- XCom for data passing between tasks
- BranchPythonOperator for conditional workflows
- Task groups for organization (`@task_group`)
- FileSensor for external dependencies
- Trigger rules (`TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`)
- Complex data transformations
- Error handling and retries

**Use Case**: Intermediate to advanced workflows with conditional logic

**Schedule**: Daily (`@daily`)

### 3. `example_async_dag.py`
**Purpose**: Async and deferred task execution

**Features Demonstrated**:
- Async/await patterns in Python tasks
- Deferrable sensors (`DateTimeSensorAsync`, `TimeDeltaSensorAsync`)
- Triggerer service usage
- Long-running task patterns
- External system integration
- Async data processing workflows

**Use Case**: Workflows with time-based delays and async operations

**Schedule**: Hourly (`@hourly`)

### 4. `example_data_pipeline_dag.py`
**Purpose**: Complete ETL data pipeline

**Features Demonstrated**:
- Extract, Transform, Load (ETL) pattern
- Data validation and quality checks
- Multiple data sources integration
- Task groups for quality monitoring
- Data enrichment and metrics calculation
- Error handling in data pipelines
- Pipeline monitoring and metrics export

**Use Case**: Production-ready data processing workflows

**Schedule**: Daily (`@daily`)

### 5. `example_advanced_sensors_dag.py`
**Purpose**: Advanced sensor patterns and custom operators

**Features Demonstrated**:
- Custom deferrable sensors with async capabilities
- HTTP sensors for external API monitoring
- Multi-file pattern sensors with glob matching
- Custom operator implementations
- Dynamic task generation based on sensor results
- File system monitoring with advanced patterns
- External system integration patterns

**Use Case**: Complex workflows with external dependencies and dynamic behavior

**Schedule**: Daily (`@daily`)

### 6. `example_grid_graph_features_dag.py`
**Purpose**: Showcase Airflow 3.1.0 Grid and Graph view enhancements

**Features Demonstrated**:
- Complex task dependencies for Graph view visualization
- Nested task groups with rich metadata
- Conditional branching and trigger rules
- Rich task documentation and display names
- Dynamic task mapping and parallel execution
- Custom task colors and visual organization
- Performance monitoring and metrics visualization

**Use Case**: Demonstrating UI capabilities and complex workflow visualization

**Schedule**: Hourly (`@hourly`)

### 7. `example_modern_async_patterns_dag.py`
**Purpose**: Cutting-edge async patterns and modern Python features

**Features Demonstrated**:
- Advanced async/await patterns with proper error handling
- Concurrent API calls using aiohttp and asyncio
- Async generators for stream processing
- Async context managers and resource management
- Custom deferrable sensors with async operations
- Async database operations with connection pooling
- Modern error handling and retry patterns

**Use Case**: High-performance async workflows and modern Python integration

**Schedule**: Daily (`@daily`)

## Airflow 3.1.0 Specific Features

These DAGs showcase features specific to Airflow 3.1.0:

### New Architecture Components
- **API Server**: Replaces the traditional webserver (demonstrated in all DAGs)
- **DAG Processor**: Separate service for DAG parsing (utilized by all DAGs)
- **Triggerer**: Handles deferred and async tasks (featured in async examples)

### Enhanced TaskFlow API
- Improved `@task` decorator functionality (used throughout examples)
- Better type hints and data passing (demonstrated in data pipeline)
- Simplified XCom usage (shown in advanced examples)
- Task display names and rich metadata (featured in Grid/Graph examples)

### Async and Deferred Tasks
- `DateTimeSensorAsync` and `TimeDeltaSensorAsync` (async DAG examples)
- Custom deferrable sensors (advanced sensors DAG)
- Async Python functions in tasks (modern async patterns DAG)
- Triggerer service integration (all async examples)
- Concurrent operations with asyncio (modern patterns)

### Advanced UI Features
- **Enhanced Grid View**: Rich task metadata, execution details, and performance metrics
- **Improved Graph View**: Complex dependency visualization, task grouping, and branching
- **Task Groups**: Nested organization and visual hierarchy
- **Custom Task Display**: Icons, colors, and enhanced documentation

### Modern Python Integration
- **Async/Await Patterns**: Native async support with proper resource management
- **Context Managers**: Async context managers for resource cleanup
- **Stream Processing**: Async generators and iterators
- **Concurrent Operations**: aiohttp, asyncio, and modern async libraries
- **Error Handling**: Advanced retry patterns and resilient async operations

### Authentication
- Simple Auth Manager (default in 3.1.0)
- JWT token support
- Configuration-based user management

## Running the DAGs

### Prerequisites
1. Airflow 3.1.0 Docker Compose setup running
2. Web UI accessible at http://localhost:8080
3. DAGs directory mounted to `/opt/airflow/dags`

### Activation Steps
1. Copy these DAG files to your `dags/` directory
2. Wait for Airflow to detect the new DAGs (usually 30-60 seconds)
3. Access the Airflow web UI at http://localhost:8080
4. Enable the DAGs you want to run
5. Trigger manual runs or wait for scheduled execution

### Authentication
Default credentials (Simple Auth Manager):
- Username: `admin`
- Password: `admin`

## DAG Configuration

### Environment Variables
The DAGs use standard Airflow configuration. Key settings:

```bash
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
```

### Customization
You can customize the DAGs by:

1. **Modifying schedules**: Change `schedule_interval` in DAG definition
2. **Adjusting retries**: Update `default_args['retries']`
3. **Changing owners**: Update `default_args['owner']`
4. **Adding notifications**: Configure email settings

## Learning Path

### Beginner
1. Start with `example_basic_dag.py`
2. Understand task dependencies and operators
3. Learn about context and XCom

### Intermediate
1. Explore `example_advanced_dag.py`
2. Learn TaskFlow API and task groups
3. Understand conditional workflows

### Advanced
1. Study `example_async_dag.py` for basic async patterns
2. Analyze `example_data_pipeline_dag.py` for ETL workflows
3. Explore `example_advanced_sensors_dag.py` for custom sensors and operators
4. Examine `example_grid_graph_features_dag.py` for UI optimization and complex workflows
5. Master `example_modern_async_patterns_dag.py` for cutting-edge async techniques
6. Implement custom operators, sensors, and async patterns

## Troubleshooting

### Common Issues

1. **DAGs not appearing**:
   - Check DAG syntax with `airflow dags list`
   - Verify file permissions
   - Check Airflow logs for parsing errors

2. **Task failures**:
   - Review task logs in the web UI
   - Check resource availability
   - Verify external dependencies

3. **Import errors**:
   - Ensure all required packages are installed
   - Check Python path and module imports
   - Verify Airflow version compatibility

### Debugging Tips

1. **Use the CLI**:
   ```bash
   # Test DAG parsing
   airflow dags list
   
   # Test specific task
   airflow tasks test example_basic_dag hello_world 2024-01-01
   ```

2. **Check logs**:
   - Task logs: Available in web UI under each task instance
   - Scheduler logs: Check Docker container logs
   - DAG processor logs: Monitor for parsing issues

3. **Validate DAG structure**:
   ```bash
   # Show DAG structure
   airflow dags show example_basic_dag
   
   # List tasks
   airflow tasks list example_basic_dag
   ```

## Best Practices Demonstrated

1. **Documentation**: Comprehensive docstrings and comments
2. **Error Handling**: Proper exception handling and retries
3. **Logging**: Informative log messages for debugging
4. **Type Hints**: Clear function signatures with type annotations
5. **Modularity**: Reusable functions and task groups
6. **Configuration**: Externalized configuration via environment variables
7. **Testing**: Testable task functions with clear inputs/outputs

## Next Steps

After exploring these examples:

1. Create your own DAGs based on these patterns
2. Integrate with your data sources and systems
3. Implement custom operators for specific use cases
4. Set up monitoring and alerting
5. Deploy to production with proper CI/CD practices

## Resources

- [Airflow 3.1.0 Documentation](https://airflow.apache.org/docs/apache-airflow/3.1.0/)
- [TaskFlow API Guide](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Async and Deferred Tasks](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)