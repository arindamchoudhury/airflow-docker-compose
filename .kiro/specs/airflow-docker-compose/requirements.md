# Requirements Document

## Introduction

This feature provides a Docker Compose configuration for Apache Airflow 3.1.0, enabling local development and testing of Airflow workflows in a containerized environment. The setup will include all necessary Airflow components and dependencies for a complete local Airflow instance.

## Glossary

- **Airflow**: Apache Airflow, an open-source platform for developing, scheduling, and monitoring workflows
- **Docker Compose**: A tool for defining and running multi-container Docker applications
- **API Server**: The new Airflow 3.x component that replaces the webserver, providing both web UI and REST API functionality
- **Scheduler**: The Airflow component responsible for scheduling and triggering task execution
- **Worker**: The Airflow component that executes tasks (when using CeleryExecutor)
- **Database**: The backend database (PostgreSQL) that stores Airflow metadata
- **Redis**: In-memory data structure store used as a message broker for Celery (optional, only needed for CeleryExecutor)
- **DAG**: Directed Acyclic Graph, representing a workflow in Airflow
- **airflowctl**: New CLI tool in Airflow 3.x for remote management and API interactions
- **Triggerer**: Component that handles deferred tasks and async operations
- **DAG Processor**: Component that parses DAG files and processes DAG definitions

## Requirements

### Requirement 1

**User Story:** As a developer, I want to run Airflow 3.1.0 locally using Docker Compose, so that I can develop and test workflows without installing Airflow directly on my system.

#### Acceptance Criteria

1. WHEN the developer runs docker-compose up, THE Docker_Compose_System SHALL start all required Airflow 3.1.0 services including api-server, scheduler, dag-processor, triggerer, and database
2. THE Docker_Compose_System SHALL use Apache Airflow version 3.1.0 specifically with the new architecture
3. THE Docker_Compose_System SHALL expose the Airflow API server on port 8080 for local web UI and API access
4. THE Docker_Compose_System SHALL persist DAG files from a local directory to the Airflow containers
5. THE Docker_Compose_System SHALL persist database data between container restarts

### Requirement 2

**User Story:** As a developer, I want the Airflow setup to include all necessary components, so that I have a fully functional Airflow environment for development.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL include a PostgreSQL database service for Airflow metadata storage
2. WHERE CeleryExecutor is used, THE Docker_Compose_System SHALL include Redis service for task queue management
3. THE Docker_Compose_System SHALL configure proper service dependencies to ensure correct startup order with Airflow 3.x architecture
4. THE Docker_Compose_System SHALL include environment variables for Airflow 3.x configuration
5. THE Docker_Compose_System SHALL support LocalExecutor by default and optionally CeleryExecutor with worker services

### Requirement 3

**User Story:** As a developer, I want easy configuration and management of the Airflow environment, so that I can quickly start development without complex setup procedures.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL configure authentication using Simple Auth Manager (default in 3.1.0)
2. THE Docker_Compose_System SHALL include volume mounts for DAGs, logs, plugins, and config directories
3. THE Docker_Compose_System SHALL automatically initialize the Airflow database using "airflow db migrate" command (airflow db upgrade is deprecated since 2.7.0)
4. THE Docker_Compose_System SHALL provide clear documentation for starting and stopping services with Airflow 3.x commands
5. WHERE custom configuration is needed, THE Docker_Compose_System SHALL support environment variable overrides compatible with Airflow 3.x

### Requirement 4

**User Story:** As a developer, I want the Docker setup to be compatible with Windows 11 and conda environments, so that it works seamlessly with my development environment.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL use Windows-compatible volume mount syntax
2. THE Docker_Compose_System SHALL work with Docker Desktop on Windows 11
3. THE Docker_Compose_System SHALL not conflict with the existing conda environment named airflow-docker-compose
4. THE Docker_Compose_System SHALL include instructions for Windows-specific considerations
5. THE Docker_Compose_System SHALL use appropriate line endings for Windows compatibility
### Req
uirement 5

**User Story:** As a developer, I want the Docker setup to leverage Airflow 3.x architectural improvements, so that I benefit from the enhanced performance and modern UI features.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL use the new API server component instead of the deprecated webserver
2. THE Docker_Compose_System SHALL include the triggerer service for handling deferred and async tasks
3. THE Docker_Compose_System SHALL include the dag-processor service for parsing DAG files separately from the scheduler
4. THE Docker_Compose_System SHALL support the new React-based UI with improved Grid and Graph views
5. THE Docker_Compose_System SHALL be compatible with both traditional airflow CLI and the new airflowctl commands
6. THE Docker_Compose_System SHALL use FastAPI-based REST API for improved performance over Flask-AppBuilder
#
## Requirement 6

**User Story:** As a developer, I want Simple Auth Manager configuration in Airflow 3.1.0, so that I have a lightweight authentication system for development.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL configure users through environment variables and configuration files using Simple Auth Manager
2. THE Docker_Compose_System SHALL provide clear documentation on Simple Auth Manager configuration
3. THE Docker_Compose_System SHALL include default admin credentials configuration for development use
4. THE Docker_Compose_System SHALL support JWT token-based API access with Simple Auth Manager##
# Requirement 7

**User Story:** As a developer, I want the Docker setup to use the correct database initialization commands for Airflow 3.1.0, so that the database is properly set up with the latest migration practices.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL use "airflow db migrate" instead of the deprecated "airflow db upgrade" command
2. THE Docker_Compose_System SHALL support both fresh database initialization and migration from existing databases
3. THE Docker_Compose_System SHALL include proper error handling for database migration failures
4. THE Docker_Compose_System SHALL ensure database migrations are idempotent and safe to run multiple times
5. THE Docker_Compose_System SHALL provide clear logging output during the database initialization process### 
Requirement 8

**User Story:** As a developer, I want the Docker setup to follow the official Airflow 3.1.0 architecture with proper service separation, so that I have optimal performance and maintainability.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL include separate services for api-server, scheduler, dag-processor, and triggerer as per official Airflow 3.1.0 architecture
2. THE Docker_Compose_System SHALL configure proper health checks for each service to ensure reliability
3. THE Docker_Compose_System SHALL support optional worker services for CeleryExecutor when needed
4. THE Docker_Compose_System SHALL include proper service dependencies and startup order
5. THE Docker_Compose_System SHALL support optional Flower service for Celery monitoring when using CeleryExecutor (available at http://localhost:5555)
6. THE Docker_Compose_System SHALL include an airflow-init service for proper database and environment initialization
###
 Requirement 9

**User Story:** As a developer, I want the Docker setup to use correct Airflow 3.1.0 configuration parameters, so that I don't encounter deprecation warnings and the system uses the proper API configuration.

#### Acceptance Criteria

1. THE Docker_Compose_System SHALL use AIRFLOW__API__SECRET_KEY instead of the deprecated AIRFLOW__WEBSERVER__SECRET_KEY configuration
2. THE Docker_Compose_System SHALL eliminate all deprecation warnings related to configuration parameter migration from webserver to api section
3. THE Docker_Compose_System SHALL maintain backward compatibility while using the correct Airflow 3.x configuration structure
4. THE Docker_Compose_System SHALL provide clear documentation about the configuration parameter changes from Airflow 2.x to 3.x
5. THE Docker_Compose_System SHALL validate that all configuration parameters follow the official Airflow 3.1.0 specification