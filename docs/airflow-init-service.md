# Airflow Init Service Documentation

## Overview

The `airflow-init` service is responsible for initializing the Airflow database and setting up the authentication configuration. This service must run successfully before any other Airflow services can start.

## Features

### Database Initialization
- **Database Connectivity Check**: Verifies PostgreSQL connection with retry logic (30 attempts, 2-second intervals)
- **Database Migration**: Runs `airflow db migrate` command with retry logic (3 attempts, 5-second intervals)
- **Error Handling**: Comprehensive error handling with detailed logging

### Authentication Setup
- **Simple Auth Manager**: Creates `users.json` file for Simple Auth Manager (default in Airflow 3.1.0)
- **Default Admin User**: Sets up admin user with username `admin` and password `admin`
- **Configuration Directory**: Ensures `/opt/airflow/config` directory exists

## Service Configuration

### Dependencies
- **PostgreSQL**: Must be healthy before initialization starts
- **Other Airflow Services**: All other Airflow services depend on successful completion of this service

### Environment Variables
- Inherits all common Airflow environment variables
- `_AIRFLOW_DB_MIGRATE: 'true'` - Enables database migration
- `_AIRFLOW_WWW_USER_CREATE: 'false'` - Disables automatic user creation (handled manually)

### Restart Policy
- `restart: "no"` - Runs once and exits (one-time initialization)

## Usage

### Manual Initialization
To run only the initialization service:
```bash
docker-compose up airflow-init
```

### Full Stack Startup
The initialization service runs automatically when starting all services:
```bash
docker-compose up -d
```

## Initialization Process

1. **Database Connectivity Check**
   - Attempts to connect to PostgreSQL database
   - Retries up to 30 times with 2-second intervals
   - Exits with error if connection fails

2. **Database Migration**
   - Runs `airflow db migrate` command
   - Retries up to 3 times with 5-second intervals
   - Handles migration failures gracefully

3. **Authentication Configuration**
   - Creates `/opt/airflow/config/users.json` file
   - Sets up default admin user credentials
   - Configures Simple Auth Manager

## Error Handling

### Database Connection Errors
- Automatic retry with exponential backoff
- Detailed error logging for troubleshooting
- Graceful failure after maximum retries

### Migration Errors
- Multiple retry attempts for transient failures
- Comprehensive error reporting
- Safe to run multiple times (idempotent)

### Authentication Setup Errors
- Directory creation with proper permissions
- File creation error handling
- Validation of configuration files

## Troubleshooting

### Common Issues

1. **Database Connection Timeout**
   - Ensure PostgreSQL service is running and healthy
   - Check network connectivity between services
   - Verify database credentials in environment variables

2. **Migration Failures**
   - Check database permissions for airflow user
   - Ensure database schema is compatible
   - Review Airflow logs for specific migration errors

3. **Authentication Setup Issues**
   - Verify volume mounts for config directory
   - Check file permissions on host system
   - Ensure Simple Auth Manager is properly configured

### Logs and Debugging

The service provides detailed logging for each step:
- Database connectivity attempts
- Migration progress and results
- Authentication setup status
- Final initialization summary

## Security Considerations

### Default Credentials
- **Username**: `admin`
- **Password**: `admin`
- **⚠️ IMPORTANT**: Change default credentials in production environments

### File Permissions
- Config files are created with appropriate permissions
- Volume mounts should be secured on host system
- Consider using secrets management for production

## Integration with Other Services

### Service Dependencies
All Airflow services depend on successful completion of airflow-init:
- `airflow-api-server`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`
- `airflow-worker` (if using CeleryExecutor)

### Health Checks
The service uses `condition: service_completed_successfully` to ensure proper startup order.

## Production Considerations

1. **Backup Strategy**: Ensure database backups before running migrations
2. **Monitoring**: Monitor initialization logs for failures
3. **Security**: Change default credentials and secure configuration files
4. **Performance**: Consider database connection pooling for large deployments