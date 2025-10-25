# Airflow 3.1.0 Docker Compose Setup

A complete Docker Compose setup for Apache Airflow 3.1.0 optimized for Windows 11 development.

## Project Structure

```
./
├── dags/                   # Place your DAG files here
├── logs/                   # Airflow logs (auto-generated)
├── plugins/                # Custom Airflow plugins
├── config/                 # Airflow configuration files
├── docker-compose.yml      # Main Docker Compose configuration
├── .env                    # Environment variables (customize this)
├── .env.example           # Environment template
└── README.md              # This file
```

## Quick Start

1. **Prerequisites**
   - Docker Desktop for Windows 11
   - At least 4GB RAM allocated to Docker
   - WSL2 backend enabled (recommended)

2. **Setup**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env file with your settings
   # Generate a new Fernet key:
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

3. **Start Airflow**
   ```bash
   # Initialize database (first time only)
   docker-compose up airflow-init
   
   # Start all services
   docker-compose up -d
   ```

4. **Access Airflow**
   - Web UI: http://localhost:8080
   - Default credentials: admin/admin (Simple Auth)

## Services

- **airflow-api-server**: Web UI and REST API (port 8080)
- **airflow-scheduler**: Task scheduling and monitoring
- **airflow-dag-processor**: DAG file parsing
- **airflow-triggerer**: Deferred and async task handling
- **postgres**: Database backend (PostgreSQL 17)
- **redis**: Message broker (CeleryExecutor only)

## Configuration

### Executors
- **LocalExecutor** (default): Single-node execution
- **CeleryExecutor**: Distributed execution with workers

### Authentication

#### Simple Auth Manager (Default)
- Configuration-based user management
- Users defined in `config/users.json`
- JWT token support for API access
- Lightweight and fast

```bash
# Start with Simple Auth Manager (default)
docker-compose up -d
```



## Windows 11 Notes

- Uses forward slashes for cross-platform volume compatibility
- Optimized for Docker Desktop with WSL2 backend
- Compatible with existing conda environments

## Documentation

### Comprehensive Guides

- **[Complete Usage Guide](docs/usage-guide.md)** - Comprehensive guide covering all aspects of using Airflow 3.1.0 with Docker Compose
- **[DAG Development Best Practices](docs/dag-development-guide.md)** - Best practices for developing DAGs in Airflow 3.1.0
- **[Windows 11 Troubleshooting Guide](docs/windows-troubleshooting-guide.md)** - Solutions for common Windows 11 specific issues

### Authentication Guides

- **[Simple Auth Manager Usage](config/simple-auth-usage.md)** - Using the default Simple Auth Manager with JWT tokens

## Next Steps

This setup is ready for development. Add your DAG files to the `dags/` directory and they will be automatically detected by Airflow.

For detailed usage instructions, DAG development best practices, and troubleshooting help, see the comprehensive documentation guides above.