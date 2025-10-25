# Windows 11 Troubleshooting Guide for Airflow 3.1.0 Docker Compose

This guide addresses common issues and solutions when running Airflow 3.1.0 with Docker Compose on Windows 11.

## Table of Contents

1. [Docker Desktop Issues](#docker-desktop-issues)
2. [Service Startup Problems](#service-startup-problems)
3. [Volume Mount Issues](#volume-mount-issues)
4. [Network and Port Conflicts](#network-and-port-conflicts)
5. [Performance Issues](#performance-issues)
6. [Authentication Problems](#authentication-problems)
7. [Database Connection Issues](#database-connection-issues)
8. [WSL2 Integration Problems](#wsl2-integration-problems)
9. [File Permission Issues](#file-permission-issues)
10. [Memory and Resource Issues](#memory-and-resource-issues)

## Docker Desktop Issues

### Issue: Docker Desktop Won't Start

**Symptoms:**
- Docker Desktop fails to start
- "Docker Desktop starting..." message persists
- Error: "Docker Desktop - Unexpected WSL error"

**Solutions:**

1. **Restart Docker Desktop Service**
   ```powershell
   # Run as Administrator
   Restart-Service -Name "com.docker.service"
   
   # Or via Services.msc
   # Services > Docker Desktop Service > Restart
   ```

2. **Reset Docker Desktop**
   ```powershell
   # Close Docker Desktop completely
   # Run as Administrator
   & "C:\Program Files\Docker\Docker\Docker Desktop.exe" --reset-to-factory-defaults
   ```

3. **Check WSL2 Installation**
   ```powershell
   # Check WSL version
   wsl --list --verbose
   
   # Update WSL if needed
   wsl --update
   
   # Set WSL2 as default
   wsl --set-default-version 2
   ```

4. **Enable Required Windows Features**
   ```powershell
   # Run as Administrator
   Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux
   Enable-WindowsOptionalFeature -Online -FeatureName VirtualMachinePlatform
   
   # Restart computer after enabling features
   ```

### Issue: Docker Desktop Performance Issues

**Symptoms:**
- Slow container startup
- High CPU/memory usage
- Unresponsive Docker Desktop

**Solutions:**

1. **Optimize Resource Allocation**
   - Docker Desktop > Settings > Resources
   - Memory: Allocate 6-8GB (minimum 4GB)
   - CPU: Allocate 4+ cores
   - Disk image size: 64GB+

2. **Enable WSL2 Backend**
   - Docker Desktop > Settings > General
   - Check "Use WSL 2 based engine"

3. **Optimize WSL2 Memory Usage**
   ```powershell
   # Create .wslconfig in user home directory
   notepad $env:USERPROFILE\.wslconfig
   ```
   
   Add content:
   ```ini
   [wsl2]
   memory=6GB
   processors=4
   swap=2GB
   localhostForwarding=true
   ```

## Service Startup Problems

### Issue: Services Fail to Start

**Symptoms:**
- `docker-compose up` fails
- Services exit with error codes
- "Connection refused" errors

**Diagnostic Commands:**
```powershell
# Check service status
docker-compose ps

# View service logs
docker-compose logs airflow-init
docker-compose logs postgres
docker-compose logs airflow-api-server

# Check Docker system status
docker system info
docker system df
```

**Solutions:**

1. **Clean Docker Environment**
   ```powershell
   # Stop all services
   docker-compose down -v
   
   # Remove unused containers and networks
   docker system prune -f
   
   # Remove unused volumes (careful - this removes data)
   docker volume prune -f
   ```

2. **Check Port Availability**
   ```powershell
   # Check if ports are in use
   netstat -an | findstr :8080    # Airflow web UI
   netstat -an | findstr :5432    # PostgreSQL
   netstat -an | findstr :6379    # Redis
   
   # Kill processes using required ports if needed
   # Find process ID
   netstat -ano | findstr :8080
   # Kill process (replace PID with actual process ID)
   taskkill /PID 1234 /F
   ```

3. **Verify Environment Configuration**
   ```powershell
   # Check .env file exists and is properly formatted
   type .env
   
   # Verify Fernet key is set
   findstr AIRFLOW__CORE__FERNET_KEY .env
   ```

### Issue: Database Initialization Fails

**Symptoms:**
- `airflow-init` service fails
- "Database connection failed" errors
- Migration errors

**Solutions:**

1. **Check PostgreSQL Service**
   ```powershell
   # Check if PostgreSQL is running
   docker-compose logs postgres
   
   # Test database connectivity
   docker-compose exec postgres pg_isready -U airflow
   ```

2. **Reset Database**
   ```powershell
   # Stop services and remove volumes
   docker-compose down -v
   
   # Remove PostgreSQL volume specifically
   docker volume rm airflow-docker-compose_postgres-db-volume
   
   # Restart initialization
   docker-compose up airflow-init
   ```

3. **Manual Database Initialization**
   ```powershell
   # Start only PostgreSQL
   docker-compose up -d postgres
   
   # Wait for PostgreSQL to be ready
   timeout /t 10
   
   # Run initialization manually
   docker-compose run --rm airflow-api-server airflow db migrate
   ```

## Volume Mount Issues

### Issue: Files Not Syncing Between Host and Container

**Symptoms:**
- DAG files not appearing in Airflow
- Changes to files not reflected in containers
- Permission denied errors

**Solutions:**

1. **Check Volume Mount Syntax**
   ```yaml
   # Correct format in docker-compose.yml
   volumes:
     - ./dags:/opt/airflow/dags          # ✓ Correct
     - .\dags:/opt/airflow/dags          # ✗ Windows-specific
     - C:\path\dags:/opt/airflow/dags    # ✗ Absolute Windows path
   ```

2. **Verify File Sharing Settings**
   - Docker Desktop > Settings > Resources > File Sharing
   - Ensure project directory is shared
   - Add drive letters if needed (C:, D:, etc.)

3. **Check File Permissions**
   ```powershell
   # In WSL2 (if using WSL2 integration)
   wsl
   cd /mnt/c/path/to/your/project
   chmod -R 755 dags/
   chmod -R 755 logs/
   chmod -R 755 plugins/
   exit
   ```

4. **Use WSL2 File System**
   ```bash
   # Move project to WSL2 file system for better performance
   # In WSL2 terminal
   cp -r /mnt/c/path/to/project ~/airflow-project
   cd ~/airflow-project
   docker-compose up -d
   ```

### Issue: Log Files Not Accessible

**Symptoms:**
- Cannot view task logs in web UI
- Log directory empty or inaccessible
- Permission errors when accessing logs

**Solutions:**

1. **Create Log Directory**
   ```powershell
   # Ensure logs directory exists
   mkdir logs -Force
   
   # Set appropriate permissions
   icacls logs /grant Everyone:F
   ```

2. **Check Log Configuration**
   ```powershell
   # Verify log volume mount
   docker-compose config | findstr logs
   
   # Check log directory in container
   docker-compose exec airflow-api-server ls -la /opt/airflow/logs
   ```

## Network and Port Conflicts

### Issue: Port Already in Use

**Symptoms:**
- "Port 8080 is already allocated" error
- Cannot access Airflow web UI
- Service fails to bind to port

**Solutions:**

1. **Find and Kill Conflicting Processes**
   ```powershell
   # Find process using port 8080
   netstat -ano | findstr :8080
   
   # Kill the process (replace PID with actual process ID)
   taskkill /PID 1234 /F
   ```

2. **Use Different Ports**
   ```yaml
   # Edit docker-compose.yml
   services:
     airflow-api-server:
       ports:
         - "8081:8080"  # Use port 8081 instead of 8080
   ```

3. **Check Windows Services**
   ```powershell
   # Check for conflicting Windows services
   Get-Service | Where-Object {$_.Status -eq "Running"} | Select-Object Name, DisplayName
   
   # Stop conflicting services if safe to do so
   Stop-Service -Name "ServiceName"
   ```

### Issue: Cannot Access Services from Host

**Symptoms:**
- Cannot reach http://localhost:8080
- Connection timeout errors
- Services running but not accessible

**Solutions:**

1. **Check Windows Firewall**
   ```powershell
   # Temporarily disable Windows Firewall for testing
   # (Re-enable after testing)
   Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled False
   
   # Or add specific firewall rules
   New-NetFirewallRule -DisplayName "Docker Airflow" -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
   ```

2. **Verify Docker Network Configuration**
   ```powershell
   # Check Docker networks
   docker network ls
   
   # Inspect network configuration
   docker network inspect airflow-docker-compose_default
   ```

3. **Test Network Connectivity**
   ```powershell
   # Test from within container
   docker-compose exec airflow-api-server curl -f http://localhost:8080/health
   
   # Test from host
   curl http://localhost:8080/health
   ```

## Performance Issues

### Issue: Slow Container Performance

**Symptoms:**
- Slow DAG parsing
- Long task execution times
- High resource usage

**Solutions:**

1. **Optimize Docker Desktop Settings**
   - Increase memory allocation to 8GB+
   - Allocate more CPU cores (4+)
   - Enable WSL2 backend

2. **Use WSL2 File System**
   ```bash
   # Move project to WSL2 for better I/O performance
   # In WSL2 terminal
   cp -r /mnt/c/path/to/project ~/airflow-project
   cd ~/airflow-project
   ```

3. **Optimize Volume Mounts**
   ```yaml
   # Use cached or delegated volume mounts for better performance
   volumes:
     - ./dags:/opt/airflow/dags:cached
     - ./logs:/opt/airflow/logs:delegated
   ```

4. **Reduce DAG Parsing Frequency**
   ```bash
   # In .env file
   AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300  # 5 minutes instead of default
   AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
   ```

### Issue: High Memory Usage

**Symptoms:**
- Docker Desktop using excessive memory
- System becomes unresponsive
- Out of memory errors

**Solutions:**

1. **Configure WSL2 Memory Limits**
   ```ini
   # In %USERPROFILE%\.wslconfig
   [wsl2]
   memory=6GB
   processors=4
   swap=2GB
   ```

2. **Optimize Airflow Configuration**
   ```bash
   # In .env file
   AIRFLOW__CORE__PARALLELISM=16
   AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=8
   AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
   ```

3. **Use LocalExecutor Instead of CeleryExecutor**
   ```bash
   # In .env file
   AIRFLOW__CORE__EXECUTOR=LocalExecutor
   ```

## Authentication Problems

### Issue: Cannot Login to Web UI

**Symptoms:**
- Login page appears but credentials don't work
- "Invalid username or password" errors
- Redirected back to login page

**Solutions:**

1. **Verify Default Credentials**
   ```bash
   # Simple Auth Manager (default)
   Username: admin
   Password: admin
   
   # Check users.json file
   type config\users.json
   ```

2. **Reset Authentication**
   ```powershell
   # For Simple Auth Manager
   # Edit config/users.json and restart services
   docker-compose restart airflow-api-server
   ```

3. **Check Authentication Configuration**
   ```powershell
   # Verify auth manager setting
   docker-compose exec airflow-api-server printenv | findstr AUTH_MANAGER
   
   # Check service logs for auth errors
   docker-compose logs airflow-api-server | findstr -i auth
   ```

### Issue: API Authentication Fails

**Symptoms:**
- API calls return 401 Unauthorized
- JWT token issues
- Basic auth not working

**Solutions:**

1. **Test Basic Authentication**
   ```powershell
   # Test API with basic auth
   curl -u admin:admin http://localhost:8080/api/v1/dags
   ```

2. **Get JWT Token**
   ```powershell
   # Get JWT token for Simple Auth Manager
   curl -X POST http://localhost:8080/api/v1/auth/login -H "Content-Type: application/json" -d "{\"username\": \"admin\", \"password\": \"admin\"}"
   ```

3. **Check API Configuration**
   ```bash
   # Verify API auth backends in .env
   AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
   ```

## Database Connection Issues

### Issue: Database Connection Refused

**Symptoms:**
- "Connection refused" errors
- "Database not ready" messages
- Services can't connect to PostgreSQL

**Solutions:**

1. **Check PostgreSQL Service**
   ```powershell
   # Check if PostgreSQL container is running
   docker-compose ps postgres
   
   # Check PostgreSQL logs
   docker-compose logs postgres
   
   # Test database connectivity
   docker-compose exec postgres pg_isready -U airflow
   ```

2. **Verify Database Configuration**
   ```bash
   # Check database connection string in .env
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
   ```

3. **Reset Database**
   ```powershell
   # Stop services and remove database volume
   docker-compose down -v
   
   # Start fresh
   docker-compose up airflow-init
   docker-compose up -d
   ```

### Issue: Database Migration Fails

**Symptoms:**
- Migration errors during initialization
- "Table already exists" errors
- Schema version conflicts

**Solutions:**

1. **Clean Database and Reinitialize**
   ```powershell
   # Remove all data and start fresh
   docker-compose down -v
   docker volume prune -f
   docker-compose up airflow-init
   ```

2. **Manual Migration**
   ```powershell
   # Run migration manually
   docker-compose run --rm airflow-api-server airflow db migrate
   
   # Check database version
   docker-compose run --rm airflow-api-server airflow db check
   ```

## WSL2 Integration Problems

### Issue: WSL2 Not Working with Docker

**Symptoms:**
- Docker can't access WSL2
- "WSL 2 installation is incomplete" error
- Performance issues with WSL2

**Solutions:**

1. **Update WSL2**
   ```powershell
   # Update WSL2 kernel
   wsl --update
   
   # Check WSL version
   wsl --list --verbose
   
   # Set WSL2 as default
   wsl --set-default-version 2
   ```

2. **Enable WSL2 Integration in Docker**
   - Docker Desktop > Settings > Resources > WSL Integration
   - Enable integration with your WSL2 distributions

3. **Restart WSL2**
   ```powershell
   # Restart WSL2
   wsl --shutdown
   wsl
   ```

### Issue: File Performance in WSL2

**Symptoms:**
- Slow file operations
- High CPU usage during file access
- Poor Docker volume performance

**Solutions:**

1. **Move Project to WSL2 File System**
   ```bash
   # In WSL2 terminal
   cp -r /mnt/c/path/to/project ~/airflow-project
   cd ~/airflow-project
   docker-compose up -d
   ```

2. **Optimize WSL2 Configuration**
   ```ini
   # In %USERPROFILE%\.wslconfig
   [wsl2]
   memory=6GB
   processors=4
   localhostForwarding=true
   ```

## File Permission Issues

### Issue: Permission Denied Errors

**Symptoms:**
- "Permission denied" when accessing files
- Cannot write to mounted volumes
- File ownership issues

**Solutions:**

1. **Fix File Permissions in WSL2**
   ```bash
   # In WSL2 terminal
   sudo chown -R $USER:$USER ~/airflow-project
   chmod -R 755 ~/airflow-project/dags
   chmod -R 755 ~/airflow-project/logs
   chmod -R 755 ~/airflow-project/plugins
   ```

2. **Set Correct User in Docker Compose**
   ```yaml
   # In docker-compose.yml
   services:
     airflow-api-server:
       user: "50000:0"  # Use airflow user
   ```

3. **Windows File Permissions**
   ```powershell
   # Grant full control to Everyone (for development only)
   icacls dags /grant Everyone:F /T
   icacls logs /grant Everyone:F /T
   icacls plugins /grant Everyone:F /T
   ```

## Memory and Resource Issues

### Issue: Out of Memory Errors

**Symptoms:**
- Containers killed due to memory limits
- "Out of memory" errors in logs
- System becomes unresponsive

**Solutions:**

1. **Increase Docker Memory Allocation**
   - Docker Desktop > Settings > Resources
   - Increase memory to 8GB or more

2. **Configure Container Memory Limits**
   ```yaml
   # In docker-compose.yml
   services:
     airflow-api-server:
       deploy:
         resources:
           limits:
             memory: 2G
           reservations:
             memory: 1G
   ```

3. **Optimize Airflow Configuration**
   ```bash
   # In .env file
   AIRFLOW__CORE__PARALLELISM=8
   AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
   AIRFLOW__SCHEDULER__MAX_THREADS=2
   ```

### Issue: High CPU Usage

**Symptoms:**
- High CPU usage by Docker processes
- System slowdown
- Fan noise from high CPU load

**Solutions:**

1. **Limit CPU Usage**
   ```yaml
   # In docker-compose.yml
   services:
     airflow-scheduler:
       deploy:
         resources:
           limits:
             cpus: '1.0'
   ```

2. **Optimize Scheduler Settings**
   ```bash
   # In .env file
   AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300
   AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
   AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL=1
   ```

## General Troubleshooting Steps

### Diagnostic Commands

```powershell
# System information
systeminfo | findstr /C:"OS Name" /C:"Total Physical Memory"

# Docker information
docker version
docker system info
docker system df

# Service status
docker-compose ps
docker-compose logs --tail=50

# Network information
ipconfig /all
netstat -an | findstr LISTEN

# Process information
tasklist | findstr docker
Get-Process | Where-Object {$_.ProcessName -like "*docker*"}
```

### Log Collection

```powershell
# Collect all logs for troubleshooting
mkdir troubleshooting-logs
docker-compose logs > troubleshooting-logs\docker-compose.log
docker system info > troubleshooting-logs\docker-info.log
systeminfo > troubleshooting-logs\system-info.log
```

### Reset Everything

```powershell
# Complete reset (use as last resort)
docker-compose down -v
docker system prune -a -f
docker volume prune -f

# Reset Docker Desktop
& "C:\Program Files\Docker\Docker\Docker Desktop.exe" --reset-to-factory-defaults

# Restart computer
shutdown /r /t 0
```

This troubleshooting guide should help resolve most common issues when running Airflow 3.1.0 with Docker Compose on Windows 11. If problems persist, check the Docker Desktop logs and Airflow documentation for additional guidance.