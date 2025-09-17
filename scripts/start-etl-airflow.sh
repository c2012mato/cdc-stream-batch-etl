#!/bin/bash

# Start ETL system with Airflow orchestration
echo "=== Starting CDC Stream Batch ETL with Airflow Orchestration ==="

# Check prerequisites
echo "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: Docker Compose is not installed"
    exit 1
fi

# Check available memory
MEMORY_GB=$(free -g | awk '/^Mem:/{print $2}')
if [ "$MEMORY_GB" -lt 4 ]; then
    echo "Warning: System has less than 4GB RAM. Some services may fail to start."
fi

# Check required ports
REQUIRED_PORTS=(5432 6379 8083 8084 9092 2181 8080 8081)
for port in "${REQUIRED_PORTS[@]}"; do
    if netstat -tuln | grep ":$port " > /dev/null; then
        echo "Warning: Port $port is already in use"
    fi
done

echo "Prerequisites checked."

# Create required directories
echo "Creating Airflow directories..."
mkdir -p airflow/{dags,logs,plugins,config}

# Set Airflow UID/GID
export AIRFLOW_UID=$(id -u)
export AIRFLOW_GID=$(id -g)

# Start core infrastructure first
echo "Starting core infrastructure..."
docker compose up -d zookeeper kafka postgres redis

echo "Waiting for core services to be ready..."
sleep 30

# Check PostgreSQL
echo "Checking PostgreSQL..."
until docker exec postgres pg_isready -U postgres; do
    echo "Waiting for PostgreSQL..."
    sleep 5
done
echo "PostgreSQL is ready."

# Check Kafka
echo "Checking Kafka..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; do
    echo "Waiting for Kafka..."
    sleep 5
done
echo "Kafka is ready."

# Check Redis
echo "Checking Redis..."
until docker exec redis redis-cli --no-auth-warning -a redis123 ping | grep PONG &> /dev/null; do
    echo "Waiting for Redis..."
    sleep 5
done
echo "Redis is ready."

# Start Airflow database
echo "Starting Airflow database..."
docker compose up -d airflow-db

echo "Waiting for Airflow database..."
sleep 20

# Initialize Airflow
echo "Initializing Airflow..."
docker compose up airflow-init

# Start Airflow services
echo "Starting Airflow services..."
docker compose up -d airflow-webserver airflow-scheduler airflow-api

echo "Waiting for Airflow services..."
sleep 30

# Start Debezium
echo "Starting Debezium..."
docker compose up -d debezium

echo "Waiting for Debezium..."
sleep 30

# Setup Debezium connector
echo "Setting up Debezium connector..."
./scripts/setup-debezium.sh

# Start ETL processors
echo "Starting ETL processors..."
docker compose up -d cdc-processor batch-processor stream-processor data-generator

echo "Waiting for ETL processors..."
sleep 20

# Display status
echo ""
echo "=== System Status ==="
docker compose ps

echo ""
echo "=== Airflow Services ==="
echo "Airflow Webserver: http://localhost:8080 (admin/admin)"
echo "Airflow API Server: http://localhost:8081"

echo ""
echo "=== Health Checks ==="

# Check Airflow webserver
if curl -s http://localhost:8080/health | grep -q "healthy"; then
    echo "✓ Airflow Webserver is healthy"
else
    echo "✗ Airflow Webserver is not healthy"
fi

# Check Debezium
if curl -s http://localhost:8083/connectors | grep -q "\[\]"; then
    echo "✓ Debezium is running (no connectors yet)"
else
    echo "✓ Debezium is running with connectors"
fi

# Check AKHQ
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8084 | grep -q "200"; then
    echo "✓ AKHQ (Kafka UI) is running"
else
    echo "✗ AKHQ (Kafka UI) is not healthy"
fi

echo ""
echo "=== Next Steps ==="
echo "1. Access Airflow UI at http://localhost:8080 (admin/admin)"
echo "2. Access AKHQ (Kafka UI) at http://localhost:8084"
echo "3. Enable the DAGs: 'cdc_stream_batch_etl_orchestration' and 'etl_system_monitoring'"
echo "4. Monitor system status: ./scripts/monitor-etl-airflow.sh"
echo "5. Check logs: docker compose logs -f [service-name]"

echo ""
echo "=== ETL System with Airflow is now running! ==="