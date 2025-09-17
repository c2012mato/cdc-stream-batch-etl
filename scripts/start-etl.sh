#!/bin/bash

# Start the ETL system
echo "Starting CDC Stream Batch ETL System..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | xargs)
fi

# Start Docker Compose services
echo "Starting Docker Compose services..."
docker compose up -d

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 60

# Setup Debezium connector
echo "Setting up Debezium connector..."
./scripts/setup-debezium.sh

echo "ETL system is starting up!"
echo "You can monitor the logs with: docker-compose logs -f"
echo "To stop the system: docker compose down"