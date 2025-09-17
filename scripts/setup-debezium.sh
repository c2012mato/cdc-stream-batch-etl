#!/bin/bash

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
while ! curl -f http://localhost:8083/connector-plugins &>/dev/null; do
    echo "Kafka Connect not ready yet, waiting..."
    sleep 10
done

echo "Kafka Connect is ready!"

# Create Debezium PostgreSQL connector
echo "Creating Debezium PostgreSQL connector..."
curl -X POST \
    -H "Content-Type: application/json" \
    --data @/home/runner/work/cdc-stream-batch-etl/cdc-stream-batch-etl/debezium/postgres-connector.json \
    http://localhost:8083/connectors

echo "Connector creation request sent!"

# Check connector status
echo "Checking connector status..."
sleep 5
curl -X GET http://localhost:8083/connectors/postgres-source-connector/status

echo "Setup complete!"