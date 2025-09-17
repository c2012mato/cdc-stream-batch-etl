#!/bin/bash

# Monitor ETL system status
echo "=== ETL System Status ==="

# Check if services are running
echo "Docker services status:"
docker compose ps

echo ""
echo "=== Kafka Topics ==="
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "=== Debezium Connector Status ==="
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq '.'

echo ""
echo "=== Redis Stats ==="
docker exec redis redis-cli --no-auth-warning -a redis123 info keyspace

echo ""
echo "=== Recent Logs ==="
echo "Recent application logs (last 20 lines):"
docker compose logs --tail=20 cdc-processor
docker compose logs --tail=20 batch-processor
docker compose logs --tail=20 stream-processor