#!/bin/bash

# Monitor ETL system status
echo "=== ETL System Status ==="

# Check if services are running
echo "Docker services status:"
docker compose ps

echo ""
echo "=== Monitoring UIs ==="

# Check AKHQ
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8084 | grep -q "200"; then
    echo "✓ AKHQ (Kafka UI): Running (http://localhost:8084)"
else
    echo "✗ AKHQ (Kafka UI): Not accessible"
fi

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

echo ""
echo "=== Quick Actions ==="
echo "• View AKHQ (Kafka UI): http://localhost:8084"
echo "• Check Debezium Connectors: http://localhost:8083/connectors"
echo "• Stop system: docker compose down"