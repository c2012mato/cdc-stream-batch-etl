#!/bin/bash

# Enhanced monitoring script for ETL system with Airflow
echo "=== ETL System with Airflow Status ==="

# Check if services are running
echo "Docker services status:"
docker compose ps

echo ""
echo "=== Airflow Services ==="
echo "Checking Airflow components..."

# Check Airflow webserver
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health | grep -q "200"; then
    echo "✓ Airflow Webserver: Running (http://localhost:8080)"
else
    echo "✗ Airflow Webserver: Not accessible"
fi

# Check Airflow API
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/api/v1/health | grep -q "200"; then
    echo "✓ Airflow API Server: Running (http://localhost:8081)"
else
    echo "✗ Airflow API Server: Not accessible"
fi

# Check Airflow scheduler
SCHEDULER_STATUS=$(docker inspect airflow-scheduler --format='{{.State.Status}}' 2>/dev/null)
if [ "$SCHEDULER_STATUS" = "running" ]; then
    echo "✓ Airflow Scheduler: Running"
else
    echo "✗ Airflow Scheduler: $SCHEDULER_STATUS"
fi

echo ""
echo "=== DAG Status ==="
if command -v curl &> /dev/null && curl -s http://localhost:8080/health &> /dev/null; then
    # Try to get DAG status via API (basic auth needed)
    echo "DAG information available via Airflow UI at http://localhost:8080"
    echo "Login with: admin/admin"
else
    echo "Airflow UI not accessible for DAG status"
fi

echo ""
echo "=== Database Services ==="

# Check PostgreSQL (main)
if docker exec postgres pg_isready -U postgres &> /dev/null; then
    echo "✓ PostgreSQL (main): Ready"
    # Show table count
    TABLE_COUNT=$(docker exec postgres psql -U postgres -d source_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | tr -d ' ')
    echo "  - Tables in source_db: $TABLE_COUNT"
else
    echo "✗ PostgreSQL (main): Not ready"
fi

# Check Airflow database
if docker exec airflow-db pg_isready -U airflow &> /dev/null; then
    echo "✓ PostgreSQL (Airflow): Ready"
else
    echo "✗ PostgreSQL (Airflow): Not ready"
fi

echo ""
echo "=== Message Queue ==="

# Check Kafka
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
    echo "✓ Kafka: Running"
    TOPIC_COUNT=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l)
    echo "  - Active topics: $TOPIC_COUNT"
else
    echo "✗ Kafka: Not accessible"
fi

echo ""
echo "=== Cache and Storage ==="

# Check Redis
if docker exec redis redis-cli --no-auth-warning -a redis123 ping 2>/dev/null | grep -q PONG; then
    echo "✓ Redis: Running"
    KEY_COUNT=$(docker exec redis redis-cli --no-auth-warning -a redis123 dbsize 2>/dev/null)
    echo "  - Cached keys: $KEY_COUNT"
else
    echo "✗ Redis: Not accessible"
fi

echo ""
echo "=== CDC and Processing ==="

# Check Debezium
if curl -s http://localhost:8083/connectors 2>/dev/null | jq . &> /dev/null; then
    echo "✓ Debezium: Running"
    CONNECTOR_COUNT=$(curl -s http://localhost:8083/connectors 2>/dev/null | jq '. | length' 2>/dev/null)
    echo "  - Active connectors: $CONNECTOR_COUNT"
    
    # Check connector status if any exist
    if [ "$CONNECTOR_COUNT" -gt 0 ]; then
        echo "  - Connector status:"
        curl -s http://localhost:8083/connectors 2>/dev/null | jq -r '.[]' | while read connector; do
            STATUS=$(curl -s "http://localhost:8083/connectors/$connector/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
            echo "    - $connector: $STATUS"
        done
    fi
else
    echo "✗ Debezium: Not accessible"
fi

echo ""
echo "=== ETL Processors ==="

# Check processor containers
PROCESSORS=("cdc-processor" "batch-processor" "stream-processor" "data-generator")
for processor in "${PROCESSORS[@]}"; do
    STATUS=$(docker inspect "$processor" --format='{{.State.Status}}' 2>/dev/null)
    if [ "$STATUS" = "running" ]; then
        echo "✓ $processor: Running"
        
        # Get basic stats
        UPTIME=$(docker inspect "$processor" --format='{{.State.StartedAt}}' 2>/dev/null)
        echo "  - Started: $UPTIME"
    else
        echo "✗ $processor: $STATUS"
    fi
done

echo ""
echo "=== Resource Usage ==="

# Memory usage
echo "Memory usage by service:"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}" | grep -E "(airflow|postgres|kafka|redis|debezium|processor|generator)"

echo ""
echo "=== Recent Activity ==="

# Show recent logs (last 5 lines from each critical service)
echo "Recent Airflow Scheduler logs:"
docker compose logs --tail=5 airflow-scheduler 2>/dev/null | tail -5

echo ""
echo "Recent ETL processor logs:"
docker compose logs --tail=3 cdc-processor 2>/dev/null | tail -3
docker compose logs --tail=3 batch-processor 2>/dev/null | tail -3

echo ""
echo "=== Quick Actions ==="
echo "• View Airflow UI: http://localhost:8080 (admin/admin)"
echo "• View Airflow API: http://localhost:8081/api/v1/health"
echo "• Restart Airflow services: docker compose restart airflow-webserver airflow-scheduler"
echo "• Check all logs: docker compose logs -f"
echo "• Stop system: docker compose down"
echo "• Full restart: docker compose down && ./scripts/start-etl-airflow.sh"

echo ""
echo "=== System Health Summary ==="

# Count healthy services
HEALTHY_COUNT=0
TOTAL_COUNT=8

# Check each critical service
if docker exec postgres pg_isready -U postgres &> /dev/null; then ((HEALTHY_COUNT++)); fi
if docker exec airflow-db pg_isready -U airflow &> /dev/null; then ((HEALTHY_COUNT++)); fi
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then ((HEALTHY_COUNT++)); fi
if docker exec redis redis-cli --no-auth-warning -a redis123 ping 2>/dev/null | grep -q PONG; then ((HEALTHY_COUNT++)); fi
if curl -s http://localhost:8083/connectors &> /dev/null; then ((HEALTHY_COUNT++)); fi
if curl -s http://localhost:8080/health &> /dev/null; then ((HEALTHY_COUNT++)); fi
if [ "$(docker inspect airflow-scheduler --format='{{.State.Status}}' 2>/dev/null)" = "running" ]; then ((HEALTHY_COUNT++)); fi
if [ "$(docker inspect cdc-processor --format='{{.State.Status}}' 2>/dev/null)" = "running" ]; then ((HEALTHY_COUNT++)); fi

echo "Overall health: $HEALTHY_COUNT/$TOTAL_COUNT services healthy"

if [ "$HEALTHY_COUNT" -eq "$TOTAL_COUNT" ]; then
    echo "🟢 System Status: All services running normally"
elif [ "$HEALTHY_COUNT" -ge 6 ]; then
    echo "🟡 System Status: Most services running (minor issues)"
else
    echo "🔴 System Status: Multiple services down (requires attention)"
fi