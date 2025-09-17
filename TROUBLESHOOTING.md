# Troubleshooting Guide

## Common Issues and Solutions

### 1. Services Won't Start

**Problem**: Docker Compose services fail to start
```bash
docker compose up -d
# Some services exit with errors
```

**Solutions**:
- Check available memory: `free -h` (need at least 4GB)
- Check port conflicts: `netstat -tlnp | grep -E '(5432|6379|8083|9092|2181)'`
- Check Docker daemon: `docker info`
- Review logs: `docker compose logs [service-name]`

### 2. Debezium Connector Fails

**Problem**: Connector creation fails or shows error status
```bash
curl http://localhost:8083/connectors/postgres-source-connector/status
# Shows FAILED state
```

**Solutions**:
- Wait for PostgreSQL to be fully ready (check with `docker compose logs postgres`)
- Verify Kafka Connect is healthy: `curl http://localhost:8083/connector-plugins`
- Check PostgreSQL permissions and logical replication settings
- Recreate connector: 
  ```bash
  curl -X DELETE http://localhost:8083/connectors/postgres-source-connector
  ./scripts/setup-debezium.sh
  ```

### 3. Python Applications Crashing

**Problem**: ETL processors exit with import or connection errors

**Solutions**:
- Check environment variables are set: `docker compose exec cdc-processor env | grep -E '(POSTGRES|KAFKA|REDIS)'`
- Verify database connectivity: `docker compose exec postgres psql -U postgres -d source_db -c '\dt'`
- Test Redis connection: `docker exec redis redis-cli --no-auth-warning -a redis123 ping`
- Check Kafka topics: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

### 4. No Data Flowing

**Problem**: No CDC events or data processing happening

**Solutions**:
- Check if data generator is creating records: `docker compose logs data-generator`
- Verify Kafka topics have messages: 
  ```bash
  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic dbserver1.public.customers --from-beginning --max-messages 5
  ```
- Check Debezium connector status and logs
- Restart the data generator: `docker compose restart data-generator`

### 5. Memory Issues

**Problem**: Services crashing due to insufficient memory

**Solutions**:
- Reduce buffer sizes in `.env`:
  ```
  STREAM_BUFFER_SIZE=500
  MOCK_DATA_RECORDS_PER_BATCH=50
  ```
- Limit Docker Compose resources:
  ```yaml
  deploy:
    resources:
      limits:
        memory: 512M
  ```

### 6. Network Connectivity Issues

**Problem**: Services can't communicate with each other

**Solutions**:
- Verify all services are on the same network: `docker network ls`
- Check service names resolve: `docker compose exec cdc-processor nslookup postgres`
- Use internal service names (not localhost) in configuration

## Debugging Commands

### Check Service Health
```bash
# All services status
docker compose ps

# Specific service logs
docker compose logs -f [service-name]

# Execute commands in containers
docker compose exec postgres psql -U postgres -d source_db
docker compose exec redis redis-cli --no-auth-warning -a redis123
```

### Monitor Data Flow
```bash
# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume Kafka messages
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic [topic-name] --from-beginning

# Redis data
docker exec redis redis-cli --no-auth-warning -a redis123 keys "*"
docker exec redis redis-cli --no-auth-warning -a redis123 get "key-name"
```

### Performance Monitoring
```bash
# Container resource usage
docker stats

# Database connections
docker compose exec postgres psql -U postgres -c "SELECT * FROM pg_stat_activity;"

# Redis memory usage
docker exec redis redis-cli --no-auth-warning -a redis123 info memory
```

## Reset and Clean Start

If you need to completely reset the system:

```bash
# Stop all services
docker compose down

# Remove all data (WARNING: This deletes all data!)
docker compose down -v

# Remove built images
docker compose down --rmi local

# Clean start
./scripts/start-etl.sh
```

## Performance Tuning

### For Development
- Reduce batch intervals and buffer sizes
- Use smaller datasets
- Limit log verbosity

### For Production
- Increase memory limits
- Use external volumes for persistence
- Implement proper monitoring and alerting
- Add health checks to services

## Getting Help

1. **Check Logs**: Always start with `docker compose logs [service]`
2. **Verify Configuration**: Ensure `.env` variables are correct
3. **Test Components**: Use the validation script: `cd python && python3 validate.py`
4. **Monitor System**: Use the monitoring script: `./scripts/monitor-etl.sh`