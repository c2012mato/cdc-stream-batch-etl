# CDC Stream Batch ETL

A comprehensive ETL (Extract, Transform, Load) system template that demonstrates **Change Data Capture (CDC)**, **Batch Processing**, and **Real-time Streaming** pipelines using modern data engineering tools.

## 🏗️ Architecture

This project implements a complete data pipeline with the following components:

### Technology Stack
- **Apache Kafka** - Message broker for streaming data
- **Apache Zookeeper** - Kafka coordination service  
- **Debezium** - Change Data Capture connector
- **PostgreSQL** - Source database
- **Redis** - Caching and intermediate data storage
- **Python** - ETL processing applications
- **Docker & Docker Compose** - Containerization and orchestration

### Data Flow
1. **Source Data** → PostgreSQL database with sample e-commerce data
2. **CDC Pipeline** → Debezium captures database changes → Kafka topics
3. **Processing Layer**:
   - **CDC Processor**: Real-time change event processing
   - **Batch Processor**: Scheduled bulk data processing and analytics
   - **Stream Processor**: Real-time streaming analytics with anomaly detection
4. **Storage** → Processed data stored in Redis for quick access

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available for containers
- Ports 5432, 6379, 8083, 9092, 2181 available

### 1. Clone and Setup
```bash
git clone <repository-url>
cd cdc-stream-batch-etl
```

### 2. Start the System
```bash
# Start all services
./scripts/start-etl.sh

# Or manually:
docker compose up -d
sleep 60
./scripts/setup-debezium.sh
```

### 3. Monitor the System
```bash
# Check system status
./scripts/monitor-etl.sh

# View logs
docker compose logs -f
```

## 📊 Components

### Data Generator
Continuously generates mock e-commerce data:
- Customer records
- Product catalog
- Order transactions
- Simulates real-world data patterns

### CDC Processor (`cdc_processor.py`)
- Consumes change events from Debezium
- Processes database changes in real-time
- Updates caches and metrics
- Tracks customer, order, and product activities

### Batch Processor (`batch_processor.py`)
- Runs scheduled ETL jobs (default: every 60 seconds)
- Performs data extraction, transformation, and loading
- Generates customer segments and analytics
- Creates product performance insights

### Stream Processor (`stream_processor.py`)
- Real-time streaming analytics
- Anomaly detection (high-value orders, bulk transactions)
- Geographic distribution analysis
- Real-time alerting system

## 🔧 Configuration

### Environment Variables (`.env`)
```bash
# Database settings
POSTGRES_DB=source_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Kafka settings
KAFKA_BROKER=kafka:29092

# Redis settings
REDIS_HOST=redis
REDIS_PASSWORD=redis123

# Processing settings
BATCH_INTERVAL_SECONDS=60
STREAM_BUFFER_SIZE=1000
```

### Customization
- Modify `sql/init.sql` to change database schema
- Update `config.py` for different configurations
- Adjust Docker Compose resources as needed

## 📈 Sample Data

The system includes realistic e-commerce sample data:

### Tables
- **customers**: Customer information and demographics
- **products**: Product catalog with categories and pricing
- **orders**: Customer orders with status tracking
- **order_items**: Individual items within orders

### Generated Metrics
- Customer segmentation (Gold, Silver, Bronze)
- Product performance categories
- Real-time sales velocity
- Geographic distribution
- Revenue analytics

## 🛠️ Development

### Project Structure
```
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment variables
├── sql/
│   └── init.sql               # Database initialization
├── python/
│   ├── requirements.txt       # Python dependencies
│   ├── Dockerfile            # Python app container
│   ├── config.py             # Configuration management
│   ├── utils.py              # Shared utilities
│   ├── data_generator.py     # Mock data generator
│   ├── cdc_processor.py      # CDC event processor
│   ├── batch_processor.py    # Batch ETL processor
│   └── stream_processor.py   # Real-time processor
├── debezium/
│   └── postgres-connector.json # Debezium configuration
└── scripts/
    ├── start-etl.sh          # System startup script
    ├── monitor-etl.sh        # Monitoring script
    └── setup-debezium.sh     # Debezium setup
```

### Adding New Processors
1. Create new Python module in `python/` directory
2. Follow existing patterns for configuration and utilities
3. Add service definition to `docker-compose.yml`
4. Update environment variables as needed

## 🔍 Monitoring

### Check System Status
```bash
# Service health
docker compose ps

# Application logs
docker compose logs -f [service-name]

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Redis data
docker exec redis redis-cli --no-auth-warning -a redis123 keys "*"
```

### Key Metrics Locations (Redis)
- `cdc:stats:*` - CDC processing statistics
- `batch_jobs:*` - Batch processing metrics
- `realtime:metrics:*` - Real-time streaming metrics
- `alerts:*` - System alerts and anomalies

## 🛑 Stopping the System

```bash
# Stop all services
docker compose down

# Remove volumes (clears all data)
docker compose down -v

# Remove images
docker compose down --rmi all
```

## 🎯 Use Cases

This template demonstrates patterns for:

1. **E-commerce Analytics**: Customer behavior, product performance
2. **Real-time Monitoring**: System health, anomaly detection
3. **Data Lake Ingestion**: CDC to data warehouses
4. **Event-Driven Architecture**: Microservices communication
5. **Compliance Tracking**: Audit logs, data lineage

## 📚 Learning Objectives

- Understanding CDC concepts and implementation
- Kafka-based streaming architectures
- Batch vs. stream processing trade-offs
- Docker containerization for data pipelines
- Python-based ETL development
- Real-time analytics and alerting

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-processor`)
3. Commit changes (`git commit -am 'Add new processor'`)
4. Push to branch (`git push origin feature/new-processor`)
5. Create Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Troubleshooting

### Common Issues

**Services not starting**
- Check available memory (need ~4GB)
- Verify ports are not in use
- Check Docker daemon is running

**Debezium connector fails**
- Ensure PostgreSQL is ready before creating connector
- Check database permissions and replication settings
- Verify Kafka Connect is healthy

**Python apps crashing**
- Check environment variables are set correctly
- Verify database connectivity
- Review application logs for specific errors

### Getting Help
- Check application logs: `docker-compose logs [service-name]`
- Monitor system status: `./scripts/monitor-etl.sh`
- Review configuration in `.env` file 
