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
- At least 6GB RAM available for containers (increased for Airflow)
- Ports 5432, 6379, 8080, 8081, 8083, 9092, 2181 available

### 1. Clone and Setup
```bash
git clone <repository-url>
cd cdc-stream-batch-etl
```

### 2. Start the System

#### Option A: With Airflow Orchestration (Recommended)
```bash
# Start complete system with Airflow 3.0 orchestration
./scripts/start-etl-airflow.sh
```

#### Option B: Traditional Setup (without Airflow)
```bash
# Start all services
./scripts/start-etl.sh

# Or manually:
docker compose up -d
sleep 60
./scripts/setup-debezium.sh
```

### 3. Access Airflow UI
```bash
# Airflow Web UI (after starting with Airflow)
http://localhost:8080
# Login: admin / admin

# Airflow API Server
http://localhost:8081
```

### 4. Monitor the System
```bash
# Check system status (with Airflow)
./scripts/monitor-etl-airflow.sh

# Check system status (traditional)
./scripts/monitor-etl.sh

# View logs
docker compose logs -f
```

## 📊 Components

### Airflow 3.0 Orchestration
Modern workflow orchestration using Apache Airflow 3.0 with:
- **Airflow SDK**: Leveraging `airflow.sdk` for modern DAG development
- **Asset Management**: Data lineage tracking with asset definitions
- **Task Orchestration**: Sophisticated task dependencies and bitwise mapping
- **API Server**: Dedicated API server for external integrations
- **Real-time Monitoring**: Continuous health checks and performance monitoring

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
- Runs scheduled ETL jobs (orchestrated by Airflow)
- Performs data extraction, transformation, and loading
- Generates customer segments and analytics
- Creates product performance insights

### Stream Processor (`stream_processor.py`)
- Real-time streaming analytics
- Anomaly detection (high-value orders, bulk transactions)
- Geographic distribution analysis
- Real-time alerting system

## 🔗 Airflow 3.0 Orchestration Features

### Modern Airflow SDK Usage
- **DAG Definition**: Using `@dag` and `@task` decorators from `airflow.sdk`
- **Asset Management**: Comprehensive data lineage with asset definitions
- **Task Dependencies**: Smart dependency management with inlet/outlet assets
- **Bitwise Operations**: Efficient data filtering using bitwise mapping

### Key DAGs

#### 1. Main ETL Orchestration (`cdc_etl_orchestration.py`)
- **Schedule**: Daily execution with catchup disabled
- **Asset Tracking**: Full data lineage from source tables to analytics caches
- **Task Flow**:
  - Health checks for all dependencies
  - Data generation with asset outlets
  - CDC processing with bitwise filtering
  - Stream analytics for real-time insights
  - Batch ETL for historical analysis
  - Geographic analytics with mapped regions
  - Data quality validation

#### 2. System Monitoring (`etl_monitoring.py`)
- **Schedule**: Every 15 minutes
- **Monitors**: Service health, data flow, performance metrics
- **Alerts**: Automated health scoring and recommendations
- **Components Tracked**:
  - Debezium connector status
  - Kafka topic metrics
  - Redis cache performance
  - Docker container health
  - ETL processor performance

### Asset Definitions
Assets are defined for complete data lineage tracking:

**Source Tables**:
- `customers_table` - Customer information
- `products_table` - Product catalog  
- `orders_table` - Order transactions
- `order_items_table` - Order line items

**CDC Streams**:
- `customers_cdc_events` - Customer change events
- `orders_cdc_events` - Order change events
- `products_cdc_events` - Product change events
- `order_items_cdc_events` - Order item change events

**Analytics Caches**:
- `customer_segments` - Customer segmentation analytics
- `product_performance` - Product performance metrics
- `real_time_metrics` - Live streaming analytics
- `anomaly_alerts` - Anomaly detection results
- `revenue_analytics` - Business revenue metrics
- `geographic_distribution` - Geographic analysis

### API Server Integration
- **Endpoint**: http://localhost:8081
- **Authentication**: Basic auth (admin/admin)
- **Capabilities**:
  - DAG management via REST API
  - Task status monitoring
  - Asset lineage queries
  - Health check endpoints

### Bitwise Mapping Examples
Efficient data processing using bitwise operations:

```python
# Region filtering with bitwise masks
region_bits = {
    'north': 0b0001,  # 1
    'south': 0b0010,  # 2  
    'east': 0b0100,   # 4
    'west': 0b1000,   # 8
}

# CDC event filtering
if use_bitwise_filters and should_filter_event(event, table):
    filtered_count += 1
    continue
```

## 🔧 Configuration

### Environment Variables (`.env`)
```bash
# Database settings
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=source_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Kafka settings
KAFKA_BROKER=kafka:29092

# Redis settings
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=redis123

# Processing settings
BATCH_INTERVAL_SECONDS=60
STREAM_BUFFER_SIZE=1000
MOCK_DATA_RECORDS_PER_BATCH=100
MOCK_DATA_INTERVAL_SECONDS=10
LOG_LEVEL=INFO

# Airflow 3.0 settings
AIRFLOW_UID=50000
AIRFLOW_GID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
```

### Customization
- Modify `sql/init.sql` to change database schema
- Update `config.py` for different configurations
- Adjust Docker Compose resources as needed
- **NEW**: Customize Airflow DAGs in `airflow/dags/`
- **NEW**: Add custom Airflow operators in `airflow/plugins/`
- **NEW**: Modify asset definitions in `airflow/dags/assets.py`

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
├── airflow/                   # Airflow 3.0 orchestration
│   ├── dags/                 # DAG definitions
│   │   ├── assets.py         # Asset definitions for lineage
│   │   ├── cdc_etl_orchestration.py  # Main ETL DAG
│   │   ├── etl_monitoring.py         # System monitoring DAG
│   │   └── airflow_etl_utils.py      # Airflow utilities
│   ├── logs/                 # Airflow logs
│   ├── plugins/              # Custom plugins
│   ├── config/               # Airflow configuration
│   └── requirements.txt      # Airflow dependencies
├── debezium/
│   └── postgres-connector.json # Debezium configuration
└── scripts/
    ├── start-etl.sh          # Traditional system startup
    ├── start-etl-airflow.sh  # Airflow-enhanced startup
    ├── monitor-etl.sh        # Traditional monitoring
    ├── monitor-etl-airflow.sh # Airflow-enhanced monitoring
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
