#!/bin/bash

# Quick demonstration of the ETL system capabilities
echo "=== CDC Stream Batch ETL System Demo ==="

echo "📁 Project Structure:"
find . -type f -name "*.py" -o -name "*.yml" -o -name "*.json" -o -name "*.sql" -o -name "*.sh" | grep -v __pycache__ | sort

echo ""
echo "🐳 Docker Services Defined:"
grep -A 1 "  [a-z-]*:" docker-compose.yml | grep -v "^--$" | grep -v "image:" | grep -v "build:"

echo ""
echo "🔧 Environment Variables:"
head -n 10 .env

echo ""
echo "📊 Database Schema:"
echo "Tables: customers, orders, products, order_items"
grep "CREATE TABLE" sql/init.sql | sed 's/CREATE TABLE IF NOT EXISTS /- /' | sed 's/ (//'

echo ""
echo "🚀 To start the system:"
echo "1. ./scripts/start-etl.sh"
echo "2. Wait for services to initialize (~2 minutes)"
echo "3. ./scripts/monitor-etl.sh (to check status)"

echo ""
echo "📈 ETL Processes:"
echo "- Data Generator: Creates mock e-commerce data continuously"
echo "- CDC Processor: Captures and processes database changes"
echo "- Batch Processor: Runs analytics every 60 seconds"
echo "- Stream Processor: Real-time analytics with anomaly detection"

echo ""
echo "🏗️ Architecture Flow:"
echo "PostgreSQL → Debezium → Kafka → Python Processors → Redis"
echo "           ↳ CDC Events ↳ Stream Processing ↳ Analytics Storage"

echo ""
echo "✅ System is ready to deploy!"