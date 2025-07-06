# Web Traffic Events Generator and Kafka Infrastructure

A system for generating realistic web traffic events and streaming them through Kafka, designed to work with Apache Flink.

## Overview

The system consists of three main components:

1. **Event Generator**: Produces realistic web traffic events with:
   - Real URLs and paths from the website
   - Actual referrers from search engines and social media
   - User and device IDs
   - Timestamps in Flink-compatible format

2. **Kafka Producer**: Streams events to Kafka with:
   - 30-second publishing interval
   - Configurable batch size
   - Comprehensive logging
   - Error handling and retries

3. **Kafka Infrastructure**:
   - Kafka broker with auto-topic creation
   - Zookeeper for coordination
   - Redpanda Console for monitoring
   - Automatic topic setup

## Quick Start

1. Start the Kafka infrastructure:
```bash
docker-compose up -d
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Run the Kafka producer:
```bash
python kafka_producer.py
```

## Infrastructure Details

### Kafka Configuration
- Bootstrap Server: localhost:9092
- Topic: web_events
- Partitions: 3
- Replication Factor: 1
- Message Retention: 24 hours

### Component Ports
- Kafka: 9092
- Zookeeper: 2181
- Redpanda Console: 8080

### Monitoring
Access Redpanda Console at http://localhost:8080 to:
- View topics
- Monitor messages
- Check consumer groups
- View broker metrics

## Event Format

Events are published as JSON messages with this structure:
```json
{
    "url": "/path/to/page",
    "referrer": "https://referring-site.com",
    "user_id": "1234567890123456789",
    "device_id": "9876543210987654321",
    "host": "www.example.com",
    "event_time": "2023-01-09 11:55:28.032000"
}
```

## Configuration Options

### Event Generator
- Configurable number of events per batch
- Real referrers from major search engines
- Various page URLs and paths
- Multiple host domains

### Kafka Producer
- Batch size: 10 events (default)
- Interval: 30 seconds
- Retries: 5
- Acks: all
- Configurable through environment variables

### Docker Services
- Health checks for all services
- Automatic topic creation
- Proper networking isolation
- Resource constraints for production use

## Troubleshooting

1. If Kafka is not accessible:
```bash
docker-compose ps  # Check service status
docker-compose logs kafka  # Check Kafka logs
```

2. If producer fails to connect:
```bash
docker-compose logs kafka-setup  # Verify topic creation
```

3. To reset the environment:
```bash
docker-compose down -v  # Remove containers and volumes
docker-compose up -d   # Start fresh
```

## Stopping the Infrastructure

```bash
# Stop the producer (Ctrl+C)
# Stop the infrastructure
docker-compose down
```

## Notes for Flink Integration

The events are generated in a format compatible with the provided Flink SQL table definition:
- Timestamps follow the format expected by Flink
- JSON format matches the table schema
- Event time is properly formatted for watermarking