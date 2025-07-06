import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from events_generator import generate_events

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_kafka_producer():
    """Create a Kafka producer instance with retry configuration"""
    try:
        return KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,
            acks='all',
            batch_size=16384,
            linger_ms=50,
            retry_backoff_ms=500
        )
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic} [{msg.partition}]')

def publish_events(producer, topic_name='web_events', interval_seconds=30, events_per_batch=10):
    """
    Continuously publish events to Kafka topic
    
    Args:
        producer: KafkaProducer instance
        topic_name: Name of the Kafka topic
        interval_seconds: Time to wait between batches
        events_per_batch: Number of events to generate per batch
    """
    logger.info(f"Starting event publisher - Topic: {topic_name}, Batch Size: {events_per_batch}, Interval: {interval_seconds}s")
    
    events_published = 0
    start_time = time.time()
    
    while True:
        try:
            events = generate_events(num_events=events_per_batch)
            batch_start = time.time()
            
            for event in events:
                producer.send(
                    topic_name, 
                    value=event
                ).add_callback(lambda *args: None).add_errback(
                    lambda e: logger.error(f'Message delivery failed: {e}')
                )
            
            # Force sending of all messages
            producer.flush()
            
            events_published += len(events)
            batch_duration = time.time() - batch_start
            total_duration = time.time() - start_time
            
            logger.info(
                f"Batch published successfully - "
                f"Events in batch: {len(events)}, "
                f"Batch duration: {batch_duration:.2f}s, "
                f"Total events: {events_published}, "
                f"Running time: {total_duration:.0f}s"
            )
            
            # Wait for next interval, accounting for processing time
            sleep_time = max(0, interval_seconds - batch_duration)
            if sleep_time > 0:
                logger.debug(f"Waiting {sleep_time:.2f}s until next batch...")
                time.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"Error publishing events: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    producer = None
    try:
        producer = create_kafka_producer()
        publish_events(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if producer:
            producer.close(timeout=5)
            logger.info("Producer closed")