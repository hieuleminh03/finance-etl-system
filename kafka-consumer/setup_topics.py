#!/usr/bin/env python3
"""Kafka topic setup script - creates required topics."""

import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from typing import List, Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("kafka_setup")

class Config:
    load_dotenv()
    
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
    TOPICS = {
        os.getenv('KAFKA_TOPIC_OHLCV', 'stock_ohlcv'): {
            "num_partitions": 3,
            "replication_factor": 3,
            "config": {
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),
                'cleanup.policy': 'delete'
            }
        },
        'financial_data': {
            "num_partitions": 3,
            "replication_factor": 3,
            "config": {
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),
                'cleanup.policy': 'delete'
            }
        },
        'price_updates': {
            "num_partitions": 3,
            "replication_factor": 3,
            "config": {
                'retention.ms': str(24 * 60 * 60 * 1000),
                'cleanup.policy': 'delete'
            }
        }
    }

def create_topics() -> None:
    """
    Creates Kafka topics if they do not already exist.
    """
    admin_client = AdminClient({'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS})
    existing_topics = admin_client.list_topics().topics
    
    topics_to_create = [
        NewTopic(topic_name, **params)
        for topic_name, params in Config.TOPICS.items()
        if topic_name not in existing_topics
    ]

    if not topics_to_create:
        logger.info("All required topics already exist.")
        return

    futures = admin_client.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic}': {e}")

def main() -> None:
    """
    Main function to set up Kafka topics.
    """
    logger.info("Starting Kafka topic setup.")
    create_topics()
    logger.info("Kafka topic setup completed.")

if __name__ == "__main__":
    main()
