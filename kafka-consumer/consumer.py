#!/usr/bin/env python3
"""Kafka consumer for stock data - consumes from Kafka and saves to MongoDB."""

import os
import json
import logging
import datetime
import pandas as pd
import pymongo
import pytz
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from collections import defaultdict
from typing import Dict, Any, Optional, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/kafka_consumer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("kafka_consumer")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)

    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
    KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC_OHLCV', 'stock_ohlcv')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'stock_consumer_group')
    KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
    BATCH_MAX_SECONDS = int(os.getenv('BATCH_MAX_SECONDS', '10'))
    
    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

def connect_to_mongodb() -> Optional[pymongo.database.Database]:
    try:
        client = pymongo.MongoClient(
            host=Config.MONGO_HOST,
            port=Config.MONGO_PORT,
            username=Config.MONGO_USERNAME,
            password=Config.MONGO_PASSWORD,
            authSource=Config.MONGO_AUTH_SOURCE,
            serverSelectionTimeoutMS=5000
        )
        client.server_info()
        logger.info(f"Connected to MongoDB at {Config.MONGO_HOST}:{Config.MONGO_PORT}")
        return client[Config.MONGO_DATABASE]
    except pymongo.errors.ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        return None

def create_kafka_consumer() -> Consumer:
    consumer_conf = {
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': Config.KAFKA_GROUP_ID,
        'auto.offset.reset': Config.KAFKA_AUTO_OFFSET_RESET,
        'enable.auto.commit': 'false'
    }
    consumer = Consumer(consumer_conf)
    topics = [Config.KAFKA_TOPIC_OHLCV]
    consumer.subscribe(topics)
    logger.info(f"Subscribed to Kafka topics: {', '.join(topics)}")
    return consumer

def flush_ohlcv_to_mongodb(
    db: pymongo.database.Database,
    ohlcv_batch: Dict[str, List]
) -> int:
    if db is None:
        logger.error("MongoDB connection not available.")
        return 0

    total_flushed = 0
    
    for ticker, docs in ohlcv_batch.items():
        if not docs:
            continue
        try:
            collection = db[f'stock_{ticker}']
            collection.insert_many(docs, ordered=False)
            logger.info(f"Flushed {len(docs)} OHLCV records for {ticker} to {collection.name}.")
            total_flushed += len(docs)
        except Exception as e:
            logger.error(f"Error flushing OHLCV batch for {ticker}: {e}")
    
    ohlcv_batch.clear()
    return total_flushed

def main() -> None:
    logger.info("Starting Kafka Consumer for OHLCV data")

    db = connect_to_mongodb()
    if not db:
        logger.critical("Failed to connect to MongoDB. Exiting.")
        return

    consumer = create_kafka_consumer()
    
    ohlcv_batch = defaultdict(list)
    last_flush_time = datetime.datetime.now()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if (datetime.datetime.now() - last_flush_time).total_seconds() > Config.BATCH_MAX_SECONDS:
                    if flush_ohlcv_to_mongodb(db, ohlcv_batch) > 0:
                        consumer.commit(asynchronous=False)
                    last_flush_time = datetime.datetime.now()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                ticker = data.get('ticker')
                if not ticker:
                    logger.warning(f"Message missing ticker: {data}")
                    continue

                data['consumed_at'] = datetime.datetime.now(pytz.UTC)
                
                topic = msg.topic()
                if topic == Config.KAFKA_TOPIC_OHLCV:
                    ohlcv_batch[ticker].append(data)

                batch_size = sum(len(v) for v in ohlcv_batch.values())

                if batch_size >= Config.BATCH_SIZE:
                    if flush_ohlcv_to_mongodb(db, ohlcv_batch) > 0:
                        consumer.commit(asynchronous=False)
                    last_flush_time = datetime.datetime.now()

            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        flush_ohlcv_to_mongodb(db, ohlcv_batch)
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
