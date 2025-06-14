#!/usr/bin/env python3
"""MongoDB initialization script - creates collections and indexes."""

import os
import sys
import time
import logging
from typing import Optional, List, Dict
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("mongo-setup")

class Config:
    MONGO_ROOT_USERNAME = os.environ.get('MONGO_ROOT_USERNAME', 'root')
    MONGO_ROOT_PASSWORD = os.environ.get('MONGO_ROOT_PASSWORD', 'example')
    MAX_RETRY_ATTEMPTS = 30
    RETRY_INTERVAL_SECONDS = 2

    MONGO_URL = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@mongodb:27017/?authSource=admin"

def connect_with_retry(mongo_url: str, max_attempts: int = Config.MAX_RETRY_ATTEMPTS, interval: int = Config.RETRY_INTERVAL_SECONDS) -> MongoClient:
    """Connect to MongoDB with retry logic."""
    attempts = 0
    host_details = mongo_url.split('@')[1].split('/')[0]

    while attempts < max_attempts:
        try:
            logger.info(f"Attempting to connect to {host_details} (attempt {attempts + 1}/{max_attempts})")
            client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logger.info(f"Successfully connected to {host_details}")
            return client
        except Exception as e:
            attempts += 1
            logger.warning(f"Failed to connect to {host_details}: {str(e)}")
            time.sleep(interval)

    logger.error(f"Could not connect to {host_details} after {max_attempts} attempts")
    raise Exception(f"Failed to connect to MongoDB at {host_details}")

def setup_database() -> None:
    """
    Set up the finance_data database, including collections and indexes.
    """
    logger.info("=== SETTING UP FINANCE_DATA DATABASE AND COLLECTIONS ===")

    try:
        client = connect_with_retry(Config.MONGO_URL)
        finance_db = client.finance_data

        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]

        setup_collection(
            db=finance_db,
            coll_name="company_info",
            indexes=[{"ticker": 1}]
        )

        setup_collection(
            db=finance_db,
            coll_name="predictions",
            indexes=[
                {"symbol": 1, "prediction_date": 1},
                {"target_date": 1, "predicted_price": 1},
                {"prediction_date": 1, "accuracy_percent": -1},
                {"symbol": 1, "accuracy_percent": -1}
            ]
        )

        for symbol in symbols:
            stock_coll_name = f"stock_{symbol}"
            setup_collection(
                db=finance_db,
                coll_name=stock_coll_name,
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1},
                    {"date": 1, "close": 1},
                    {"date": 1, "volume": 1}
                ]
            )

            actions_coll_name = f"stock_{symbol}_actions"
            setup_collection(
                db=finance_db,
                coll_name=actions_coll_name,
                indexes=[
                    {"date": 1},
                    {"ticker": 1, "date": 1},
                    {"date": 1, "dividends": 1},
                    {"dividends": -1}
                ]
            )

        setup_collection(
            db=finance_db,
            coll_name="analytics_summary",
            indexes=[
                {"date": 1},
                {"most_accurate_model": 1}
            ]
        )

        try:
            finance_db.command({
                "create": "prediction_accuracy_timeseries",
                "viewOn": "predictions",
                "pipeline": [
                    {"$match": {"actual_price": {"$ne": None}}},
                    {"$project": {
                        "symbol": 1,
                        "prediction_date": 1,
                        "target_date": 1,
                        "accuracy_percent": {
                            "$multiply": [
                                100,
                                {"$subtract": [
                                    1,
                                    {"$abs": {"$divide": [
                                        {"$subtract": ["$predicted_price", "$actual_price"]},
                                        "$actual_price"
                                    ]}}
                                ]}
                            ]
                        }
                    }}
                ]
            })
            logger.info("Created prediction_accuracy_timeseries view for analytics")
        except errors.OperationFailure as e:
            if "already exists" in str(e):
                logger.info("prediction_accuracy_timeseries view already exists")
            else:
                raise

        client.close()
        logger.info("=== DATABASE SETUP COMPLETED SUCCESSFULLY ===")
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        raise

def setup_collection(db: any, coll_name: str, indexes: Optional[List[Dict]] = None) -> None:
    """
    Set up a collection with specified indexes.

    Args:
        db (pymongo.database.Database): The database object.
        coll_name (str): The name of the collection to create.
        indexes (list, optional): A list of indexes to create on the collection.
    """
    try:
        try:
            db.create_collection(coll_name)
            logger.info(f"Created collection {coll_name}")
        except errors.CollectionInvalid as e:
            if "already exists" in str(e):
                logger.info(f"Collection {coll_name} already exists")
            else:
                raise

        if indexes:
            for index in indexes:
                db[coll_name].create_index(index)
                logger.info(f"Created index {index} on collection {coll_name}")
    except Exception as e:
        logger.error(f"Error setting up collection {coll_name}: {str(e)}")
        raise

def main() -> None:
    """
    Main function to set up the MongoDB database.
    """
    try:
        logger.info("Starting MongoDB Single Instance Setup")
        logger.info("Waiting for MongoDB service to be available...")
        time.sleep(30)

        import socket
        try:
            logger.info("Resolving hostname mongodb...")
            ip_address = socket.gethostbyname("mongodb")
            logger.info(f"Successfully resolved mongodb to {ip_address}")
        except socket.gaierror as e:
            logger.warning(f"Could not resolve mongodb: {str(e)}")

        setup_database()
        logger.info("=== MONGODB SETUP COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        logger.error(f"MongoDB setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
