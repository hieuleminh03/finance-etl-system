#!/usr/bin/env python3
"""Financial data crawler - fetches stock data and sends to Kafka."""

import os
import time
import json
import logging
import datetime
import pandas as pd
import pytz
import re
import random
import concurrent.futures
from confluent_kafka import Producer
from tqdm import tqdm
from typing import Dict, Any, Optional, List

from fetch_utils import (
    fetch_stock_history,
    fetch_stock_actions,
    fetch_stock_info,
    load_stock_symbols
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crawler")

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker1:9092')
    KAFKA_TOPIC_OHLCV = os.getenv('KAFKA_TOPIC', 'stock_ohlcv')
    KAFKA_TOPIC_ACTIONS = os.getenv('KAFKA_TOPIC_ACTIONS', 'stock_actions')
    KAFKA_TOPIC_INFO = os.getenv('KAFKA_TOPIC_INFO', 'stock_info')
    FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '86400'))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '10'))
    BACKOFF_MAX_TRIES = int(os.getenv('BACKOFF_MAX_TRIES', '5'))
    MIN_RETRY_WAIT = float(os.getenv('MIN_RETRY_WAIT', '1.0'))
    MAX_RETRY_WAIT = float(os.getenv('MAX_RETRY_WAIT', '60.0'))
    ENABLE_CSV_BACKUP = os.getenv('ENABLE_CSV_BACKUP', 'false').lower() == 'true'

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_kafka_producer() -> Producer:
    producer_conf = {'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS}
    return Producer(producer_conf)

def save_to_csv(data: pd.DataFrame, symbol: str, data_type: str = 'history') -> None:
    if not Config.ENABLE_CSV_BACKUP:
        return
        
    try:
        backup_dir = os.path.join('/app/logs', 'data_backup', symbol)
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        filename = os.path.join(backup_dir, f"{symbol}_{data_type}_{timestamp}")
        
        if isinstance(data, pd.DataFrame):
            data.to_csv(f"{filename}.csv", index=False)
        elif isinstance(data, dict):
            with open(f"{filename}.json", 'w') as json_file:
                json.dump(data, json_file, default=str)
        
        logger.info(f"Saved {data_type} data to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving {data_type} data to CSV/JSON: {e}")
def format_date(date_value: Any) -> Optional[str]:
    """Convert date to YYYY-MM-DD format."""
    if pd.isnull(date_value):
        return None

    try:
        # Convert to pandas Timestamp
        ts = pd.to_datetime(date_value)
        # Format to YYYY-MM-DD
        return ts.strftime('%Y-%m-%d')
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not format date {date_value}: {e}")
        # Fallback for non-standard string formats
        if isinstance(date_value, str):
            match = re.search(r'(\d{4}-\d{2}-\d{2})', date_value)
            if match:
                return match.group(1)
        return str(date_value)

def _create_kafka_record(symbol: str, row: pd.Series) -> Optional[Dict[str, Any]]:
    try:
        date_str = format_date(row.get('Date', row.name))
        if not date_str:
            date_str = datetime.date.today().strftime('%Y-%m-%d')

        return {
            'ticker': symbol,
            'date': date_str,
            'open': float(row['Open']),
            'high': float(row['High']),
            'low': float(row['Low']),
            'close': float(row['Close']),
            'volume': int(row.get('Volume', 0)),
            'timestamp': datetime.datetime.now().isoformat()
        }
    except (KeyError, ValueError) as e:
        logger.error(f"Error creating record for {symbol}: {e}")
        return None

def process_historical_data(symbol: str, producer: Producer) -> bool:
    logger.info(f"Processing historical data for {symbol}")
    
    df = fetch_stock_history(symbol, period="max")
    if df is None or df.empty:
        return False
    
    save_to_csv(df, symbol, 'history')
    
    count = 0
    for _, row in df.iterrows():
        record = _create_kafka_record(symbol, row)
        if record:
            json_value = json.dumps(record)
            producer.produce(
                topic=Config.KAFKA_TOPIC_OHLCV,
                key=symbol,
                value=json_value,
                callback=delivery_report
            )
            count += 1
    
    producer.flush()
    logger.info(f"Sent {count} historical data records for {symbol}")
    return True

def _create_actions_record(symbol: str, row: pd.Series) -> Optional[Dict[str, Any]]:
    try:
        date_str = format_date(row.get('date', row.name))
        if not date_str:
            date_str = datetime.date.today().strftime('%Y-%m-%d')

        return {
            'ticker': symbol,
            'date': date_str,
            'dividends': float(row.get('Dividends', 0.0)),
            'stock_splits': float(row.get('Stock Splits', 0.0)),
            'timestamp': datetime.datetime.now().isoformat()
        }
    except (KeyError, ValueError) as e:
        logger.error(f"Error creating actions record for {symbol}: {e}")
        return None

def process_actions_data(symbol: str, producer: Producer) -> bool:
    logger.info(f"Processing actions data for {symbol}")
    
    df = fetch_stock_actions(symbol)
    if df is None or df.empty:
        logger.info(f"No actions data available for {symbol}")
        return False
    
    save_to_csv(df, symbol, 'actions')
    
    count = 0
    for _, row in df.iterrows():
        record = _create_actions_record(symbol, row)
        if record:
            json_value = json.dumps(record)
            producer.produce(
                topic=Config.KAFKA_TOPIC_ACTIONS,
                key=symbol,
                value=json_value,
                callback=delivery_report
            )
            count += 1
            
    producer.flush()
    logger.info(f"Sent {count} actions records for {symbol}")
    return True

def process_company_info(symbol: str, producer: Producer) -> bool:
    logger.info(f"Processing company info for {symbol}")
    
    info = fetch_stock_info(symbol)
    if info is None:
        logger.info(f"No company info available for {symbol}")
        return False
    
    save_to_csv(info, symbol, 'info')
    
    info['timestamp'] = datetime.datetime.now().isoformat()
    
    json_value = json.dumps(info, default=str)
    producer.produce(
        topic=Config.KAFKA_TOPIC_INFO,
        key=symbol,
        value=json_value,
        callback=delivery_report
    )
    
    producer.flush()
    logger.info(f"Sent company info for {symbol}")
    return True

def process_symbol(symbol: str, producer: Producer) -> bool:
    logger.info(f"Processing all data for {symbol}")
    
    try:
        # Chain data processing functions
        process_historical_data(symbol, producer)
        process_actions_data(symbol, producer)
        process_company_info(symbol, producer)
        return True
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {e}")
        return False

def process_symbol_with_retry(symbol: str, producer: Producer) -> bool:
    retry_wait = Config.MIN_RETRY_WAIT
    
    for attempt in range(Config.BACKOFF_MAX_TRIES):
        try:
            return process_symbol(symbol, producer)
        except Exception as e:
            if attempt < Config.BACKOFF_MAX_TRIES - 1:
                logger.warning(f"Attempt {attempt + 1}/{Config.BACKOFF_MAX_TRIES} failed for {symbol}: {e}. Retrying in {retry_wait:.2f}s")
                time.sleep(retry_wait)
                retry_wait = min(retry_wait * 2, Config.MAX_RETRY_WAIT)
            else:
                logger.error(f"All {Config.BACKOFF_MAX_TRIES} attempts failed for {symbol}: {e}")
                return False

def process_symbols_parallel(symbols: List[str], producer: Producer) -> bool:
    logger.info(f"Processing {len(symbols)} symbols in parallel with {Config.MAX_WORKERS} workers")
    
    successful_symbols = 0
    failed_symbols = 0
    
    shuffled_symbols = random.sample(symbols, len(symbols))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        future_to_symbol = {
            executor.submit(process_symbol_with_retry, symbol.strip(), producer): symbol.strip()
            for symbol in shuffled_symbols if symbol.strip()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_symbol), total=len(future_to_symbol), desc="Processing Symbols"):
            symbol = future_to_symbol[future]
            try:
                if future.result():
                    successful_symbols += 1
                else:
                    failed_symbols += 1
            except Exception as e:
                logger.error(f"Exception while processing {symbol}: {e}")
                failed_symbols += 1
    
    logger.info(f"Parallel processing complete. Success: {successful_symbols}, Failed: {failed_symbols}")
    return successful_symbols > 0

def main() -> None:
    logger.info("Starting Financial Data Crawler")
    
    producer = create_kafka_producer()
    
    while True:
        try:
            start_time = datetime.datetime.now()
            logger.info(f"Starting data collection cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            symbols = load_stock_symbols()
            if not symbols:
                logger.error("No stock symbols loaded, skipping cycle.")
                time.sleep(Config.FETCH_INTERVAL)
                continue

            process_symbols_parallel(symbols, producer)
            
            end_time = datetime.datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            logger.info(f"Completed data collection cycle in {processing_time:.2f} seconds.")
            
            wait_time = max(1, Config.FETCH_INTERVAL - processing_time)
            logger.info(f"Waiting {wait_time:.2f} seconds before the next cycle.")
            time.sleep(wait_time)
            
        except Exception as e:
            logger.critical(f"Critical error in main loop: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main()
