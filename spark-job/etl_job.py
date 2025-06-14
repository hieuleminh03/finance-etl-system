#!/usr/bin/env python3
"""Spark ETL job - processes stock data and calculates technical indicators."""

import os
import sys
import time
import logging
import datetime
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, lag, avg, stddev, sum as spark_sum, regexp_extract, to_date, concat_ws, date_format, isnan, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType, ArrayType, DateType, FloatType
from dotenv import load_dotenv
import pymongo

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/etl_job.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_job")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)

    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', '/app/data/raw')
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
    NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS', '4'))

    # Elasticsearch Configuration
    ES_NODES = os.getenv('ES_NODES', 'elasticsearch-master')
    ES_PORT = os.getenv('ES_PORT', '9200')
    ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'processed_stock_data')

def get_symbols_from_mongodb_collections() -> List[str]:
    """
    Connects to MongoDB and lists collections matching the 'stock_<SYMBOL>' pattern
    to dynamically discover symbols to process.
    """
    symbols = []
    try:
        logger.info("Attempting to connect to MongoDB to discover stock symbols...")
        mongo_uri = f"mongodb://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DATABASE}?authSource={Config.MONGO_AUTH_SOURCE}"
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[Config.MONGO_DATABASE]

        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB: {Config.MONGO_HOST} for symbol discovery.")

        collection_names = db.list_collection_names()
        logger.info(f"Found collections: {collection_names}")

        pattern = re.compile(r"^stock_([A-Z.]+)$")

        for name in collection_names:
            match = pattern.match(name)
            if match:
                symbol = match.group(1)
                symbols.append(symbol)

        if symbols:
            logger.info(f"Dynamically discovered symbols from MongoDB: {symbols}")
        else:
            logger.warning("No 'stock_<SYMBOL>' collections found in MongoDB. Falling back to environment variables.")
            env_symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA')
            symbols = [s.strip() for s in env_symbols_str.split(',') if s.strip()]
            logger.info(f"Using fallback symbols from STOCK_SYMBOLS: {symbols}")

        client.close()
    except Exception as e:
        logger.error(f"Error discovering symbols from MongoDB: {e}. Falling back to environment variables.")
        env_symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOG,AMZN,TSLA')
        symbols = [s.strip() for s in env_symbols_str.split(',') if s.strip()]
        logger.info(f"Using fallback symbols: {symbols}")

    return list(set(symbols))

def get_mongo_client() -> Optional[pymongo.MongoClient]:
    try:
        mongo_uri = f"mongodb://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DATABASE}?authSource={Config.MONGO_AUTH_SOURCE}"
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def get_last_watermark(db: pymongo.database.Database, symbol: str) -> Optional[datetime.datetime]:
    try:
        watermark_collection = db['etl_watermarks']
        result = watermark_collection.find_one({'_id': symbol})
        if result and 'last_processed_timestamp' in result:
            return result['last_processed_timestamp']
        return None
    except Exception as e:
        logger.error(f"Error getting watermark for {symbol}: {e}")
        return None

def update_watermark(db: pymongo.database.Database, symbol: str, new_watermark: datetime.datetime) -> None:
    try:
        watermark_collection = db['etl_watermarks']
        watermark_collection.update_one(
            {'_id': symbol},
            {'$set': {'last_processed_timestamp': new_watermark, 'updated_at': datetime.datetime.utcnow()}},
            upsert=True
        )
        logger.info(f"Updated watermark for {symbol} to {new_watermark}")
    except Exception as e:
        logger.error(f"Error updating watermark for {symbol}: {e}")

def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session for distributed processing.

    Returns:
        pyspark.sql.SparkSession: The configured Spark session.
    """
    try:
        spark = SparkSession.builder \
            .appName("Financial Data ETL") \
            .master(Config.SPARK_MASTER) \
            .config("spark.mongodb.input.uri", f"mongodb://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DATABASE}?authSource={Config.MONGO_AUTH_SOURCE}") \
            .config("spark.mongodb.output.uri", f"mongodb://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DATABASE}?authSource={Config.MONGO_AUTH_SOURCE}") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.3.3") \
            .config("spark.sql.shuffle.partitions", Config.NUM_PARTITIONS) \
            .config("spark.default.parallelism", Config.NUM_PARTITIONS) \
            .config("spark.es.nodes", Config.ES_NODES) \
            .config("spark.es.port", Config.ES_PORT) \
            .config("spark.es.nodes.wan.only", "true") \
            .config("spark.es.resource", f"{Config.ES_INDEX_PREFIX}_{{symbol}}") \
            .config("spark.es.mapping.id", "row_id") \
            .config("spark.es.write.operation", "upsert") \
            .config("spark.executor.instances", "1") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Created distributed Spark session successfully")
        return spark

    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        sys.exit(1)

def load_data_from_mongodb(spark: SparkSession, symbol: str, last_watermark: Optional[datetime.datetime] = None) -> Optional[Any]:
    """
    Load raw financial data from MongoDB, optionally filtering by a watermark.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        symbol (str): The stock symbol to load.
        last_watermark (datetime, optional): The last processed timestamp. Defaults to None.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the raw stock data.
    """
    try:
        logger.info(f"Loading data for {symbol} from MongoDB.")

        read_options = {
            "database": Config.MONGO_DATABASE,
            "collection": f"stock_{symbol}",
            "readPreference": "primary"
        }

        if last_watermark:
            pipeline = f"{{ '$match': {{ 'timestamp': {{ '$gt': {{ '$date': '{last_watermark.isoformat()}' }} }} }} }}"
            read_options["pipeline"] = pipeline
            logger.info(f"Applying watermark filter for {symbol}: loading data after {last_watermark}")

        df = spark.read.format("mongo").options(**read_options).load()

        if df.rdd.isEmpty():
            logger.info(f"No new data found for {symbol} since last watermark.")
            return None

        df = df.repartition(Config.NUM_PARTITIONS)

        logger.info(f"Loaded {df.count()} new records for {symbol}.")
        return df

    except Exception as e:
        logger.error(f"Error loading data from MongoDB for {symbol}: {str(e)}")
        return None

def clean_and_prepare_data(df: Optional[Any], symbol: str) -> Optional[Any]:
    """
    Clean and prepare raw financial data.

    Args:
        df (pyspark.sql.DataFrame): A DataFrame containing raw stock data.
        symbol (str): The stock symbol.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with cleaned and prepared data.
    """
    if df is None or df.count() == 0:
        logger.warning(f"No data to clean for {symbol}")
        return None

    try:
        logger.info(f"Cleaning and preparing data for {symbol}")

        required_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        missing_cols = [col_name for col_name in required_cols if col_name not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in DataFrame: {missing_cols}")

        if "date" in df.columns:
            df = df.withColumn(
                "trading_date",
                when(
                    col("date").rlike("\\d{4}-\\d{2}-\\d{2}"),
                    regexp_extract(col("date"), "(\\d{4}-\\d{2}-\\d{2})", 1)
                ).otherwise(None)
            )
            df = df.withColumn("trading_date", to_date(col("trading_date"), "yyyy-MM-dd"))

            null_dates = df.filter(col("trading_date").isNull()).count()
            logger.info(f"Records with null trading_date after extraction: {null_dates}")

            if null_dates > df.count() * 0.5 and "timestamp" in df.columns:
                logger.warning("Date extraction failed for most records. Using timestamp as fallback.")
                df = df.withColumn("trading_date", to_date(col("timestamp")))

        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.filter(col(col_name).isNotNull())

        if "trading_date" in df.columns:
            df = df.orderBy("trading_date")
        elif "date" in df.columns:
            df = df.orderBy("date")

        if "symbol" not in df.columns:
            df = df.withColumn("symbol", lit(symbol))

        df = df.withColumn("row_id", expr("uuid()"))
        logger.info(f"Data cleaning completed for {symbol}")
        return df

    except Exception as e:
        logger.error(f"Error cleaning data for {symbol}: {str(e)}")
        return None

def get_base_window(df: Any) -> Any:
    if "trading_date" in df.columns:
        return Window.partitionBy("symbol").orderBy("trading_date")
    else:
        return Window.partitionBy("symbol").orderBy("date")

def calculate_technical_indicators(df: Optional[Any]) -> Optional[Any]:
    """
    Calculate technical indicators for stock data.

    Args:
        df (pyspark.sql.DataFrame): A DataFrame containing cleaned stock data.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with added technical indicators.
    """
    if df is None or df.count() == 0:
        return None

    try:
        logger.info("Calculating technical indicators")

        base_window = get_base_window(df)

        df = df.withColumn("sma_5", avg("close").over(base_window.rowsBetween(-4, 0)))
        df = df.withColumn("sma_20", avg("close").over(base_window.rowsBetween(-19, 0)))
        df = df.withColumn("sma_50", avg("close").over(base_window.rowsBetween(-49, 0)))
        df = df.withColumn("sma_200", avg("close").over(base_window.rowsBetween(-199, 0)))

        df = df.withColumn("_ema_12", avg("close").over(base_window.rowsBetween(-11, 0)))
        df = df.withColumn("_ema_26", avg("close").over(base_window.rowsBetween(-25, 0)))
        df = df.withColumn("macd", col("_ema_12") - col("_ema_26"))
        df = df.withColumn("signal_line", avg("macd").over(base_window.rowsBetween(-8, 0)))
        df = df.withColumn("macd_histogram", col("macd") - col("signal_line"))

        df = df.withColumn("bb_middle", avg("close").over(base_window.rowsBetween(-19, 0)))
        df = df.withColumn("_bb_stddev", stddev("close").over(base_window.rowsBetween(-19, 0)))
        df = df.withColumn("bb_upper", col("bb_middle") + (col("_bb_stddev") * lit(2)))
        df = df.withColumn("bb_lower", col("bb_middle") - (col("_bb_stddev") * lit(2)))

        df = df.withColumn("_prev_close", lag("close", 1).over(base_window))
        df = df.withColumn("_price_change", col("close") - col("_prev_close"))
        df = df.withColumn("_gain", when(col("_price_change") > 0, col("_price_change")).otherwise(0))
        df = df.withColumn("_loss", when(col("_price_change") < 0, -col("_price_change")).otherwise(0))

        df = df.withColumn("_avg_gain", avg("_gain").over(base_window.rowsBetween(-13, 0)))
        df = df.withColumn("_avg_loss", avg("_loss").over(base_window.rowsBetween(-13, 0)))
        df = df.withColumn("_rs", when(col("_avg_loss") != 0, col("_avg_gain") / col("_avg_loss")).otherwise(lit(100)))
        df = df.withColumn("rsi", lit(100) - (lit(100) / (lit(1) + col("_rs"))))

        df = df.withColumn("_volume_sign",
                        when(col("_price_change") > 0, col("volume"))
                        .when(col("_price_change") < 0, -col("volume"))
                        .otherwise(0))
        df = df.withColumn("obv", spark_sum("_volume_sign").over(base_window.rowsBetween(Window.unboundedPreceding, 0)))

        df = df.withColumn("day_change_pct", (col("close") - col("_prev_close")) / col("_prev_close") * 100)

        df = df.withColumn("_prev_5d_close", lag("close", 5).over(base_window))
        df = df.withColumn("week_change_pct",
                        when(col("_prev_5d_close").isNotNull(),
                            (col("close") - col("_prev_5d_close")) / col("_prev_5d_close") * 100)
                        .otherwise(lit(0)))

        df = df.withColumn("_prev_20d_close", lag("close", 20).over(base_window))
        df = df.withColumn("month_change_pct",
                        when(col("_prev_20d_close").isNotNull(),
                            (col("close") - col("_prev_20d_close")) / col("_prev_20d_close") * 100)
                        .otherwise(lit(0)))

        cols_to_drop = [col_name for col_name in df.columns if col_name.startswith("_")]
        df = df.drop(*cols_to_drop)

        final_indicators = [
            "sma_5", "sma_20", "sma_50", "sma_200",
            "macd", "signal_line", "macd_histogram",
            "bb_middle", "bb_upper", "bb_lower",
            "rsi", "obv", "day_change_pct", "week_change_pct", "month_change_pct"
        ]

        for col_name in final_indicators:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            else:
                df = df.withColumn(col_name, lit(None).cast(DoubleType()))

        logger.info("Technical indicators calculated and optimized")
        return df

    except Exception as e:
        logger.error(f"Error calculating technical indicators: {str(e)}")
        return df


def write_processed_data_to_mongo_and_elasticsearch(df: Optional[Any], symbol: str) -> bool:
    """
    Writes processed data to MongoDB and Elasticsearch.

    Args:
        df (pyspark.sql.DataFrame): A DataFrame containing processed data.
        symbol (str): The stock symbol.

    Returns:
        bool: True if the write operation was successful, False otherwise.
    """
    if df is None or df.count() == 0:
        logger.warning(f"No data to write for {symbol}")
        return False

    try:
        processed_collection_name = f"processed_{symbol}"

        logger.info(f"Writing processed data for {symbol} to MongoDB collection: {processed_collection_name}")

        df_to_write = df

        if "trading_date" not in df_to_write.columns and "date" in df_to_write.columns:
            df_to_write = df_to_write.withColumn("trading_date", col("date"))

        if "trading_date" in df_to_write.columns:
            logger.info("Using composite key of symbol and trading_date for MongoDB write.")
            df_to_write = df_to_write.withColumn("symbol_date_key",
                                              concat_ws("_", col("symbol"),
                                                        date_format(col("trading_date"), "yyyy-MM-dd")))
            df_to_write.write \
              .format("mongo") \
              .mode("append") \
              .option("database", Config.MONGO_DATABASE) \
              .option("collection", processed_collection_name) \
              .save()
        else:
            logger.warning("No trading_date column available. Using simple overwrite by symbol.")
            df_to_write.write \
              .format("mongo") \
              .mode("append") \
              .option("database", Config.MONGO_DATABASE) \
              .option("collection", processed_collection_name) \
              .save()

        logger.info(f"Successfully wrote {df_to_write.count()} records to MongoDB collection: {processed_collection_name}")

        try:
            logger.info(f"Writing processed data for {symbol} to Elasticsearch index: {Config.ES_INDEX_PREFIX}_{symbol}")
            if "symbol" not in df_to_write.columns:
                df_with_symbol_for_es = df_to_write.withColumn("symbol", lit(symbol))
            else:
                df_with_symbol_for_es = df_to_write

            if "_id" in df_with_symbol_for_es.columns:
                df_for_es = df_with_symbol_for_es.drop("_id")
                logger.info("Dropped MongoDB '_id' column before writing to Elasticsearch.")
            else:
                df_for_es = df_with_symbol_for_es

            if "trading_date" in df_for_es.columns:
                df_for_es = df_for_es.withColumn("es_id",
                                               concat_ws("_", col("symbol"),
                                                        date_format(col("trading_date"), "yyyy-MM-dd")))

            logger.info(f"Converting NaN to null for numeric fields before writing to ES for symbol: {symbol}")
            numeric_cols_for_nan_conversion = [
                "open", "high", "low", "close", "volume",
                "sma_5", "sma_20", "sma_50", "sma_200",
                "ema_12", "ema_26", "macd", "signal_line", "macd_histogram",
                "bb_middle", "bb_stddev", "bb_upper", "bb_lower",
                "prev_close",
                "price_change", "gain", "loss",
                "avg_gain", "avg_loss", "rs", "rsi",
                "volume_sign", "obv",
                "day_change_pct",
                "prev_5d_close", "week_change_pct",
                "prev_20d_close", "month_change_pct"
            ]

            df_for_es_final = df_for_es
            for field_name in numeric_cols_for_nan_conversion:
                if field_name in df_for_es_final.columns:
                    current_type = df_for_es_final.schema[field_name].dataType
                    if isinstance(current_type, (DoubleType, FloatType)):
                        df_for_es_final = df_for_es_final.withColumn(field_name,
                                                                     when(isnan(col(field_name)), lit(None).cast(DoubleType()))
                                                                     .otherwise(col(field_name)))
                    elif isinstance(current_type, StringType):
                        df_for_es_final = df_for_es_final.withColumn(field_name,
                                                                     when(col(field_name) == "NaN", lit(None).cast(DoubleType()))
                                                                     .otherwise(col(field_name).cast(DoubleType())))

            logger.info(f"Completed NaN to null conversion for symbol: {symbol}")

            df_for_es_final.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", f"{Config.ES_INDEX_PREFIX}_{symbol.lower()}") \
                .option("es.mapping.id", "es_id" if "es_id" in df_for_es_final.columns else "row_id") \
                .option("es.spark.dataframe.write.null", "true") \
                .mode("overwrite") \
                .save()

            logger.info(f"Successfully wrote {df_for_es_final.count()} records to Elasticsearch index: {Config.ES_INDEX_PREFIX}_{symbol.lower()}")

        except Exception as es_ex:
            logger.error(f"Error writing data to Elasticsearch for {symbol}: {str(es_ex)}")

        return True

    except Exception as e:
        logger.error(f"Error writing processed data for {symbol}: {str(e)}")
        return False

def process_symbol(spark: SparkSession, symbol: str, last_watermark: Optional[datetime.datetime]) -> tuple[bool, Optional[datetime.datetime]]:
    """
    Process a single stock symbol using distributed computing.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        symbol (str): The stock symbol to process.
        last_watermark (datetime): The last processed timestamp.

    Returns:
        tuple: A tuple containing a boolean indicating success and the new watermark datetime.
    """
    try:
        logger.info(f"Processing symbol {symbol} using distributed computing")

        raw_df = load_data_from_mongodb(spark, symbol, last_watermark)
        if raw_df is None or raw_df.rdd.isEmpty():
            return True, None

        raw_df.cache()

        new_watermark_row = raw_df.agg(spark_max("timestamp")).first()
        new_watermark = new_watermark_row[0] if new_watermark_row else None

        clean_df = clean_and_prepare_data(raw_df, symbol)
        if clean_df is None:
            return False, None

        raw_df.unpersist()
        clean_df.cache()

        tech_df = calculate_technical_indicators(clean_df)
        if tech_df is None:
            return False, None

        clean_df.unpersist()
        tech_df.cache()

        success = write_processed_data_to_mongo_and_elasticsearch(tech_df, symbol)

        tech_df.unpersist()

        if success:
            return True, new_watermark
        else:
            return False, None

    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {str(e)}")
        return False, None

def main() -> None:
    logger.info("Starting Distributed Financial Data ETL Job")

    spark = create_spark_session()
    mongo_client = get_mongo_client()

    if not mongo_client:
        logger.error("Could not connect to MongoDB. Aborting ETL job.")
        spark.stop()
        return

    db = mongo_client[Config.MONGO_DATABASE]

    discovered_symbols = get_symbols_from_mongodb_collections()

    if not discovered_symbols:
        logger.error("No symbols to process. Exiting ETL job.")
        spark.stop()
        mongo_client.close()
        return

    logger.info(f"ETL job will process the following symbols: {discovered_symbols}")

    results = []
    for symbol in discovered_symbols:
        logger.info(f"Initiating processing for symbol: {symbol}")
        last_watermark = get_last_watermark(db, symbol)

        success, new_watermark = process_symbol(spark, symbol, last_watermark)

        if success and new_watermark:
            update_watermark(db, symbol, new_watermark)

        results.append((symbol, success))

    success_count = sum(1 for _, success in results if success)

    for symbol, success in results:
        logger.info(f"Symbol {symbol}: {'Success' if success else 'Failed'}")

    logger.info(f"Distributed ETL job completed. Successfully processed {success_count}/{len(discovered_symbols)} symbols")

    spark.stop()
    mongo_client.close()
    logger.info("Distributed Spark session and MongoDB connection stopped")

if __name__ == "__main__":
    main()
