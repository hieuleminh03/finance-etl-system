#!/usr/bin/env python3
"""ML scheduler for automated training and prediction tasks."""

import os
import time
import logging
import datetime
import schedule
from typing import List, Dict, Any, Optional
from ml_pipeline import MLPipeline
import pymongo
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/ml_scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_scheduler")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)

    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'finance_user')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'FinanceUserPass2024!')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

    TRAINING_SCHEDULE = os.getenv('TRAINING_SCHEDULE', '02:00')
    PREDICTION_SCHEDULE = os.getenv('PREDICTION_SCHEDULE', '09:00')
    RETRAIN_INTERVAL_DAYS = int(os.getenv('RETRAIN_INTERVAL_DAYS', '7'))
    RUN_DEMO_ON_START = os.getenv("RUN_DEMO_ON_START", "false").lower() == "true"

class MLScheduler:
    
    def __init__(self) -> None:
        self.pipeline = MLPipeline()
        self.db = self.pipeline.db
    
    def get_symbols_needing_training(self) -> List[str]:
        if self.db is None:
            return []

        symbols_to_train = []
        processed_symbols = [name.replace('processed_', '') for name in self.db.list_collection_names() if name.startswith('processed_')]
        
        for symbol in processed_symbols:
            latest_model = self.db['ml_models'].find_one({'symbol': symbol}, sort=[('training_date', -1)])
            
            if not latest_model:
                symbols_to_train.append(symbol)
                continue

            last_trained_date = datetime.datetime.fromisoformat(latest_model['training_date'])
            if (datetime.datetime.now() - last_trained_date).days >= Config.RETRAIN_INTERVAL_DAYS:
                symbols_to_train.append(symbol)
                
        logger.info(f"Found {len(symbols_to_train)} symbols needing training.")
        return symbols_to_train
    
    def get_symbols_for_prediction(self) -> List[str]:
        if self.db is None:
            return []
            
        return self.db['ml_models'].distinct('symbol')
    
    def scheduled_training(self) -> None:
        logger.info("Starting scheduled model training.")
        symbols_to_train = self.get_symbols_needing_training()
        
        if not symbols_to_train:
            logger.info("No symbols require training at this time.")
            return
            
        self.pipeline.batch_train_models(symbols_to_train)
    
    def scheduled_prediction(self) -> None:
        """Scheduled prediction task"""
        try:
            logger.info("Starting scheduled predictions")
            start_time = datetime.datetime.now()
            
            # Get symbols for prediction
            symbols = self.get_symbols_for_prediction()
            
            if not symbols:
                logger.info("No symbols available for prediction")
                return
            
            # Make predictions
            predictions = self.pipeline.batch_predict(symbols)
            
            # Log results
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Scheduled predictions completed in {duration:.1f}s: {len(predictions)} predictions made")
            
            # Log summary of predictions
            for pred in predictions:
                current_price = pred['current_price']
                predicted_price = pred['predicted_price']
                change_pct = ((predicted_price - current_price) / current_price) * 100
                logger.info(f"Prediction: {pred['symbol']} ${current_price:.2f} → ${predicted_price:.2f} ({change_pct:+.2f}%)")
            
            # Update prediction log
            prediction_log = {
                'task': 'scheduled_prediction',
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': duration,
                'symbols_processed': symbols,
                'predictions_made': len(predictions),
                'predictions': predictions
            }
            
            # Save to MongoDB
            if self.db is not None:
                self.db['ml_prediction_logs'].insert_one(prediction_log)
            
        except Exception as e:
            logger.error(f"Error in scheduled prediction: {e}")
    
    def model_performance_monitoring(self) -> None:
        """Monitor model performance and alert on issues"""
        try:
            if self.db is None:
                return
            
            logger.info("Starting model performance monitoring")
            
            models_collection = self.db['ml_models']
            alerts = []
            
            # Get all latest models
            pipeline_agg = [
                {'$sort': {'created_at': -1}},
                {'$group': {
                    '_id': '$symbol',
                    'latest_model': {'$first': '$$ROOT'}
                }}
            ]
            
            latest_models = list(models_collection.aggregate(pipeline_agg))
            
            for model_group in latest_models:
                model = model_group['latest_model']
                symbol = model['symbol']
                
                # Check R² score thresholds
                val_r2 = model.get('val_r2', 0)
                test_r2 = model.get('test_r2', 0)
                
                if val_r2 < 0.1:  # Poor validation performance
                    alerts.append({
                        'symbol': symbol,
                        'model_name': model['model_name'],
                        'issue': 'low_validation_r2',
                        'value': val_r2,
                        'threshold': 0.1
                    })
                
                if test_r2 < 0.1:  # Poor test performance
                    alerts.append({
                        'symbol': symbol,
                        'model_name': model['model_name'],
                        'issue': 'low_test_r2',
                        'value': test_r2,
                        'threshold': 0.1
                    })
                
                # Check if validation and test performance are very different (overfitting)
                r2_diff = abs(val_r2 - test_r2)
                if r2_diff > 0.3:
                    alerts.append({
                        'symbol': symbol,
                        'model_name': model['model_name'],
                        'issue': 'potential_overfitting',
                        'val_r2': val_r2,
                        'test_r2': test_r2,
                        'difference': r2_diff
                    })
                
                # Check model age
                model_date = model['created_at']
                if isinstance(model_date, str):
                    model_date = datetime.datetime.fromisoformat(model_date.replace('Z', '+00:00'))
                
                days_old = (datetime.datetime.now() - model_date).days
                if days_old > Config.RETRAIN_INTERVAL_DAYS * 2:  # Model is very old
                    alerts.append({
                        'symbol': symbol,
                        'model_name': model['model_name'],
                        'issue': 'model_too_old',
                        'days_old': days_old,
                        'threshold': Config.RETRAIN_INTERVAL_DAYS * 2
                    })
            
            # Log alerts
            if alerts:
                logger.warning(f"Found {len(alerts)} model performance issues:")
                for alert in alerts:
                    logger.warning(f"Alert: {alert}")
                
                # Save alerts to MongoDB
                alert_log = {
                    'timestamp': datetime.datetime.now(),
                    'task': 'performance_monitoring',
                    'alerts_count': len(alerts),
                    'alerts': alerts
                }
                self.db['ml_alerts'].insert_one(alert_log)
            else:
                logger.info("No model performance issues detected")
            
        except Exception as e:
            logger.error(f"Error in model performance monitoring: {e}")
    
    def run_scheduler(self) -> None:
        """Run the ML scheduler"""
        logger.info("Starting ML Scheduler")
        logger.info(f"Training scheduled at: {Config.TRAINING_SCHEDULE}")
        logger.info(f"Predictions scheduled at: {Config.PREDICTION_SCHEDULE}")
        logger.info(f"Retrain interval: {Config.RETRAIN_INTERVAL_DAYS} days")
        logger.info(f"Demo mode: {Config.RUN_DEMO_ON_START}")
        
        # Schedule tasks
        schedule.every().day.at(Config.TRAINING_SCHEDULE).do(self.scheduled_training)
        schedule.every().day.at(Config.PREDICTION_SCHEDULE).do(self.scheduled_prediction)
        schedule.every().hour.do(self.model_performance_monitoring)
        
        # Run initial tasks
        if Config.RUN_DEMO_ON_START:
            logger.info("Demo mode enabled - running initial training and prediction...")
            
            # Wait a bit for data to be available from ETL
            logger.info("Waiting for processed data to be available...")
            time.sleep(60)  # Wait 1 minute for data
            
            self.scheduled_training()
            time.sleep(30)  # Wait between tasks
            self.scheduled_prediction()
            
            logger.info("Demo initialization completed.")
        else:
            logger.info("Running initial training and prediction...")
            self.scheduled_training()
            time.sleep(10)  # Wait a bit between tasks
            self.scheduled_prediction()
        
        # Main scheduler loop
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

def main() -> None:
    """Main function to run the ML scheduler"""
    scheduler = MLScheduler()
    scheduler.run_scheduler()

if __name__ == "__main__":
    main()
