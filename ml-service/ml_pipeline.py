#!/usr/bin/env python3
"""ML Pipeline for stock price prediction with feature engineering and model training."""

import os
import sys
import json
import logging
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import pickle
import joblib
import warnings

# ML libraries
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import xgboost as xgb
import lightgbm as lgb

# MongoDB
import pymongo
from pymongo import MongoClient
import gridfs

# Environment setup
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/ml_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_pipeline")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)

    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'devpassword123')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

    PREDICTION_DAYS = int(os.getenv('PREDICTION_DAYS', '5'))
    LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '60'))
    MIN_DATA_POINTS = int(os.getenv('MIN_DATA_POINTS', '252'))
    TEST_SIZE = float(os.getenv('TEST_SIZE', '0.2'))
    VALIDATION_SIZE = float(os.getenv('VALIDATION_SIZE', '0.2'))

class MLPipeline:
    
    def __init__(self) -> None:
        self.db = self._connect_to_mongodb()
        self.fs = gridfs.GridFS(self.db) if self.db else None
        self.models: Dict[str, Dict[str, Any]] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.feature_columns: List[str] = []
        self.target_column: str = 'close'
        self.model_configs = self._get_model_configs()
    
    def _connect_to_mongodb(self) -> Optional[pymongo.database.Database]:
        try:
            client = MongoClient(
                host=Config.MONGO_HOST,
                port=Config.MONGO_PORT,
                username=Config.MONGO_USERNAME,
                password=Config.MONGO_PASSWORD,
                authSource=Config.MONGO_AUTH_SOURCE,
                serverSelectionTimeoutMS=5000
            )
            client.admin.command('ping')
            logger.info(f"Connected to MongoDB at {Config.MONGO_HOST}:{Config.MONGO_PORT}")
            return client[Config.MONGO_DATABASE]
        except pymongo.errors.ConnectionFailure as e:
            logger.critical(f"MongoDB connection failed: {e}")
            return None
    
    def _get_model_configs(self) -> Dict[str, Dict[str, Any]]:
        return {
            'random_forest': {
                'model': RandomForestRegressor(random_state=42),
                'params': {'n_estimators': [50, 100], 'max_depth': [10, 20]}
            },
            'lightgbm': {
                'model': lgb.LGBMRegressor(random_state=42, verbose=-1),
                'params': {'n_estimators': [50, 100], 'learning_rate': [0.05, 0.1]}
            },
            'linear_regression': {
                'model': LinearRegression(),
                'params': {}
            }
        }
    
    def load_processed_data(self, symbol: str) -> Optional[pd.DataFrame]:
        if not self.db:
            logger.error("No MongoDB connection available.")
            return None
            
        try:
            collection = self.db[f"processed_{symbol}"]
            data = list(collection.find({}).sort("trading_date", 1))
            
            if not data:
                logger.warning(f"No processed data found for {symbol}.")
                return None
                
            df = pd.DataFrame(data)
            df['trading_date'] = pd.to_datetime(df['trading_date'])
            df.set_index('trading_date', inplace=True)
            
            logger.info(f"Loaded {len(df)} records for {symbol}.")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {e}")
            return None
    
    def prepare_features(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Prepare features for ML training.
        """
        if df is None or df.empty:
            return None
            
        try:
            # Use existing feature columns from technical analysis
            self.feature_columns = [col for col in df.columns if col not in ['_id', 'ticker']]
            
            # Create future target
            df['future_close'] = df[self.target_column].shift(-Config.PREDICTION_DAYS)
            
            # Drop rows with NaN values
            df.dropna(inplace=True)
            
            logger.info(f"Prepared {len(self.feature_columns)} features.")
            return df
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return None
    
    def train_models(self, symbol: str) -> bool:
        """
        Train multiple models for a given symbol.
        """
        df = self.load_processed_data(symbol)
        if df is None or len(df) < Config.MIN_DATA_POINTS:
            logger.warning(f"Insufficient data for {symbol}. Skipping training.")
            return False

        df = self.prepare_features(df)
        if df is None:
            return False

        X = df[self.feature_columns]
        y = df['future_close']

        tscv = TimeSeriesSplit(n_splits=5)
        train_index, test_index = list(tscv.split(X))[-1]
        
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        self.scalers[symbol] = scaler

        trained_models = {}
        for model_name, config in self.model_configs.items():
            logger.info(f"Training {model_name} for {symbol}...")
            model = config['model']
            
            if config['params']:
                grid_search = GridSearchCV(model, config['params'], cv=TimeSeriesSplit(n_splits=3), n_jobs=-1)
                grid_search.fit(X_train_scaled, y_train)
                best_model = grid_search.best_estimator_
            else:
                best_model = model
                best_model.fit(X_train_scaled, y_train)

            y_pred = best_model.predict(X_test_scaled)
            r2 = r2_score(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            logger.info(f"{model_name} for {symbol} - R²: {r2:.4f}, MSE: {mse:.4f}")

            trained_models[model_name] = {
                'model': best_model,
                'r2_score': r2,
                'mse': mse,
                'feature_columns': self.feature_columns,
                'training_date': datetime.datetime.now().isoformat()
            }
        
        self.models[symbol] = trained_models
        return self.save_models_to_mongodb(symbol)
    
    def save_models_to_mongodb(self, symbol: str) -> bool:
        """
        Save trained models and scalers to MongoDB using GridFS.
        """
        if not self.db or not self.fs or not self.models.get(symbol):
            logger.error("Cannot save models. No DB connection or no models trained.")
            return False

        models_to_save = self.models[symbol]
        models_collection = self.db['ml_models']
        
        for model_name, data in models_to_save.items():
            try:
                model_bytes = joblib.dumps(data['model'])
                scaler_bytes = joblib.dumps(self.scalers[symbol])

                model_file_id = self.fs.put(model_bytes, filename=f"{symbol}_{model_name}_model.joblib")
                scaler_file_id = self.fs.put(scaler_bytes, filename=f"{symbol}_scaler.joblib")

                metadata = {
                    'symbol': symbol,
                    'model_name': model_name,
                    'model_file_id': model_file_id,
                    'scaler_file_id': scaler_file_id,
                    'r2_score': data['r2_score'],
                    'mse': data['mse'],
                    'feature_columns': data['feature_columns'],
                    'training_date': data['training_date']
                }
                
                models_collection.update_one(
                    {'symbol': symbol, 'model_name': model_name},
                    {'$set': metadata},
                    upsert=True
                )
                logger.info(f"Saved model '{model_name}' for {symbol}.")
            except Exception as e:
                logger.error(f"Error saving model {model_name} for {symbol}: {e}")
                return False
        return True
    
    def load_model_from_mongodb(self, symbol: str, model_name: str) -> Optional[Tuple[Any, Any, List[str]]]:
        """
        Load a trained model and its corresponding scaler from MongoDB.
        """
        if not self.db or not self.fs:
            return None
            
        try:
            models_collection = self.db['ml_models']
            model_doc = models_collection.find_one({'symbol': symbol, 'model_name': model_name})

            if not model_doc:
                logger.warning(f"Model '{model_name}' for {symbol} not found.")
                return None

            model_file = self.fs.get(model_doc['model_file_id'])
            model = joblib.loads(model_file.read())

            scaler_file = self.fs.get(model_doc['scaler_file_id'])
            scaler = joblib.loads(scaler_file.read())
            
            logger.info(f"Loaded model '{model_name}' for {symbol}.")
            return model, scaler, model_doc['feature_columns']

        except Exception as e:
            logger.error(f"Error loading model {model_name} for {symbol}: {e}")
            return None
    
    def get_best_model(self, symbol: str) -> Optional[str]:
        """
        Get the best performing model for a symbol based on R² score.
        """
        if not self.db:
            return None
            
        try:
            models_collection = self.db['ml_models']
            best_model_doc = models_collection.find_one(
                {'symbol': symbol},
                sort=[('r2_score', -1)]
            )
            
            if best_model_doc:
                return best_model_doc['model_name']
            else:
                logger.warning(f"No models found for {symbol}.")
                return None
                
        except Exception as e:
            logger.error(f"Error getting best model for {symbol}: {e}")
            return None
    
    def predict_price(self, symbol: str, model_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Make a price prediction for a given symbol.
        """
        if model_name is None:
            model_name = self.get_best_model(symbol)
            if not model_name:
                return None

        load_result = self.load_model_from_mongodb(symbol, model_name)
        if not load_result:
            return None
        model, scaler, feature_columns = load_result

        df = self.load_processed_data(symbol)
        if df is None:
            return None
            
        latest_features = df[feature_columns].iloc[-1:]
        latest_features_scaled = scaler.transform(latest_features)
        
        prediction = model.predict(latest_features_scaled)[0]
        current_price = df[self.target_column].iloc[-1]

        return {
            'symbol': symbol,
            'model_name': model_name,
            'current_price': current_price,
            'predicted_price': prediction,
            'prediction_date': datetime.datetime.now().isoformat()
        }
    
    def save_prediction_to_mongodb(self, prediction: Dict[str, Any]) -> bool:
        """
        Save a prediction to MongoDB.
        """
        if not self.db or not prediction:
            return False
            
        try:
            predictions_collection = self.db['price_predictions']
            predictions_collection.insert_one(prediction)
            logger.info(f"Saved prediction for {prediction['symbol']}.")
            return True
            
        except Exception as e:
            logger.error(f"Error saving prediction for {prediction['symbol']}: {e}")
            return False
    
    def batch_train_models(self, symbols: Optional[List[str]] = None) -> None:
        """
        Train models for a list of symbols.
        """
        if not self.db:
            return

        if symbols is None:
            symbols = [name.replace('processed_', '') for name in self.db.list_collection_names() if name.startswith('processed_')]

        logger.info(f"Starting batch training for {len(symbols)} symbols.")
        for symbol in symbols:
            self.train_models(symbol)

    def batch_predict(self, symbols: Optional[List[str]] = None) -> None:
        """
        Make predictions for a list of symbols.
        """
        if not self.db:
            return

        if symbols is None:
            symbols = self.db['ml_models'].distinct('symbol')

        logger.info(f"Starting batch prediction for {len(symbols)} symbols.")
        for symbol in symbols:
            prediction = self.predict_price(symbol)
            if prediction:
                self.save_prediction_to_mongodb(prediction)

def main() -> None:
    """
    Main function to run the ML pipeline for a set of test symbols.
    """
    logger.info("Starting ML Pipeline main execution.")
    pipeline = MLPipeline()
    
    test_symbols = ['AAPL', 'MSFT', 'GOOG']
    
    logger.info(f"Training models for: {', '.join(test_symbols)}")
    pipeline.batch_train_models(test_symbols)
    
    logger.info(f"Making predictions for: {', '.join(test_symbols)}")
    pipeline.batch_predict(test_symbols)
    
    logger.info("ML Pipeline execution finished.")

if __name__ == "__main__":
    main()
