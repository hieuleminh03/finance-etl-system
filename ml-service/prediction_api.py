#!/usr/bin/env python3
"""FastAPI service for real-time stock price predictions and model management."""

import os
import logging
import datetime
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from ml_pipeline import MLPipeline
import pymongo
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/prediction_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("prediction_api")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)

    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'finance_data')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'finance_user')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'FinanceUserPass2024!')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    
    API_HOST = os.getenv('API_HOST', '0.0.0.0')
    API_PORT = int(os.getenv('API_PORT', 8000))

# FastAPI app
app = FastAPI(
    title="Stock Price Prediction API",
    description="AI-powered stock price prediction service",
    version="1.0.0"
)

pipeline = MLPipeline()

def connect_to_mongodb() -> Optional[pymongo.database.Database]:
    """
    Connect to the MongoDB database.

    Returns:
        pymongo.database.Database: The database object, or None on failure.
    """
    try:
        mongo_uri = f"mongodb://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DATABASE}?authSource={Config.MONGO_AUTH_SOURCE}"
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logger.info(f"API connected to MongoDB: {Config.MONGO_HOST}")
        return client[Config.MONGO_DATABASE]
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return None

db = connect_to_mongodb()

@app.get("/")
async def root() -> Dict[str, Any]:
    """
    Root endpoint providing API information.
    """
    return {
        "message": "Stock Price Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "predict": "/predict/{symbol}",
            "models": "/models/{symbol}",
            "train": "/train/{symbol}",
            "latest_predictions": "/predictions/latest",
            "model_performance": "/models/performance"
        }
    }

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint to verify service status.
    """
    try:
        if db is None:
            raise Exception("No database connection")
        
        db.admin.command('ping')
        
        return {
            "status": "healthy",
            "timestamp": datetime.datetime.now().isoformat(),
            "services": {
                "database": "connected",
                "ml_pipeline": "ready"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/predict/{symbol}")
async def predict_price(symbol: str, model_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a price prediction for a given stock symbol.
    """
    try:
        symbol = symbol.upper()
        logger.info(f"Prediction requested for {symbol} with model {model_name}")
        
        prediction = pipeline.predict_price(symbol, model_name)
        
        if not prediction:
            raise HTTPException(
                status_code=404,
                detail=f"No trained models or data available for {symbol}"
            )
        
        return prediction
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error making prediction for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@app.get("/models/{symbol}")
async def get_models(symbol: str) -> Dict[str, Any]:
    """
    Get all available models for a given stock symbol.
    """
    try:
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        
        symbol = symbol.upper()
        models_collection = db['ml_models']
        
        models_cursor = models_collection.find(
            {'symbol': symbol},
            {
                'model_name': 1,
                'val_r2': 1,
                'test_r2': 1,
                'val_mae': 1,
                'test_mae': 1,
                'training_date': 1,
                'created_at': 1,
                'prediction_days': 1,
                'lookback_days': 1
            }
        ).sort([('created_at', -1)])
        
        models = list(models_cursor)
        
        if not models:
            raise HTTPException(status_code=404, detail=f"No models found for {symbol}")
        
        for model in models:
            model['_id'] = str(model['_id'])
            if 'created_at' in model and model['created_at']:
                if isinstance(model['created_at'], datetime.datetime):
                    model['created_at'] = model['created_at'].isoformat()
        
        return {
            'symbol': symbol,
            'models_count': len(models),
            'models': models
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting models for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get models: {str(e)}")

@app.post("/train/{symbol}")
async def train_model(symbol: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    Trigger model training for a given stock symbol.
    """
    try:
        symbol = symbol.upper()
        logger.info(f"Training requested for {symbol}")
        
        background_tasks.add_task(train_model_background, symbol)
        
        return {
            'message': f'Training started for {symbol}',
            'symbol': symbol,
            'status': 'training_initiated',
            'timestamp': datetime.datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error initiating training for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Training initiation failed: {str(e)}")

async def train_model_background(symbol: str) -> None:
    """
    Background task for model training.
    """
    try:
        logger.info(f"Starting background training for {symbol}")
        
        success = pipeline.train_models(symbol)
        
        if success:
            logger.info(f"Successfully trained and saved models for {symbol}")
        else:
            logger.warning(f"Failed to train models for {symbol}")
            
    except Exception as e:
        logger.error(f"Error in background training for {symbol}: {e}")

@app.get("/predictions/latest")
async def get_latest_predictions(limit: int = 50, symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the latest price predictions.
    """
    try:
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        
        predictions_collection = db['price_predictions']
        
        query = {}
        if symbol:
            query['symbol'] = symbol.upper()
        
        predictions_cursor = predictions_collection.find(
            query,
            {
                'symbol': 1,
                'current_price': 1,
                'predicted_price': 1,
                'price_change': 1,
                'price_change_pct': 1,
                'model_name': 1,
                'prediction_days': 1,
                'prediction_date': 1,
                'data_date': 1,
                'model_performance': 1
            }
        ).sort([('created_at', -1)]).limit(limit)
        
        predictions = list(predictions_cursor)
        
        for pred in predictions:
            pred['_id'] = str(pred['_id'])
        
        return {
            'predictions_count': len(predictions),
            'limit': limit,
            'symbol_filter': symbol,
            'predictions': predictions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting latest predictions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get predictions: {str(e)}")

@app.get("/models/performance")
async def get_model_performance() -> Dict[str, Any]:
    """
    Get performance metrics for all models.
    """
    try:
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        
        models_collection = db['ml_models']
        
        pipeline_agg = [
            {
                '$group': {
                    '_id': {
                        'symbol': '$symbol',
                        'model_name': '$model_name'
                    },
                    'latest_model': {'$last': '$$ROOT'},
                    'model_count': {'$sum': 1}
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            '$latest_model',
                            {'model_count': '$model_count'}
                        ]
                    }
                }
            },
            {
                '$project': {
                    'symbol': 1,
                    'model_name': 1,
                    'val_r2': 1,
                    'test_r2': 1,
                    'val_mae': 1,
                    'test_mae': 1,
                    'training_date': 1,
                    'created_at': 1,
                    'model_count': 1
                }
            },
            {
                '$sort': {'val_r2': -1}
            }
        ]
        
        performance_data = list(models_collection.aggregate(pipeline_agg))
        
        if performance_data:
            val_r2_scores = [model['val_r2'] for model in performance_data if model.get('val_r2') is not None]
            test_r2_scores = [model['test_r2'] for model in performance_data if model.get('test_r2') is not None]
            
            summary = {
                'total_models': len(performance_data),
                'avg_val_r2': sum(val_r2_scores) / len(val_r2_scores) if val_r2_scores else 0,
                'avg_test_r2': sum(test_r2_scores) / len(test_r2_scores) if test_r2_scores else 0,
                'best_val_r2': max(val_r2_scores) if val_r2_scores else 0,
                'worst_val_r2': min(val_r2_scores) if val_r2_scores else 0,
                'symbols_count': len(set(model['symbol'] for model in performance_data))
            }
        else:
            summary = {
                'total_models': 0,
                'avg_val_r2': 0,
                'avg_test_r2': 0,
                'best_val_r2': 0,
                'worst_val_r2': 0,
                'symbols_count': 0
            }
        
        for model in performance_data:
            model['_id'] = str(model['_id'])
            if 'created_at' in model and model['created_at']:
                if isinstance(model['created_at'], datetime.datetime):
                    model['created_at'] = model['created_at'].isoformat()
        
        return {
            'summary': summary,
            'models': performance_data
        }
        
    except Exception as e:
        logger.error(f"Error getting model performance: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance data: {str(e)}")

@app.get("/symbols")
async def get_available_symbols() -> Dict[str, Any]:
    """
    Get a list of symbols with trained models or processed data.
    """
    try:
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        
        models_collection = db['ml_models']
        symbols_with_models = models_collection.distinct('symbol')
        
        collection_names = db.list_collection_names()
        processed_collections = [name for name in collection_names if name.startswith('processed_')]
        symbols_with_data = [name.replace('processed_', '') for name in processed_collections]
        
        all_symbols = list(set(symbols_with_models + symbols_with_data))
        all_symbols.sort()
        
        return {
            'symbols_count': len(all_symbols),
            'symbols_with_models': len(symbols_with_models),
            'symbols_with_data': len(symbols_with_data),
            'symbols': all_symbols
        }
        
    except Exception as e:
        logger.error(f"Error getting available symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get symbols: {str(e)}")

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc) -> JSONResponse:
    """
    Custom HTTP exception handler.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    logger.info(f"Starting Prediction API on {Config.API_HOST}:{Config.API_PORT}")
    
    uvicorn.run(
        "prediction_api:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=False,
        log_level="info"
    )
