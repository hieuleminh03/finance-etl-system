# ML Service - Stock Price Prediction

AI-powered price prediction for financial data pipeline.

## Components

- `ml_pipeline.py` - Core ML training/prediction
- `ml_scheduler.py` - Automated scheduling  
- `prediction_api.py` - REST API

## Models
Random Forest, XGBoost, LightGBM, Gradient Boosting, Linear Regression

## Features
Technical indicators (SMA, MACD, RSI, Bollinger Bands), lag features, rolling stats, volatility measures

## API Endpoints

```bash
GET  /predict/{symbol}      # Get prediction
GET  /predictions/latest    # Latest predictions
GET  /models/{symbol}       # Available models
POST /train/{symbol}        # Trigger training
GET  /symbols              # Available symbols
GET  /health               # Health check
```

## Config (.env)

```bash
PREDICTION_DAYS=5
LOOKBACK_DAYS=60
MIN_DATA_POINTS=252
TRAINING_SCHEDULE=02:00
PREDICTION_SCHEDULE=09:00
ML_API_PORT=8000
```

## Usage

```python
from ml_pipeline import MLPipeline

pipeline = MLPipeline()
models = pipeline.train_models('AAPL')
prediction = pipeline.predict_price('AAPL')
```

## Storage
Models stored in MongoDB GridFS with automatic versioning and performance tracking.

## Monitoring
Performance metrics: R² Score, MAE, MSE
Logs: `/app/logs/*.log`

## Troubleshooting
- Need 252+ data points (1 year minimum)
- Check MongoDB connection
- Monitor R² scores for model quality
