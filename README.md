# Financial Data Pipeline + ML Price Prediction

Real-time stock price data processing with AI-powered price prediction. Focused on OHLCV (Open, High, Low, Close, Volume) data only.

## Architecture

**Data Flow:** Crawler → Kafka (OHLCV only) → MongoDB → Spark ETL → ML Models → API

**Services:**
- MongoDB (data storage)
- Kafka (streaming OHLCV data only)
- Spark (ETL processing with technical indicators)
- ML API (predictions on port 8000)
- Kibana (visualization on port 5601)

## Data Sources

The system fetches **only stock historical OHLCV data** from Yahoo Finance:
- Open, High, Low, Close prices
- Volume data
- No stock actions (dividends/splits) or company info

## Quick Start

```bash
cp .env.example .env
# Edit .env with your config
docker compose up -d --build
```

## ML API Usage

```bash
# Get price prediction
curl http://localhost:8000/predict/AAPL

# View available models  
curl http://localhost:8000/models/AAPL

# Trigger training
curl -X POST http://localhost:8000/train/AAPL

# API docs
http://localhost:8000/docs
```

## Key URLs
- ML API: http://localhost:8000
- Kibana: http://localhost:5601
- Spark UI: http://localhost:8080

## ML Config (.env)

```bash
PREDICTION_DAYS=5
LOOKBACK_DAYS=60
MIN_DATA_POINTS=252
TRAINING_SCHEDULE=02:00
ML_API_PORT=8000
```

## Models
- Random Forest, XGBoost, LightGBM, Gradient Boosting
- Auto feature engineering (technical indicators, lag features)
- Daily retraining, GridFS storage
- Performance tracking (R², MAE)

## Logs
Check `./logs/` for service logs.

## Manual Operations

```bash
# Force training
docker exec finance_ml_trainer python ml_pipeline.py

# Health check
curl http://localhost:8000/health
