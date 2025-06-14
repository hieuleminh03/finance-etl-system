# Financial Data Pipeline + ML Price Prediction

Real-time financial data processing with AI-powered stock price prediction.

## Architecture

**Data Flow:** Crawler → Kafka → MongoDB → Spark ETL → ML Models → API

**Services:**
- MongoDB (data storage)
- Kafka (streaming)
- Spark (ETL processing) 
- ML API (predictions on port 8000)
- Kibana (visualization on port 5601)

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
