FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY ml-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy ML service code
COPY ml-service/ /app/
COPY .env /app/

# Create logs and data directories
RUN mkdir -p /app/logs /app/data/models /app/data/processed_backup /app/data/raw

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "ml_pipeline.py"]
