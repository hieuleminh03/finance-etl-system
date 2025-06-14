# Docker Compose Startup Issue Fix

## Problem Identified

The Docker Compose startup was hanging because of a dependency deadlock:

1. **Crawler was designed to run infinitely** - The `crawler.py` had a `while True:` loop
2. **ETL service waited for crawler to complete** - The `etl` service had `depends_on: crawler: condition: service_completed_successfully`
3. **This created a deadlock** - ETL waited forever for crawler to "complete successfully", but crawler never completed

## Solution Implemented

### 1. Modified Crawler Behavior (`crawler/crawler.py`)

Added a new environment variable `CRAWLER_CONTINUOUS_MODE` to control crawler behavior:

- **`CRAWLER_CONTINUOUS_MODE=false` (default)**: Run once and exit - for initial data load
- **`CRAWLER_CONTINUOUS_MODE=true`**: Run continuously - for scheduled operations

### 2. Updated Docker Compose Configuration (`docker-compose.yml`)

- Set `CRAWLER_CONTINUOUS_MODE=false` for the initial crawler service
- This allows the crawler to complete successfully and unblock the ETL service

### 3. Enhanced Scheduler (`docker/scheduler.py`)

- Added `run_initial_crawler_job()` function for one-time initial data loads
- Modified `run_container()` to set `CRAWLER_CONTINUOUS_MODE=true` for scheduled crawler runs
- Updated demo mode to use the initial crawler function

### 4. Created Environment Configuration (`.env`)

- Added proper environment variables for all services
- Configured reasonable defaults for the finance ETL system

## How It Works Now

1. **Initial Startup**: Crawler runs once, fetches data, and exits successfully
2. **ETL Activation**: Once crawler completes, ETL service starts and processes the data
3. **Scheduler Takes Over**: Scheduler runs periodic crawler jobs in continuous mode
4. **Clean Architecture**: Clear separation between initial data load and ongoing operations

## Testing the Fix

### Start the System
```bash
docker-compose up --build
```

### Expected Behavior
1. All infrastructure services start (MongoDB, Kafka, Elasticsearch, etc.)
2. Crawler runs once, fetches data, and exits with status 0
3. ETL service starts and processes the data
4. Scheduler begins running periodic jobs
5. ML services start for training and API

### Verify the Fix
```bash
# Check if crawler completed successfully
docker-compose logs crawler

# Check if ETL started after crawler completed
docker-compose logs etl

# Check scheduler logs for periodic jobs
docker-compose logs scheduler
```

## Key Benefits

- **Eliminates startup deadlock** - Services start in proper order
- **Maintains functionality** - All original features preserved
- **Better architecture** - Clear separation of concerns
- **Configurable behavior** - Can run crawler in both modes as needed
- **Robust scheduling** - Scheduler handles ongoing data collection

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CRAWLER_CONTINUOUS_MODE` | `false` | Controls crawler run mode |
| `RUN_DEMO_ON_START` | `true` | Run initial jobs when scheduler starts |
| `FETCH_INTERVAL` | `86400` | Seconds between crawler cycles (when continuous) |
| `MAX_WORKERS` | `10` | Number of parallel workers for data fetching |

The system should now start successfully without hanging!