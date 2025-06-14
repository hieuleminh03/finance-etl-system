#!/usr/bin/env python3
"""Docker job scheduler for crawler and ETL tasks."""

import os
import time
import datetime
import logging
import schedule
import docker
import sys
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("scheduler")

class Config:
    load_dotenv()
    
    CRAWLER_SCHEDULE = os.getenv("CRAWLER_SCHEDULE", "*/10 * * * *")
    ETL_SCHEDULE = os.getenv("ETL_SCHEDULE", "0 */12 * * *")
    
    MAX_RUNTIME = {
        "crawler": 30 * 60,
        "etl": 6 * 60 * 60
    }

client = docker.from_env()
last_run_times: Dict[str, Optional[datetime.datetime]] = {
    "crawler": None,
    "etl": None
}

def get_container_runtime(container: docker.models.containers.Container) -> float:
    Calculate how long a container has been running.
    """
    if container.status != 'running':
        return 0.0
        
    try:
        container_info = container.attrs
        started_at_str = container_info.get('State', {}).get('StartedAt')
        if not started_at_str:
            return 0.0
            
        start_time = datetime.datetime.fromisoformat(started_at_str.split('.')[0])
        start_time = start_time.replace(tzinfo=datetime.timezone.utc)
        
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - start_time).total_seconds()
        
    except (ValueError, KeyError) as e:
        logger.error(f"Error calculating container runtime: {e}")
        return 0.0

def run_container(service_name: str) -> bool:
    """
    Run a specific container using Docker API.
    """
    container_name = f"finance_{service_name}"
    image_name = f"final-{service_name}:latest"
    
    try:
        logger.info(f"Starting {service_name} job.")
        
        try:
            container = client.containers.get(container_name)
            runtime = get_container_runtime(container)
            
            if container.status == 'running':
                if runtime < Config.MAX_RUNTIME.get(service_name, 3600):
                    logger.info(f"{service_name} is still running. Skipping new job.")
                    return False
                else:
                    logger.warning(f"{service_name} has been running for too long. Stopping and removing.")
                    container.stop()
                    container.remove()
            else:
                container.remove()

        except docker.errors.NotFound:
            pass  # Container not found, which is fine

        new_container = client.containers.run(
            image_name,
            name=container_name,
            detach=True,
            network="finance_network",
            volumes={
                '/app/data': {'bind': '/app/data', 'mode': 'rw'},
                '/app/logs': {'bind': '/app/logs', 'mode': 'rw'},
                '/app/configs': {'bind': '/app/configs', 'mode': 'rw'}
            }
        )
        
        last_run_times[service_name] = datetime.datetime.now()
        logger.info(f"Successfully started {service_name} with container ID: {new_container.id[:12]}")
        return True
        
    except docker.errors.ImageNotFound:
        logger.error(f"Image '{image_name}' not found. Trying docker-compose.")
        return run_with_compose(service_name)
    except Exception as e:
        logger.error(f"Error running {service_name}: {e}")
        return run_with_compose(service_name)

def run_with_compose(service_name: str) -> bool:
    """
    Fallback to docker-compose if Docker API fails.
    """
    try:
        os.system(f"docker-compose up -d --build {service_name}")
        last_run_times[service_name] = datetime.datetime.now()
        return True
    except Exception as e:
        logger.critical(f"docker-compose also failed for {service_name}: {e}")
        return False

def run_crawler_job() -> None:
    """Run the crawler job to fetch financial data."""
    logger.info("Executing crawler job.")
    run_container("crawler")

def run_etl_job() -> None:
    """Run the ETL job to process raw data."""
    logger.info("Executing ETL job.")
    run_container("etl")

def setup_schedule() -> None:
    """
    Configure the schedule for all jobs.
    """
    schedule.every(10).minutes.do(run_crawler_job)
    logger.info(f"Scheduled crawler job every 10 minutes.")
    
    schedule.every(12).hours.do(run_etl_job)
    logger.info(f"Scheduled ETL job every 12 hours.")

def main() -> None:
    """
    Main function to set up and run the scheduler.
    """
    logger.info("Starting the scheduler.")
    
    setup_schedule()
    
    logger.info("Running initial jobs.")
    run_crawler_job()
    time.sleep(900)  # Wait 15 minutes before initial ETL
    run_etl_job()
    
    logger.info("Entering main scheduling loop.")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
