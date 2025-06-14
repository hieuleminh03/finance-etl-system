#!/usr/bin/env python3
"""Kibana dashboard import utility."""
import os
import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/dashboard_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dashboard_import")

class Config:
    load_dotenv(dotenv_path='/app/.env', override=True)
    
    ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch-master')
    ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
    KIBANA_HOST = os.getenv('KIBANA_HOST', 'kibana')
    KIBANA_PORT = int(os.getenv('KIBANA_PORT', 5601))
    KIBANA_URL = f"http://{KIBANA_HOST}:{KIBANA_PORT}"

def wait_for_kibana(retries: int = 30, delay: int = 10) -> bool:
    kibana_status_url = f"{Config.KIBANA_URL}/api/status"
    for i in range(retries):
        try:
            response = requests.get(kibana_status_url)
            if response.status_code == 200:
                logger.info("Kibana is ready.")
                return True
        except requests.ConnectionError:
            logger.warning(f"Kibana not ready yet. Retrying in {delay}s...")
        
        time.sleep(delay)
        
    logger.error("Kibana did not become ready in time.")
    return False

def import_dashboards() -> bool:
    """
    Imports dashboards from the dashboards directory.
    """
    dashboard_dir = os.path.dirname(os.path.realpath(__file__))
    import_url = f"{Config.KIBANA_URL}/api/saved_objects/_import"
    headers = {"kbn-xsrf": "true"}
    
    dashboards_to_import = [f for f in os.listdir(dashboard_dir) if f.endswith('.ndjson')]
    if not dashboards_to_import:
        logger.warning("No dashboards found to import.")
        return True

    success_count = 0
    for filename in dashboards_to_import:
        file_path = os.path.join(dashboard_dir, filename)
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (filename, f, 'application/ndjson')}
                response = requests.post(
                    import_url,
                    headers=headers,
                    files=files,
                    params={"overwrite": "true"}
                )
            
            if response.status_code == 200:
                logger.info(f"Successfully imported dashboard: {filename}")
                success_count += 1
            else:
                logger.error(f"Failed to import dashboard {filename}: {response.text}")
        except Exception as e:
            logger.error(f"Error importing dashboard {filename}: {e}")
            
    return success_count == len(dashboards_to_import)

def main() -> None:
    """
    Main function to import dashboards.
    """
    logger.info("Starting Kibana dashboard import process.")
    
    if not wait_for_kibana():
        logger.critical("Could not connect to Kibana. Aborting.")
        return

    if import_dashboards():
        logger.info("Dashboard import completed successfully.")
    else:
        logger.error("Dashboard import failed.")

if __name__ == "__main__":
    main()
