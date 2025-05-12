#!/usr/bin/env python3
"""
Script to run a test of DataForSEO SERP collection on 10 cities
and verify the results in the data_raw directory.
"""
import os
import subprocess
import time
import json
from pathlib import Path
import pandas as pd
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data_raw")
MAX_CITIES = 10

def ensure_directories():
    """Ensure necessary directories exist"""
    Path(DATA_RAW_DIR).mkdir(parents=True, exist_ok=True)
    logger.info(f"Ensured data_raw directory exists at: {DATA_RAW_DIR}")

def check_credentials():
    """Check if DataForSEO credentials are set"""
    login = os.getenv("DATAFORSEO_LOGIN")
    password = os.getenv("DATAFORSEO_PASSWORD")
    
    logger.info(f"DataForSEO login: {login[:5]}*** (masked for security)")
    logger.info(f"DataForSEO password: {password[:5]}*** (masked for security)" if password else "DataForSEO password not found")
    
    if not login or not password:
        logger.error("DataForSEO credentials not set. Please set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables.")
        return False
    
    logger.info("DataForSEO credentials found.")
    return True

def run_discovery_process():
    """Run the discovery process through the Python module directly"""
    logger.info("Starting discovery process for 10 cities...")
    
    try:
        # Using module import to run discovery directly
        from services.discovery.discovery import main as discovery_main
        discovery_main()
        logger.info("Discovery process completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Error running discovery process: {e}")
        logger.info("Falling back to subprocess method...")
        
        try:
            # Fallback to subprocess
            result = subprocess.run(
                ["python", "services/discovery/discovery.py"],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Discovery output: {result.stdout}")
            if result.stderr:
                logger.warning(f"Discovery stderr: {result.stderr}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running discovery subprocess: {e}")
            logger.error(f"Process stderr: {e.stderr}")
            return False

def analyze_serp_data():
    """Analyze the collected SERP data in data_raw directory"""
    logger.info(f"Analyzing SERP data in {DATA_RAW_DIR}...")
    
    raw_files = list(Path(DATA_RAW_DIR).glob("*.json"))
    logger.info(f"Found {len(raw_files)} raw SERP files")
    
    if not raw_files:
        logger.warning("No SERP data files found. The collection may have failed.")
        return
    
    # Count cities
    cities = set()
    organic_result_counts = []
    total_results = 0
    
    for file_path in raw_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract city name from filename
            filename = file_path.name
            city_part = filename.split('_')[0]
            cities.add(city_part)
            
            # Count organic results
            items = data.get('items', [])
            organic_count = sum(1 for item in items if item.get('type') == 'organic')
            organic_result_counts.append(organic_count)
            total_results += organic_count
            
            logger.info(f"File {filename}: {organic_count} organic results")
            
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON in {file_path}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Data summary:")
    logger.info(f"  - Cities found: {len(cities)}")
    logger.info(f"  - Total organic results: {total_results}")
    
    if organic_result_counts:
        avg_results = sum(organic_result_counts) / len(organic_result_counts)
        logger.info(f"  - Average organic results per file: {avg_results:.2f}")
        logger.info(f"  - Min results: {min(organic_result_counts)}")
        logger.info(f"  - Max results: {max(organic_result_counts)}")

def main():
    logger.info("=== Starting DataForSEO SERP Test ===")
    
    # Ensure directories exist
    ensure_directories()
    
    # Check credentials
    if not check_credentials():
        return
    
    # Run the discovery process
    if not run_discovery_process():
        logger.error("Discovery process failed. Exiting.")
        return
    
    # Wait a moment for file system to sync
    time.sleep(1)
    
    # Analyze SERP data
    analyze_serp_data()
    
    logger.info("=== DataForSEO SERP Test Complete ===")
    logger.info(f"Check the raw SERP data in {DATA_RAW_DIR}")
    logger.info("Next step: Use this data for LLM-based parsing")

if __name__ == "__main__":
    main() 