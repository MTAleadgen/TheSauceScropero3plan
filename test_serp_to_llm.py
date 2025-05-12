#!/usr/bin/env python3
"""
Script to run a full test of the SERP to LLM pipeline:
1. Run DataForSEO SERP collection on 10 cities
2. Process the raw SERP data with the LLM parser
3. Analyze the extracted event data

This script ties together the DataForSEO collection and LLM parsing
as a complete end-to-end test of the pipeline.
"""
import os
import argparse
import logging
import json
import glob
import time
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data_raw")
DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR", "./data_parsed")
MAX_FILES_TO_PROCESS = 10  # Limit files to process for testing

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Test SERP to LLM pipeline')
    parser.add_argument('--skip-serp', action='store_true', help='Skip SERP collection step')
    parser.add_argument('--skip-llm', action='store_true', help='Skip LLM parsing step')
    parser.add_argument('--llm-model', default="Qwen/Qwen3-7B", help='LLM model to use')
    parser.add_argument('--max-files', type=int, default=MAX_FILES_TO_PROCESS, help='Maximum files to process')
    return parser.parse_args()

def ensure_directories():
    """Ensure necessary directories exist"""
    Path(DATA_RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(DATA_PARSED_DIR).mkdir(parents=True, exist_ok=True)
    logger.info(f"Ensured directories exist: {DATA_RAW_DIR}, {DATA_PARSED_DIR}")

def run_serp_collection():
    """Run the SERP collection process"""
    logger.info("Starting SERP collection...")
    
    try:
        # Try running through our test script
        from run_serp_test import main as serp_test_main
        serp_test_main()
        return True
    except ImportError:
        logger.warning("Could not import run_serp_test, falling back to subprocess")
    except Exception as e:
        logger.error(f"Error running SERP collection: {e}")
        logger.info("Falling back to subprocess method...")
    
    try:
        # Fallback to subprocess
        result = subprocess.run(
            ["python", "run_serp_test.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"SERP collection output: {result.stdout}")
        if result.stderr:
            logger.warning(f"SERP collection stderr: {result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running SERP collection: {e}")
        logger.error(f"Process stderr: {e.stderr}")
        return False

def run_llm_parsing(model_name: str, max_files: int):
    """Run the LLM parsing on raw SERP data"""
    logger.info(f"Starting LLM parsing with model {model_name}...")
    
    # Find raw SERP files
    serp_files = glob.glob(os.path.join(DATA_RAW_DIR, "*.json"))
    if not serp_files:
        logger.error("No SERP data files found. Cannot proceed with LLM parsing.")
        return False
    
    logger.info(f"Found {len(serp_files)} SERP data files, processing up to {max_files}")
    files_to_process = serp_files[:max_files]
    
    successful_parses = 0
    failed_parses = 0
    
    for file_path in files_to_process:
        try:
            file_name = os.path.basename(file_path)
            output_path = os.path.join(DATA_PARSED_DIR, f"parsed_{file_name}")
            
            logger.info(f"Processing {file_name} with LLM parser...")
            
            # Build command for LLM parser
            cmd = [
                "python", "llm_serp_parser_qwen.py",
                "--input", file_path,
                "--output", output_path,
                "--model", model_name
            ]
            
            # Run LLM parser
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            
            if "Error" in result.stdout or "error" in result.stdout.lower():
                logger.warning(f"Possible error in LLM parsing for {file_name}: {result.stdout}")
                failed_parses += 1
            else:
                logger.info(f"Successfully parsed {file_name}")
                successful_parses += 1
                
            # Add some delay to avoid overwhelming GPU
            time.sleep(1)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running LLM parser on {file_path}: {e}")
            logger.error(f"Process stderr: {e.stderr}")
            failed_parses += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {file_path}: {e}")
            failed_parses += 1
    
    logger.info(f"LLM parsing complete. Successful: {successful_parses}, Failed: {failed_parses}")
    return successful_parses > 0

def analyze_results():
    """Analyze the extracted event data"""
    logger.info("Analyzing parsed event data...")
    
    parsed_files = glob.glob(os.path.join(DATA_PARSED_DIR, "*.json"))
    if not parsed_files:
        logger.warning("No parsed data files found.")
        return
    
    logger.info(f"Found {len(parsed_files)} parsed event files")
    
    total_events = 0
    events_by_city = {}
    dance_styles = set()
    
    for file_path in parsed_files:
        try:
            with open(file_path, 'r') as f:
                events = json.load(f)
            
            if not isinstance(events, list):
                logger.warning(f"Unexpected format in {file_path}, expected a list of events")
                continue
            
            # Extract city name from filename
            file_name = os.path.basename(file_path)
            if file_name.startswith("parsed_"):
                city_part = file_name.split('_')[1]
                
                if city_part not in events_by_city:
                    events_by_city[city_part] = 0
                
                events_by_city[city_part] += len(events)
            
            # Count events and gather statistics
            for event in events:
                total_events += 1
                
                # Track dance styles
                if event.get('dance_style'):
                    dance_styles.add(event.get('dance_style').lower())
            
            logger.info(f"File {file_name}: {len(events)} events extracted")
            
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON in {file_path}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Results summary:")
    logger.info(f"  - Total events extracted: {total_events}")
    logger.info(f"  - Cities with events: {len(events_by_city)}")
    
    if events_by_city:
        logger.info(f"  - Events per city:")
        for city, count in events_by_city.items():
            logger.info(f"    * {city}: {count} events")
    
    logger.info(f"  - Dance styles found: {len(dance_styles)}")
    if dance_styles:
        logger.info(f"    * {', '.join(sorted(dance_styles))}")

def main():
    """Main function to run the complete pipeline test"""
    args = parse_arguments()
    
    logger.info("=== Starting SERP to LLM Pipeline Test ===")
    
    # Ensure directories exist
    ensure_directories()
    
    # Step 1: Run SERP collection
    if not args.skip_serp:
        if not run_serp_collection():
            logger.error("SERP collection failed. Cannot proceed with LLM parsing.")
            return
    else:
        logger.info("Skipping SERP collection step as requested.")
    
    # Step 2: Run LLM parsing
    if not args.skip_llm:
        if not run_llm_parsing(args.llm_model, args.max_files):
            logger.error("LLM parsing failed. Cannot analyze results.")
            return
    else:
        logger.info("Skipping LLM parsing step as requested.")
    
    # Step 3: Analyze results
    analyze_results()
    
    logger.info("=== SERP to LLM Pipeline Test Complete ===")
    logger.info(f"Raw SERP data stored in: {DATA_RAW_DIR}")
    logger.info(f"Parsed event data stored in: {DATA_PARSED_DIR}")

if __name__ == "__main__":
    main() 