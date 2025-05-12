#!/usr/bin/env python3
"""
Helper script to run DataForSEO discovery tasks.
This script can be used to retrieve specific task IDs or run the main discovery pipeline.
"""

import os
import sys
import argparse
import getpass
from dotenv import load_dotenv
from services.discovery.discovery_enhanced import main as discovery_main
from services.discovery.discovery_enhanced import retrieve_known_tasks, get_db_connection

def parse_args():
    parser = argparse.ArgumentParser(description='Run DataForSEO discovery tasks')
    
    # Create a mutually exclusive group for the two modes
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--run-main', action='store_true', help='Run the main discovery pipeline')
    mode_group.add_argument('--retrieve-tasks', action='store_true', help='Retrieve specific task IDs')
    
    # Arguments for task retrieval
    parser.add_argument('--task-ids', nargs='+', help='Task IDs to retrieve (required with --retrieve-tasks)')
    parser.add_argument('--task-file', help='File containing task IDs to retrieve (one per line)')
    
    # Environment variable overrides
    parser.add_argument('--max-cities', type=int, help='Maximum number of cities to process')
    parser.add_argument('--include-organic', action='store_true', help='Include organic search in addition to events search')
    parser.add_argument('--use-batching', action='store_true', help='Use batch processing instead of sequential')
    
    # DataForSEO credentials
    parser.add_argument('--dataforseo-login', help='DataForSEO API login')
    parser.add_argument('--dataforseo-password', help='DataForSEO API password')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Load environment variables from .env file if it exists
    load_dotenv()
    
    # Set environment variables based on command line arguments
    if args.max_cities is not None:
        os.environ['MAX_CITIES'] = str(args.max_cities)
    
    if args.include_organic:
        os.environ['INCLUDE_ORGANIC_SEARCH'] = 'true'
    
    if args.use_batching:
        os.environ['USE_BATCH_PROCESSING'] = 'true'
    
    # Handle DataForSEO credentials
    dataforseo_login = args.dataforseo_login or os.environ.get('DATAFORSEO_LOGIN')
    dataforseo_password = args.dataforseo_password or os.environ.get('DATAFORSEO_PASSWORD')
    
    # If credentials are not provided, prompt for them
    if not dataforseo_login:
        dataforseo_login = input("Enter DataForSEO login: ")
        os.environ['DATAFORSEO_LOGIN'] = dataforseo_login
    
    if not dataforseo_password:
        dataforseo_password = getpass.getpass("Enter DataForSEO password: ")
        os.environ['DATAFORSEO_PASSWORD'] = dataforseo_password
    
    # Run in the appropriate mode
    if args.run_main:
        print("Running main discovery pipeline...")
        discovery_main()
    elif args.retrieve_tasks:
        # Get task IDs from arguments or file
        task_ids = []
        if args.task_ids:
            task_ids.extend(args.task_ids)
        
        if args.task_file:
            try:
                with open(args.task_file, 'r') as f:
                    file_task_ids = [line.strip() for line in f if line.strip()]
                    task_ids.extend(file_task_ids)
                    print(f"Loaded {len(file_task_ids)} task IDs from {args.task_file}")
            except Exception as e:
                print(f"Error loading task IDs from file: {e}")
                return 1
        
        if not task_ids:
            print("Error: No task IDs provided. Use --task-ids or --task-file")
            return 1
        
        print(f"Retrieving {len(task_ids)} task IDs...")
        db_conn = get_db_connection()
        if db_conn:
            retrieve_known_tasks(task_ids, dataforseo_login, dataforseo_password, db_conn)
            db_conn.close()
        else:
            print("Error: Could not connect to database")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 