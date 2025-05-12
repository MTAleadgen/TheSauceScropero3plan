#!/usr/bin/env python3
"""
run_dance_discovery.py

Quick script to run the dance events pipeline with optimal settings.
This is a test script to verify the events endpoint is working properly 
before scaling up to all cities.

By default, this script only tests with 1 city to validate the workflow
before scaling to all cities.

Usage:
  python run_dance_discovery.py              # Run with 1 test city (default)
  python run_dance_discovery.py --full-run   # Run collection and processing
  python run_dance_discovery.py --collect    # Only collect data
  python run_dance_discovery.py --process    # Only process existing data
"""

import subprocess
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run dance events discovery with optimal settings")
    parser.add_argument("--city-limit", type=int, default=1, 
                        help="Number of cities to process (default: 1 for testing)")
    parser.add_argument("--collect", action="store_true", 
                        help="Only collect data from DataForSEO, skip processing")
    parser.add_argument("--process", action="store_true", 
                        help="Only process existing data, skip collection")
    parser.add_argument("--full-run", action="store_true", 
                        help="Run both collection and processing steps (default if no flags provided)")
    args = parser.parse_args()
    
    # Determine which mode to run
    if args.collect:
        run_mode = "--collect"
    elif args.process:
        run_mode = "--process"
    elif args.full_run:
        run_mode = "--full-run"
    else:
        # Default to full-run if no specific mode is provided
        run_mode = "--full-run"
    
    # Build command with optimal settings
    cmd = [
        "python", "dance_events_pipeline.py",
        run_mode,
        "--batch-size", "50",  # Process 50 records at a time
    ]
    
    # Add city limit (always included with default=1)
    cmd.extend(["--city-limit", str(args.city_limit)])
    
    print(f"\nRunning dance events discovery with {args.city_limit} {'city' if args.city_limit == 1 else 'cities'}")
    print(f"Command: {' '.join(cmd)}\n")
    
    # Execute the dance events pipeline
    try:
        subprocess.run(cmd, check=True)
        print("\nDance events discovery completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"\nError running dance events discovery: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 