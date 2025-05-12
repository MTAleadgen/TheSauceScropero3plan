#!/usr/bin/env python3
"""
run_workers.py

A script to run all data processing workers for the event discovery system.
It provides options to run specific workers or all workers at once.
"""

import os
import sys
import argparse
import time
import threading
from dotenv import load_dotenv

# Import worker functions from each worker module
from worker_fetch import initialize_redis as fetch_init_redis, worker_fetch
from worker_parse import initialize_redis as parse_init_redis, initialize_db as parse_init_db, worker_parse 
from worker_normalize import initialize_db as normalize_init_db, worker_normalize

def run_fetch_worker():
    """Run the fetch worker in a separate thread."""
    print("Starting fetch worker thread...")
    redis_conn = fetch_init_redis()
    if not redis_conn:
        print("Failed to initialize Redis connection for fetch worker. Exiting.")
        return
    
    try:
        worker_fetch(redis_conn)
    except Exception as e:
        print(f"Error in fetch worker: {e}")

def run_parse_worker():
    """Run the parse worker in a separate thread."""
    print("Starting parse worker thread...")
    redis_conn = parse_init_redis()
    db_conn = parse_init_db()
    
    if not redis_conn or not db_conn:
        print("Failed to initialize connections for parse worker. Exiting.")
        return
    
    try:
        worker_parse(redis_conn, db_conn)
    except Exception as e:
        print(f"Error in parse worker: {e}")

def run_normalize_worker():
    """Run the normalize worker in a separate thread."""
    print("Starting normalize worker thread...")
    db_conn = normalize_init_db()
    
    if not db_conn:
        print("Failed to initialize database connection for normalize worker. Exiting.")
        return
    
    try:
        worker_normalize(db_conn)
    except Exception as e:
        print(f"Error in normalize worker: {e}")

def main():
    """Run the selected worker(s) based on command line arguments."""
    parser = argparse.ArgumentParser(description="Run event discovery data processing workers")
    parser.add_argument("--fetch", action="store_true", help="Run the fetch worker")
    parser.add_argument("--parse", action="store_true", help="Run the parse worker")
    parser.add_argument("--normalize", action="store_true", help="Run the normalize worker")
    parser.add_argument("--all", action="store_true", help="Run all workers")
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not (args.fetch or args.parse or args.normalize or args.all):
        parser.print_help()
        sys.exit(1)
    
    # Load environment variables
    load_dotenv()
    
    threads = []
    
    # Start requested workers
    if args.all or args.fetch:
        fetch_thread = threading.Thread(target=run_fetch_worker)
        fetch_thread.daemon = True
        fetch_thread.start()
        threads.append(fetch_thread)
    
    if args.all or args.parse:
        parse_thread = threading.Thread(target=run_parse_worker)
        parse_thread.daemon = True
        parse_thread.start()
        threads.append(parse_thread)
    
    if args.all or args.normalize:
        normalize_thread = threading.Thread(target=run_normalize_worker)
        normalize_thread.daemon = True
        normalize_thread.start()
        threads.append(normalize_thread)
    
    # Keep the main thread alive until Ctrl+C
    try:
        while True:
            still_running = False
            for thread in threads:
                if thread.is_alive():
                    still_running = True
                    break
            
            if not still_running:
                print("All worker threads have exited.")
                break
            
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutdown signal received. Waiting for workers to finish...")
        print("(This may take a moment, and you may need to press Ctrl+C again to force exit)")

if __name__ == "__main__":
    main() 