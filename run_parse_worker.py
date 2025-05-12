#!/usr/bin/env python3
"""
run_parse_worker.py

A script to run the parse worker that processes raw event data
before it can be normalized.
"""

import os
import sys
import time
from dotenv import load_dotenv

# Import the worker_parse function and initialization functions
from worker_parse import worker_parse, initialize_redis, initialize_db

def main():
    """Run the parse worker to process raw event data."""
    print("Starting parse worker process...")
    
    # Load environment variables
    load_dotenv()
    
    # Initialize connections
    redis_conn = initialize_redis()
    if not redis_conn:
        print("Failed to initialize Redis connection. Exiting.")
        sys.exit(1)
    
    db_conn = initialize_db()
    if not db_conn:
        print("Failed to initialize database connection. Exiting.")
        sys.exit(1)
    
    try:
        # Run the worker process
        worker_parse(redis_conn, db_conn)
    except KeyboardInterrupt:
        print("\nShutdown signal received, closing connections...")
    finally:
        # Clean up connections
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 