#!/usr/bin/env python3
"""
run_normalize_worker.py

A script to run the normalizer worker that processes data
from event_raw to event_clean tables.
"""

import os
import sys
import time
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

# Import the worker_normalize function
from worker_normalize import worker_normalize, initialize_db

def main():
    """Run the normalize worker to process event_raw data into event_clean."""
    print("Starting normalize worker process...")
    
    # Load environment variables
    load_dotenv()
    
    # Initialize database connection
    db_conn = initialize_db()
    if not db_conn:
        print("Failed to initialize database connection. Exiting.")
        sys.exit(1)
    
    try:
        # Run the worker process
        worker_normalize(db_conn)
    except KeyboardInterrupt:
        print("\nShutdown signal received, closing connections...")
    finally:
        # Clean up database connection
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 