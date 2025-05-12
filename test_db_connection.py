#!/usr/bin/env python3
"""
Test database connection without password prompts
"""
import os
import sys
import psycopg2
import dotenv
from pathlib import Path

# Load environment variables
dotenv.load_dotenv()

# Get DATABASE_URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

def main():
    """Test database connection and run a simple query"""
    print("Testing database connection...")
    
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable not set.")
        print("Set it in your .env file or in your environment.")
        sys.exit(1)
    
    try:
        # Connect to the database
        print(f"Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        
        # Create a cursor
        with conn.cursor() as cur:
            # Execute a simple query
            print("Executing query...")
            cur.execute("SELECT COUNT(*) FROM event_raw")
            
            # Fetch the result
            count = cur.fetchone()[0]
            print(f"Found {count} records in the event_raw table")
            
            # Try another query to see some sample data
            print("\nFetching 5 sample records...")
            cur.execute("SELECT id, source, source_event_id, metro_id, normalization_status FROM event_raw LIMIT 5")
            
            # Print column names
            col_names = [desc[0] for desc in cur.description]
            print(" | ".join(col_names))
            print("-" * 80)
            
            # Print results
            for row in cur.fetchall():
                print(" | ".join(str(v) for v in row))
        
        # Close the connection
        conn.close()
        print("\nDatabase test completed successfully.")
        
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 