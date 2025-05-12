#!/usr/bin/env python3
"""
check_event_raw_status.py

A script to check the status of records in the event_raw table
to diagnose worker processing issues.
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

def initialize_db():
    """Initialize database connection."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL environment variable not set.")
        return None
    
    try:
        conn = psycopg2.connect(db_url, cursor_factory=DictCursor)
        print("Connected to database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def check_event_raw_status(db_conn):
    """Check the status of records in the event_raw table."""
    try:
        with db_conn.cursor() as cur:
            # Get total count
            cur.execute("SELECT COUNT(*) FROM event_raw")
            total_count = cur.fetchone()[0]
            print(f"Total records in event_raw: {total_count}")
            
            # Get count of records ready for normalization
            cur.execute("""
                SELECT COUNT(*) FROM event_raw
                WHERE parsed_at IS NOT NULL
                AND normalized_at IS NULL
            """)
            ready_for_normalization = cur.fetchone()[0]
            print(f"Records ready for normalization (parsed_at IS NOT NULL AND normalized_at IS NULL): {ready_for_normalization}")
            
            # Get counts by status
            cur.execute("""
                SELECT 
                    CASE 
                        WHEN parsed_at IS NULL THEN 'not_parsed'
                        WHEN normalized_at IS NULL THEN 'parsed_not_normalized'
                        ELSE normalization_status 
                    END AS status,
                    COUNT(*) as count
                FROM event_raw
                GROUP BY status
                ORDER BY count DESC
            """)
            status_counts = cur.fetchall()
            print("\nStatus breakdown:")
            for status in status_counts:
                print(f"  {status['status']}: {status['count']}")
                
            # Check latest records
            print("\nLast 5 records in event_raw:")
            cur.execute("""
                SELECT id, source, parsed_at, normalized_at, normalization_status
                FROM event_raw
                ORDER BY id DESC
                LIMIT 5
            """)
            latest_records = cur.fetchall()
            for record in latest_records:
                print(f"  ID: {record['id']}, Source: {record['source']}, Parsed: {'Yes' if record['parsed_at'] else 'No'}, " 
                      f"Normalized: {'Yes' if record['normalized_at'] else 'No'}, Status: {record['normalization_status']}")
    
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    """Main function to check event_raw status."""
    load_dotenv()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        check_event_raw_status(db_conn)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 