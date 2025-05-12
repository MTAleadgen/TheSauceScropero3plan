#!/usr/bin/env python3
"""
check_event_clean.py

A script to check the number of records in the event_clean table
and compare with the event_raw table status.
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

def check_event_stats(db_conn):
    """Check the status of records in both event_raw and event_clean tables."""
    try:
        with db_conn.cursor() as cur:
            # Count records in event_clean
            cur.execute("SELECT COUNT(*) FROM event_clean")
            clean_count = cur.fetchone()[0]
            print(f"Total records in event_clean: {clean_count}")
            
            # Count records in event_raw
            cur.execute("SELECT COUNT(*) FROM event_raw")
            raw_count = cur.fetchone()[0]
            print(f"Total records in event_raw: {raw_count}")
            
            # Count event_raw records by status
            cur.execute("""
                SELECT 
                    normalization_status,
                    COUNT(*) as count
                FROM event_raw
                GROUP BY normalization_status
                ORDER BY count DESC
            """)
            status_counts = cur.fetchall()
            print("\nEvent_raw status breakdown:")
            for status in status_counts:
                status_name = status['normalization_status'] if status['normalization_status'] else 'NULL'
                print(f"  {status_name}: {status['count']}")
            
            # Check which event_raw IDs made it to event_clean
            cur.execute("""
                SELECT COUNT(*) FROM event_raw er
                JOIN event_clean ec ON er.id = ec.event_raw_id
                WHERE er.normalization_status = 'processed'
            """)
            processed_match = cur.fetchone()[0]
            print(f"\nProcessed records that match between tables: {processed_match}")
            
            # Check the latest records in event_clean
            print("\nLatest 5 records in event_clean:")
            cur.execute("""
                SELECT 
                    ec.id, 
                    ec.event_raw_id, 
                    ec.title, 
                    ec.start_ts, 
                    ec.venue_name,
                    ec.normalized_at
                FROM event_clean ec
                ORDER BY ec.id DESC
                LIMIT 5
            """)
            latest_records = cur.fetchall()
            for record in latest_records:
                print(f"  ID: {record['id']}, Raw ID: {record['event_raw_id']}, Title: {record['title'][:30]}...")
            
            # Try to identify why some records might not have been processed
            cur.execute("""
                SELECT COUNT(*) FROM event_raw
                WHERE parsed_at IS NOT NULL
                AND normalized_at IS NULL
            """)
            waiting_count = cur.fetchone()[0]
            print(f"\nRecords waiting to be normalized: {waiting_count}")
            
            if waiting_count > 0:
                # Get sample of waiting records
                cur.execute("""
                    SELECT id, source FROM event_raw
                    WHERE parsed_at IS NOT NULL
                    AND normalized_at IS NULL
                    LIMIT 3
                """)
                waiting_sample = cur.fetchall()
                print("Sample of waiting records:")
                for record in waiting_sample:
                    print(f"  ID: {record['id']}, Source: {record['source']}")
            
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    """Main function."""
    load_dotenv()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        check_event_stats(db_conn)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 