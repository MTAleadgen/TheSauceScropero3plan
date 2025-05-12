#!/usr/bin/env python3
"""
mark_events_as_parsed.py

A script to directly mark event_raw records as parsed
so they can be processed by the normalize worker.
"""

import os
import sys
import json
from datetime import datetime, timezone
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

def mark_events_as_parsed(db_conn, limit=None):
    """Mark unparsed events in event_raw as parsed.
    
    Args:
        db_conn: Database connection
        limit: Optional limit of records to process
    """
    try:
        with db_conn.cursor() as cur:
            # Get count of unparsed records
            cur.execute("SELECT COUNT(*) FROM event_raw WHERE parsed_at IS NULL")
            unparsed_count = cur.fetchone()[0]
            print(f"Found {unparsed_count} unparsed records in event_raw")
            
            if unparsed_count == 0:
                print("No records to process.")
                return 0
            
            # Update records as parsed
            limit_clause = f"LIMIT {limit}" if limit else ""
            cur.execute(f"""
                UPDATE event_raw
                SET parsed_at = CURRENT_TIMESTAMP
                WHERE id IN (
                    SELECT id FROM event_raw
                    WHERE parsed_at IS NULL
                    ORDER BY id
                    {limit_clause}
                )
                RETURNING id;
            """)
            updated_ids = cur.fetchall()
            db_conn.commit()
            
            updated_count = len(updated_ids)
            print(f"Successfully marked {updated_count} records as parsed")
            
            if updated_count > 0:
                id_list = ", ".join(str(row['id']) for row in updated_ids[:10])
                if updated_count > 10:
                    id_list += f", ... and {updated_count - 10} more"
                print(f"Updated IDs: {id_list}")
            
            return updated_count
    
    except psycopg2.Error as e:
        db_conn.rollback()
        print(f"Database error: {e}")
        return 0

def ensure_json_data(db_conn):
    """Ensure raw_json field contains valid JSON data."""
    try:
        with db_conn.cursor() as cur:
            # Check for records that might have raw_json as string instead of JSON
            cur.execute("""
                SELECT id, raw_json
                FROM event_raw
                WHERE parsed_at IS NULL
                AND raw_json IS NOT NULL
                AND raw_json::TEXT NOT LIKE '{%'
                LIMIT 5;
            """)
            invalid_json_records = cur.fetchall()
            
            if not invalid_json_records:
                print("All raw_json fields appear to be valid JSON.")
                return
            
            print(f"Found {len(invalid_json_records)} records with potentially invalid JSON. Converting...")
            
            for record in invalid_json_records:
                record_id = record['id']
                
                try:
                    # Check if it's a JSON string that needs to be parsed
                    if isinstance(record['raw_json'], str):
                        # Try to parse the string as JSON
                        parsed_json = json.loads(record['raw_json'])
                        
                        # Update the record with properly formatted JSON
                        cur.execute(
                            "UPDATE event_raw SET raw_json = %s::jsonb WHERE id = %s",
                            (json.dumps(parsed_json), record_id)
                        )
                        print(f"  Fixed JSON for record ID {record_id}")
                except json.JSONDecodeError:
                    print(f"  Could not convert raw_json for record ID {record_id} - invalid JSON format")
                except Exception as e:
                    print(f"  Error processing record ID {record_id}: {e}")
            
            db_conn.commit()
            print("JSON conversion completed.")
    
    except psycopg2.Error as e:
        db_conn.rollback()
        print(f"Database error during JSON validation: {e}")

def main():
    """Main function."""
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="Mark event_raw records as parsed")
    parser.add_argument("--limit", type=int, help="Limit the number of records to process")
    parser.add_argument("--fix-json", action="store_true", help="Fix any invalid JSON in raw_json field")
    args = parser.parse_args()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        if args.fix_json:
            ensure_json_data(db_conn)
        
        updated_count = mark_events_as_parsed(db_conn, args.limit)
        
        # Check how many records are now ready for normalization
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM event_raw
                WHERE parsed_at IS NOT NULL
                AND normalized_at IS NULL
            """)
            ready_for_normalization = cur.fetchone()[0]
            print(f"\nRecords ready for normalization: {ready_for_normalization}")
            
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 