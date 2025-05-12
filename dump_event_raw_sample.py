#!/usr/bin/env python3
"""
dump_event_raw_sample.py

A script to dump the raw JSON content of a sample event record
to better understand the DataForSEO format.
"""

import os
import sys
import json
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

def dump_record(db_conn, record_id=None):
    """Dump the raw JSON content of a record from event_raw."""
    try:
        with db_conn.cursor() as cur:
            if record_id is None:
                # Get the first record
                cur.execute("""
                    SELECT id, raw_json FROM event_raw
                    ORDER BY id
                    LIMIT 1;
                """)
            else:
                # Get the specified record
                cur.execute("""
                    SELECT id, raw_json FROM event_raw
                    WHERE id = %s;
                """, (record_id,))
            
            record = cur.fetchone()
            
            if not record:
                print(f"No record found with ID {record_id}.")
                return
            
            record_id = record['id']
            raw_json = record['raw_json']
            
            print(f"Dumping raw_json for record ID {record_id}:")
            
            if isinstance(raw_json, str):
                try:
                    # Parse JSON string
                    json_data = json.loads(raw_json)
                    print(json.dumps(json_data, indent=2))
                except json.JSONDecodeError:
                    print("Invalid JSON string!")
                    print(raw_json)
            else:
                # Already a Python object
                print(json.dumps(raw_json, indent=2, default=str))
            
            # If there's a events_data field, examine its structure
            if isinstance(raw_json, dict) and 'events_data' in raw_json:
                events_data = raw_json.get('events_data', [])
                if events_data and isinstance(events_data, list):
                    print(f"\nFound {len(events_data)} events in events_data array.")
                    
                    # Print the first event's structure
                    if events_data:
                        first_event = events_data[0]
                        print("\nFirst event structure:")
                        
                        if isinstance(first_event, dict):
                            print("\nEvent keys:")
                            for key in first_event:
                                print(f"  - {key}")
                            
                            # Check if there are 'items' in the event
                            if 'items' in first_event and isinstance(first_event['items'], list):
                                items = first_event['items']
                                print(f"\nFound {len(items)} items in first event.")
                                
                                # Examine the first item
                                if items:
                                    first_item = items[0]
                                    print("\nFirst item structure:")
                                    if isinstance(first_item, dict):
                                        print(json.dumps(first_item, indent=2, default=str))
                                    else:
                                        print(f"Item is not a dictionary: {first_item}")
                        else:
                            print(f"Event is not a dictionary: {first_event}")
                else:
                    print("events_data is not a list or is empty.")
    
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    """Main function."""
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="Dump raw JSON content from event_raw")
    parser.add_argument("--id", type=int, help="Record ID to dump (default: first record)")
    args = parser.parse_args()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        dump_record(db_conn, args.id)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 