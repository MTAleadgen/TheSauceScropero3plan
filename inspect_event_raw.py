#!/usr/bin/env python3
"""
inspect_event_raw.py

A script to inspect sample records from the event_raw table
to understand what data is available for normalization.
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

def inspect_sample_records(db_conn, limit=5, verbose=False):
    """Inspect a sample of records from the event_raw table."""
    try:
        with db_conn.cursor() as cur:
            # Get sample records
            cur.execute(f"""
                SELECT id, source, metro_id, raw_json, parsed_at, normalized_at, normalization_status
                FROM event_raw
                ORDER BY id
                LIMIT {limit};
            """)
            records = cur.fetchall()
            
            if not records:
                print("No records found in event_raw.")
                return
            
            print(f"Examining {len(records)} sample records from event_raw:")
            
            for record in records:
                record_id = record['id']
                print(f"\n{'='*70}")
                print(f"RECORD ID: {record_id}")
                print(f"Source: {record['source']}")
                print(f"Metro ID: {record['metro_id']}")
                print(f"Parsed at: {record['parsed_at']}")
                print(f"Normalized at: {record['normalized_at']}")
                print(f"Status: {record['normalization_status']}")
                
                # Check raw_json content
                raw_json = record['raw_json']
                if raw_json:
                    if isinstance(raw_json, str):
                        print("\nRaw JSON is stored as a string.")
                        try:
                            json_data = json.loads(raw_json)
                            print("JSON is valid and can be parsed.")
                        except json.JSONDecodeError:
                            print("WARNING: Invalid JSON string!")
                            print(f"Raw content (truncated): {raw_json[:200]}...")
                            continue
                    else:
                        json_data = raw_json
                        print("\nRaw JSON is stored as a Python object.")
                    
                    # Check for basic JSON-LD fields
                    name = None
                    start_date = None
                    try:
                        # Try to find name/title
                        if isinstance(json_data, dict):
                            name = json_data.get('name')
                            print(f"Event name: {name}")
                            
                            # Try to find start date
                            start_date = json_data.get('startDate')
                            print(f"Start date: {start_date}")
                            
                            # Other potentially useful fields
                            print(f"Description: {json_data.get('description', '')[:100]}...")
                            
                            # Check if it has a location
                            if 'location' in json_data:
                                location = json_data['location']
                                print("Has location data.")
                            else:
                                print("No location data found.")
                                
                            # Show keys in the JSON
                            print(f"JSON keys: {list(json_data.keys())}")
                            
                            # Check for DataForSEO events_data
                            if 'events_data' in json_data:
                                events_data = json_data.get('events_data', [])
                                if events_data and isinstance(events_data, list):
                                    print(f"\nFound {len(events_data)} events in events_data array.")
                                    
                                    # Look at first event
                                    if events_data and len(events_data) > 0:
                                        event = events_data[0]
                                        if isinstance(event, dict):
                                            print("\nFirst event structure:")
                                            for key, value in event.items():
                                                print(f"  {key}: {type(value).__name__}")
                                                if verbose:
                                                    if isinstance(value, (dict, list)):
                                                        print(f"    {value}")
                                                    else:
                                                        print(f"    {value}")
                                            
                                            # Show title and date
                                            title = event.get('title')
                                            date_info = event.get('date')
                                            print(f"\nEvent title: {title}")
                                            print(f"Event date info: {date_info}")
                                            
                                            # Show venue information
                                            venue_info = event.get('venue')
                                            print(f"Venue info: {venue_info}")
                                else:
                                    print("events_data array is empty or not a list.")
                        else:
                            print(f"JSON is not a dictionary but a {type(json_data)}")
                            
                    except Exception as e:
                        print(f"Error examining JSON data: {e}")
                else:
                    print("\nNo raw_json data found!")
                    
            print(f"\n{'='*70}")
            
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    """Main function."""
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="Inspect sample records from event_raw")
    parser.add_argument("--limit", type=int, default=5, help="Number of records to inspect")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show more detailed output")
    args = parser.parse_args()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        inspect_sample_records(db_conn, args.limit, args.verbose)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 