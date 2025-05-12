#!/usr/bin/env python3
"""
transform_event_raw.py

A script to transform the raw_json in event_raw from DataForSEO format
to the JSON-LD format expected by the normalize worker.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor, Json

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

def format_date(date_str):
    """Convert date string to ISO format."""
    if not date_str:
        return None
    try:
        # Handle various date formats
        formats = [
            "%Y-%m-%d %H:%M:%S", 
            "%Y-%m-%d", 
            "%b %d, %Y, %I:%M %p",
            "%b %d, %Y",
            "%A, %B %d, %Y at %I:%M %p"
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.isoformat()
            except ValueError:
                continue
                
        # If none of the formats worked, return the original string
        return date_str
    except Exception:
        return date_str

def transform_event_raw(db_conn, limit=None, dry_run=False, specific_id=None):
    """
    Transform event_raw records from DataForSEO format to the expected JSON-LD format.
    
    Args:
        db_conn: Database connection
        limit: Optional limit of records to process
        dry_run: If True, don't update the database
        specific_id: Optional ID to process only one specific record
    """
    try:
        with db_conn.cursor() as cur:
            # Get records to transform
            if specific_id:
                cur.execute(
                    "SELECT id, source, metro_id, raw_json FROM event_raw WHERE id = %s",
                    (specific_id,)
                )
            else:
                limit_clause = f"LIMIT {limit}" if limit else ""
                cur.execute(f"""
                    SELECT id, source, metro_id, raw_json 
                    FROM event_raw
                    WHERE normalization_status = 'error'
                    OR (normalized_at IS NULL AND parsed_at IS NOT NULL)
                    ORDER BY id
                    {limit_clause};
                """)
            records = cur.fetchall()
            
            if not records:
                print("No records found to transform.")
                return 0
            
            print(f"Found {len(records)} records to transform.")
            processed_count = 0
            
            for record in records:
                record_id = record['id']
                raw_json = record['raw_json']
                
                if not isinstance(raw_json, dict):
                    print(f"Skipping record {record_id}: raw_json is not a dictionary.")
                    continue
                
                # Check for DataForSEO format
                if 'events_data' in raw_json and isinstance(raw_json['events_data'], list):
                    events_data = raw_json['events_data']
                    if not events_data:
                        print(f"Skipping record {record_id}: events_data is empty.")
                        continue
                    
                    print(f"\nProcessing record {record_id} with DataForSEO format:")
                    events_transformed = 0
                    
                    # Process the first event group in events_data
                    event_group = events_data[0]
                    if not isinstance(event_group, dict) or 'items' not in event_group:
                        print(f"  Skipping record {record_id}: Invalid event_group structure.")
                        continue
                    
                    items = event_group.get('items', [])
                    if not items or not isinstance(items, list):
                        print(f"  Skipping record {record_id}: No event items found.")
                        continue
                    
                    # Get item info from first event item
                    event_item = items[0]
                    if not isinstance(event_item, dict):
                        print(f"  Skipping record {record_id}: Event item is not a dictionary.")
                        continue
                    
                    # Extract event details from the item
                    title = event_item.get('title')
                    if not title:
                        print(f"  Skipping record {record_id}: No title found.")
                        continue
                    
                    # Extract date information
                    event_dates = event_item.get('event_dates', {})
                    start_datetime = None
                    end_datetime = None
                    
                    if isinstance(event_dates, dict):
                        start_datetime = event_dates.get('start_datetime')
                        end_datetime = event_dates.get('end_datetime')
                    
                    if not start_datetime:
                        print(f"  Skipping record {record_id}: No start date found for event '{title}'.")
                        continue
                    
                    # Extract location information
                    location_info = event_item.get('location_info', {})
                    venue_name = None
                    venue_address = None
                    location_data = {}
                    
                    if isinstance(location_info, dict):
                        venue_name = location_info.get('name')
                        venue_address = location_info.get('address')
                        
                        if venue_name or venue_address:
                            location_data = {
                                "@type": "Place",
                                "name": venue_name
                            }
                            
                            if venue_address:
                                location_data["address"] = venue_address
                    
                    # Create JSON-LD representation
                    json_ld = {
                        "@context": "http://schema.org",
                        "@type": "Event",
                        "name": title,
                        "startDate": start_datetime
                    }
                    
                    # Add optional fields
                    if end_datetime:
                        json_ld["endDate"] = end_datetime
                    
                    description = event_item.get('description')
                    if description:
                        json_ld["description"] = description
                    
                    if location_data:
                        json_ld["location"] = location_data
                    
                    url = event_item.get('url')
                    if url:
                        json_ld["url"] = url
                    
                    image_url = event_item.get('image_url')
                    if image_url:
                        json_ld["image"] = image_url
                    
                    # Include dance style context if available
                    metadata = raw_json.get('_script_discovery_metadata', {})
                    if isinstance(metadata, dict):
                        dance_style = metadata.get('dance_style_context')
                        if dance_style and dance_style != "Unknown (Retrieved by ID)":
                            if json_ld.get("description"):
                                json_ld["description"] += f"\n\nDance Style: {dance_style}"
                            else:
                                json_ld["description"] = f"Dance Style: {dance_style}"
                    
                    # Add city/location context from metadata or keyword
                    city_name = None
                    if isinstance(metadata, dict):
                        city_name = metadata.get('city_name_context')
                    
                    if not city_name or city_name == "Unknown (Retrieved by ID)":
                        # Try to extract from the keyword
                        keyword = event_group.get('keyword', '')
                        if keyword and 'in ' in keyword:
                            city_name = keyword.split('in ', 1)[1].strip()
                    
                    if city_name:
                        # Add city to location data if available
                        if "location" in json_ld:
                            if "address" in json_ld["location"] and isinstance(json_ld["location"]["address"], str):
                                # Don't add city if it's already in the address
                                if city_name.lower() not in json_ld["location"]["address"].lower():
                                    json_ld["location"]["address"] += f", {city_name}"
                            elif "address" not in json_ld["location"]:
                                json_ld["location"]["address"] = city_name
                        else:
                            json_ld["location"] = {
                                "@type": "Place",
                                "address": city_name
                            }
                    
                    # Store original metro_id if available
                    metro_id = record['metro_id']
                    
                    # Print the transformed data
                    print(f"  Transformed event: '{title}' on {start_datetime}")
                    
                    # Update the record in the database
                    if not dry_run:
                        try:
                            # Reset normalization status
                            cur.execute(
                                """UPDATE event_raw 
                                SET raw_json = %s::jsonb, 
                                    normalized_at = NULL, 
                                    normalization_status = NULL
                                WHERE id = %s""",
                                (Json(json_ld), record_id)
                            )
                            db_conn.commit()
                            processed_count += 1
                            events_transformed += 1
                            print(f"  Successfully updated record {record_id} with transformed data.")
                        except Exception as e:
                            db_conn.rollback()
                            print(f"  Error updating record {record_id}: {e}")
                    else:
                        print("  Dry run - no update performed.")
                        print(f"  JSON-LD structure would be: {json.dumps(json_ld, indent=2)[:300]}...")
                
                else:
                    print(f"Skipping record {record_id}: Not in DataForSEO format.")
            
            return processed_count
    
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return 0

def main():
    """Main function."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Transform event_raw records to the expected format")
    parser.add_argument("--limit", type=int, help="Limit the number of records to process")
    parser.add_argument("--id", type=int, help="Process a specific record ID")
    parser.add_argument("--dry-run", action="store_true", help="Don't update the database, just show what would be done")
    args = parser.parse_args()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        processed_count = transform_event_raw(db_conn, args.limit, args.dry_run, args.id)
        
        if not args.dry_run:
            print(f"\nSuccessfully transformed {processed_count} records.")
            
            # Check how many records are now ready for normalization
            with db_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM event_raw
                    WHERE parsed_at IS NOT NULL
                    AND normalized_at IS NULL
                """)
                ready_for_normalization = cur.fetchone()[0]
                print(f"Records ready for normalization: {ready_for_normalization}")
        else:
            print("\nDry run completed. No records were updated.")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 