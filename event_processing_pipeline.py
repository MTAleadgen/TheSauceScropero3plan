#!/usr/bin/env python3
"""
event_processing_pipeline.py

Main orchestration script for the complete event processing pipeline:
1. Process raw event data through normalizers
2. Use Qwen 3 to enrich event data with missing fields
3. Fix geocoding errors with Google Places API
"""

import os
import sys
import json
import datetime
import time
import argparse
import boto3
import psycopg2
from psycopg2.extras import DictCursor, Json
from dotenv import load_dotenv

# Import the Places API helper
from places_api_helper import (
    resolve_venue_address,
    setup_venue_cache_table,
    get_api_usage_stats
)

# Load environment variables
load_dotenv()

# Initialize database connection
def initialize_db():
    """Initialize database connection."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL environment variable not set.")
        return None
    
    try:
        conn = psycopg2.connect(db_url, cursor_factory=DictCursor)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

# Initialize AWS Lambda client
def initialize_lambda():
    """Initialize AWS Lambda client for Qwen 3 access."""
    try:
        session = boto3.Session(
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_REGION', 'us-east-1')
        )
        return session.client('lambda')
    except Exception as e:
        print(f"Error initializing Lambda client: {e}")
        return None

def get_pending_normalizations(conn, limit=100):
    """Get records from event_raw that need normalization."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, raw_json 
                FROM event_raw 
                WHERE normalization_status IS NULL
                ORDER BY id 
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error fetching pending normalizations: {e}")
        return []

def get_events_needing_enrichment(conn, limit=50):
    """
    Get records from event_clean that have missing fields 
    that could be extracted from description using Qwen 3.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, json_data 
                FROM event_clean 
                WHERE (
                    json_data->>'description' IS NOT NULL AND 
                    json_data->>'description' != '' AND
                    (
                        json_data->>'price' IS NULL OR
                        json_data->>'eventAttendanceMode' IS NULL OR
                        json_data->>'eventStatus' IS NULL OR
                        json_data->'location'->>'address' IS NULL OR
                        json_data->'organizer'->>'name' IS NULL
                    )
                )
                AND enrichment_status IS NULL
                ORDER BY id 
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error fetching events needing enrichment: {e}")
        return []

def get_geocoding_errors(conn, limit=50):
    """Get records from event_raw with geocoding/location errors."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, raw_json 
                FROM event_raw 
                WHERE normalization_status = 'error'
                AND raw_json->>'location' IS NOT NULL
                ORDER BY id 
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error fetching geocoding errors: {e}")
        return []

def invoke_qwen_enrichment(lambda_client, event_data):
    """
    Invoke Qwen 3 via Lambda to extract missing fields from event description.
    
    Args:
        lambda_client: Initialized AWS Lambda client
        event_data: Dict containing event information with description
        
    Returns:
        Dict with extracted fields or None if processing failed
    """
    if not lambda_client:
        print("Lambda client not initialized")
        return None
    
    try:
        # Prepare payload for Lambda function
        payload = {
            "event_id": event_data['id'],
            "description": event_data['json_data'].get('description', ''),
            "title": event_data['json_data'].get('name', ''),
            "fields_to_extract": [
                "price",
                "eventAttendanceMode",
                "eventStatus",
                "organizer_name",
                "additional_location_details"
            ]
        }
        
        # Invoke Lambda function
        lambda_function_name = os.environ.get('QWEN_LAMBDA_FUNCTION', 'event-enrichment-qwen')
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Process response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        if response['StatusCode'] != 200:
            print(f"Lambda invocation error: {response_payload.get('errorMessage', 'Unknown error')}")
            return None
            
        return response_payload
        
    except Exception as e:
        print(f"Error invoking Qwen 3 Lambda: {e}")
        return None

def update_event_with_enrichment(conn, event_id, enrichment_data):
    """Update event_clean record with enrichment data from Qwen 3."""
    if not enrichment_data:
        return False
        
    try:
        with conn.cursor() as cur:
            # Get current event data
            cur.execute("""
                SELECT json_data FROM event_clean WHERE id = %s
            """, (event_id,))
            result = cur.fetchone()
            if not result:
                return False
                
            current_data = result[0]
            
            # Update fields based on enrichment data
            if 'price' in enrichment_data and enrichment_data['price']:
                current_data['price'] = enrichment_data['price']
                
            if 'eventAttendanceMode' in enrichment_data and enrichment_data['eventAttendanceMode']:
                current_data['eventAttendanceMode'] = enrichment_data['eventAttendanceMode']
                
            if 'eventStatus' in enrichment_data and enrichment_data['eventStatus']:
                current_data['eventStatus'] = enrichment_data['eventStatus']
                
            if 'organizer_name' in enrichment_data and enrichment_data['organizer_name']:
                if 'organizer' not in current_data:
                    current_data['organizer'] = {"@type": "Organization"}
                current_data['organizer']['name'] = enrichment_data['organizer_name']
                
            if 'additional_location_details' in enrichment_data and enrichment_data['additional_location_details']:
                # Only update if we have a location object
                if 'location' in current_data:
                    # Add any additional location details that might help with geocoding later
                    if 'address' not in current_data['location'] and enrichment_data['additional_location_details'].get('address'):
                        current_data['location']['address'] = enrichment_data['additional_location_details']['address']
            
            # Update the record
            cur.execute("""
                UPDATE event_clean
                SET json_data = %s::jsonb,
                    enrichment_status = 'processed',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (Json(current_data), event_id))
            
            conn.commit()
            return True
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error updating event with enrichment: {e}")
        return False

def fix_geocoding_with_places_api(conn, error_records):
    """
    Fix geocoding errors using Google Places API.
    
    Args:
        conn: Database connection
        error_records: List of records with geocoding errors
        
    Returns:
        tuple: (fixed_count, total_count)
    """
    fixed_count = 0
    total_count = len(error_records)
    
    for record in error_records:
        record_id = record[0]
        raw_json = record[1]
        
        if not isinstance(raw_json, dict) or 'location' not in raw_json:
            continue
            
        location = raw_json['location']
        if not isinstance(location, dict):
            continue
            
        venue_name = location.get('name')
        address = location.get('address')
        
        if not venue_name:
            continue
            
        # Try to extract city from address or event title
        city = None
        if address and isinstance(address, str) and ',' in address:
            parts = address.split(',')
            if len(parts) >= 2:
                # Take the last meaningful part as city
                for i in range(len(parts)-1, 0, -1):
                    potential_city = parts[i].strip()
                    if len(potential_city) > 2 and not potential_city.isdigit():
                        city = potential_city
                        break
        
        # If city not found in address, try looking in the event title
        if not city and 'name' in raw_json:
            title = raw_json['name']
            for common_city in ['New York', 'San Francisco', 'Los Angeles', 'Chicago', 'London', 
                               'Paris', 'Barcelona', 'Madrid', 'Berlin', 'Rio', 'Tokyo']:
                if common_city in title:
                    city = common_city
                    break
        
        venue_data = resolve_venue_address(venue_name, address, city)
        
        if venue_data:
            # Update the record with resolved location
            updated_location = {
                "@type": "Place",
                "name": venue_name
            }
            
            if venue_data['formatted_address']:
                updated_location["address"] = venue_data['formatted_address']
                
            if venue_data['latitude'] and venue_data['longitude']:
                updated_location["geo"] = {
                    "@type": "GeoCoordinates",
                    "latitude": venue_data['latitude'],
                    "longitude": venue_data['longitude']
                }
                
            # Update the raw_json
            raw_json['location'] = updated_location
            
            try:
                # Reset normalization status to try again
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE event_raw
                        SET raw_json = %s::jsonb,
                            normalized_at = NULL,
                            normalization_status = NULL
                        WHERE id = %s
                    """, (Json(raw_json), record_id))
                    
                    conn.commit()
                    fixed_count += 1
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Database error updating record {record_id}: {e}")
    
    return fixed_count, total_count

def run_normalizers():
    """Run the normalization worker to process event_raw records."""
    # In a real implementation, this might start a worker process
    # or send a message to a queue to trigger normalization
    print("Running normalizers to process raw events...")
    # For now, we'll just simulate this with a placeholder
    time.sleep(2)  # Simulate some processing time
    print("Normalization workers triggered")

def process_pipeline(args):
    """Run the complete event processing pipeline."""
    conn = initialize_db()
    if not conn:
        print("Failed to connect to database, exiting.")
        return
        
    # Ensure venue cache table exists
    setup_venue_cache_table(conn)
    
    lambda_client = None
    if not args.skip_enrichment:
        lambda_client = initialize_lambda()
        if not lambda_client and not args.skip_enrichment:
            print("Warning: Lambda client initialization failed. Skipping enrichment steps.")
    
    try:
        # Step 1: Check for and process pending normalizations
        if not args.skip_normalize:
            pending_records = get_pending_normalizations(conn, args.batch_size)
            if pending_records:
                print(f"Found {len(pending_records)} records needing normalization")
                run_normalizers()
            else:
                print("No records pending normalization")
        
        # Step 2: Enrich event_clean records using Qwen 3
        if not args.skip_enrichment and lambda_client:
            enrichment_records = get_events_needing_enrichment(conn, args.batch_size)
            if enrichment_records:
                print(f"Found {len(enrichment_records)} records needing enrichment")
                enriched_count = 0
                
                for record in enrichment_records:
                    event_id = record[0]
                    event_data = {
                        'id': event_id,
                        'json_data': record[1]
                    }
                    
                    print(f"Enriching event ID {event_id}...")
                    enrichment_data = invoke_qwen_enrichment(lambda_client, event_data)
                    
                    if enrichment_data:
                        success = update_event_with_enrichment(conn, event_id, enrichment_data)
                        if success:
                            enriched_count += 1
                            print(f"  Successfully enriched event {event_id}")
                        else:
                            print(f"  Failed to update event {event_id} with enrichment data")
                    else:
                        print(f"  Failed to get enrichment data for event {event_id}")
                        
                        # Mark as processed even if we couldn't enrich it
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE event_clean
                                SET enrichment_status = 'failed',
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (event_id,))
                            conn.commit()
                
                print(f"Enriched {enriched_count} out of {len(enrichment_records)} records")
            else:
                print("No records needing enrichment")
        
        # Step 3: Fix geocoding errors with Google Places API
        if not args.skip_geocoding:
            api_stats = get_api_usage_stats()
            if api_stats:
                print("\n=== Google Places API Usage ===")
                print(f"Daily usage: {api_stats['daily_usage']}/{api_stats['daily_limit']} ({api_stats['daily_percent']}%)")
                print(f"Monthly usage: {api_stats['monthly_usage']}/{api_stats['monthly_limit']} ({api_stats['monthly_percent']}%)")
                print(f"Cached venues: {api_stats['cached_venues']}")
                
                # Only proceed if we're under 80% of daily limit
                if api_stats['daily_percent'] < 80:
                    geocoding_errors = get_geocoding_errors(conn, args.batch_size)
                    if geocoding_errors:
                        print(f"\nFound {len(geocoding_errors)} records with geocoding errors")
                        fixed_count, total_count = fix_geocoding_with_places_api(conn, geocoding_errors)
                        print(f"Fixed {fixed_count} out of {total_count} geocoding errors")
                    else:
                        print("No records with geocoding errors")
                else:
                    print("API usage near limit, skipping geocoding fixes")
            else:
                print("Failed to get API usage stats, skipping geocoding fixes")
                
        print("\nPipeline processing complete")
    
    except Exception as e:
        print(f"Error in pipeline processing: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Event Processing Pipeline")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records to process in each step")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip normalization step")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip Qwen enrichment step")
    parser.add_argument("--skip-geocoding", action="store_true", help="Skip geocoding fixes with Places API")
    
    args = parser.parse_args()
    process_pipeline(args) 