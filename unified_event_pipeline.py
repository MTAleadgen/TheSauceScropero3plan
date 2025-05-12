#!/usr/bin/env python3
"""
unified_event_pipeline.py

Complete end-to-end pipeline for event discovery and processing:
1. DataForSEO data collection (9 queries per city × 1785 cities)
2. Normalization of raw data into event_clean
3. Enrichment of events with Qwen 3
4. Geocoding fixes with Google Places API

Usage:
  python unified_event_pipeline.py --full-run    # Run the complete pipeline
  python unified_event_pipeline.py --collect     # Only collect data from DataForSEO
  python unified_event_pipeline.py --process     # Only process existing data
"""

import os
import sys
import json
import time
import datetime
import argparse
import requests
import psycopg2
import random
import base64
from psycopg2.extras import DictCursor, Json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import boto3

# Import Places API helper functions
from places_api_helper import (
    resolve_venue_address,
    setup_venue_cache_table,
    get_api_usage_stats
)

# Load environment variables
load_dotenv()

# DataForSEO API credentials
DATAFORSEO_API_LOGIN = os.environ.get('DATAFORSEO_API_LOGIN')
DATAFORSEO_API_PASSWORD = os.environ.get('DATAFORSEO_API_PASSWORD')
if not DATAFORSEO_API_LOGIN or not DATAFORSEO_API_PASSWORD:
    print("Warning: DataForSEO API credentials not set in environment variables")

# Database connection
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

# DataForSEO Functions
def get_dataforseo_client():
    """Setup DataForSEO API client."""
    auth_string = f"{DATAFORSEO_API_LOGIN}:{DATAFORSEO_API_PASSWORD}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded_auth}',
        'Content-Type': 'application/json',
    }
    
    return headers

def load_cities(conn):
    """Load cities from the database or an external source."""
    try:
        with conn.cursor() as cur:
            # Try to get cities from a cities table if it exists
            try:
                cur.execute("SELECT id, name, country FROM cities ORDER BY id")
                cities = cur.fetchall()
                if cities:
                    return [{'id': city[0], 'name': city[1], 'country': city[2]} for city in cities]
            except psycopg2.Error:
                # Table might not exist
                pass
                
            # Alternative: Query distinct metro_id values from event_clean
            try:
                cur.execute("SELECT DISTINCT metro_id FROM event_clean")
                metro_ids = [row[0] for row in cur.fetchall() if row[0]]
                if metro_ids:
                    return [{'id': metro_id, 'name': f'Metro {metro_id}'} for metro_id in metro_ids]
            except psycopg2.Error:
                # Table might not exist or have the column
                pass
        
        # If we can't get cities from the database, use a default list
        # In a real implementation, you'd want a complete list of cities
        print("Warning: Using default test cities list. Replace with your actual city data.")
        return [
            {'id': 1, 'name': 'New York', 'country': 'US'},
            {'id': 2, 'name': 'Los Angeles', 'country': 'US'},
            {'id': 3, 'name': 'Chicago', 'country': 'US'},
            {'id': 4, 'name': 'London', 'country': 'UK'},
            {'id': 5, 'name': 'Paris', 'country': 'FR'},
        ]
    except Exception as e:
        print(f"Error loading cities: {e}")
        return []

def get_query_progress(conn):
    """Get the current progress of DataForSEO queries."""
    try:
        with conn.cursor() as cur:
            # Check if we have a progress tracking table
            try:
                cur.execute("""
                    SELECT COUNT(*) FROM dataforseo_query_progress 
                    WHERE status = 'completed'
                """)
                completed = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM dataforseo_query_progress")
                total = cur.fetchone()[0]
                
                return completed, total
            except psycopg2.Error:
                # Table might not exist
                pass
                
            # Alternative: Just count records in event_raw
            cur.execute("SELECT COUNT(*) FROM event_raw")
            count = cur.fetchone()[0]
            return count, None
    except Exception as e:
        print(f"Error getting query progress: {e}")
        return 0, 0

def setup_progress_tracking(conn):
    """Set up table for tracking query progress."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dataforseo_query_progress (
                    id SERIAL PRIMARY KEY,
                    city_id INTEGER NOT NULL,
                    query_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT
                )
            """)
            conn.commit()
            print("Progress tracking table set up")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error setting up progress tracking: {e}")

def create_dataforseo_tasks(cities, query_types, conn):
    """Create tasks for DataForSEO queries."""
    try:
        with conn.cursor() as cur:
            # Check if tasks already exist
            cur.execute("SELECT COUNT(*) FROM dataforseo_query_progress")
            if cur.fetchone()[0] > 0:
                print("Tasks already created, continuing with existing tasks")
                return
            
            # Create tasks for each city and query type
            for city in cities:
                for query_type in query_types:
                    cur.execute("""
                        INSERT INTO dataforseo_query_progress 
                        (city_id, query_type) 
                        VALUES (%s, %s)
                    """, (city['id'], query_type))
            
            conn.commit()
            print(f"Created {len(cities) * len(query_types)} query tasks")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating DataForSEO tasks: {e}")

def get_pending_tasks(conn, limit=10):
    """Get pending DataForSEO query tasks."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, city_id, query_type 
                FROM dataforseo_query_progress 
                WHERE status = 'pending' 
                ORDER BY id 
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error getting pending tasks: {e}")
        return []

def update_task_status(conn, task_id, status, error_message=None):
    """Update the status of a DataForSEO query task."""
    try:
        with conn.cursor() as cur:
            if status == 'completed':
                cur.execute("""
                    UPDATE dataforseo_query_progress 
                    SET status = %s, completed_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (status, task_id))
            elif status == 'error':
                cur.execute("""
                    UPDATE dataforseo_query_progress 
                    SET status = %s, error_message = %s 
                    WHERE id = %s
                """, (status, error_message, task_id))
            else:
                cur.execute("""
                    UPDATE dataforseo_query_progress 
                    SET status = %s 
                    WHERE id = %s
                """, (status, task_id))
            
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error updating task status: {e}")

def make_dataforseo_query(city, query_type):
    """
    Make a query to DataForSEO API.
    
    Args:
        city: Dict with city info
        query_type: Type of query (e.g., 'organic', 'events')
        
    Returns:
        API response or None on error
    """
    headers = get_dataforseo_client()
    endpoint = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    
    # Build the query based on type
    if query_type == 'events':
        keyword = f"events in {city['name']}"
    elif query_type == 'concerts':
        keyword = f"concerts in {city['name']}"
    elif query_type == 'exhibitions':
        keyword = f"exhibitions in {city['name']}"
    elif query_type == 'festivals':
        keyword = f"festivals in {city['name']}"
    elif query_type == 'shows':
        keyword = f"shows in {city['name']}"
    elif query_type == 'performances':
        keyword = f"performances in {city['name']}"
    elif query_type == 'workshops':
        keyword = f"workshops in {city['name']}"
    elif query_type == 'sporting_events':
        keyword = f"sporting events in {city['name']}"
    elif query_type == 'meetups':
        keyword = f"meetups in {city['name']}"
    else:
        keyword = f"{query_type} in {city['name']}"
    
    data = {
        "keyword": keyword,
        "location_name": city['name'],
        "language_name": "English",
        "device": "desktop",
        "os": "windows",
        "depth": 2  # Get more results
    }
    
    try:
        response = requests.post(endpoint, headers=headers, json=[data])
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error making DataForSEO query: {e}")
        return None

def process_dataforseo_response(response, city_id, query_type, conn):
    """
    Process response from DataForSEO and insert into event_raw.
    
    Args:
        response: API response
        city_id: ID of the city
        query_type: Type of query
        conn: Database connection
        
    Returns:
        Count of events inserted
    """
    if not response or 'tasks' not in response or not response['tasks']:
        return 0
    
    # Extract events from response
    events = []
    
    for task in response['tasks']:
        if task.get('status_code') != 20000:
            continue
            
        result = task.get('result', [])
        if not result:
            continue
            
        for item in result:
            items = item.get('items', [])
            for event_data in items:
                # Check if this is an event
                if 'event_element' in event_data:
                    event_info = event_data['event_element']
                    
                    # Transform to our raw_json format
                    raw_event = {
                        'name': event_info.get('title'),
                        'description': event_info.get('description', ''),
                        'date': event_info.get('date'),
                        'url': event_info.get('url'),
                        'location': {
                            'name': event_info.get('venue'),
                            'address': event_info.get('address')
                        },
                        'metro_id': city_id,
                        'source': 'dataforseo',
                        'query_type': query_type
                    }
                    
                    # Add thumbnail if available
                    if 'thumbnail' in event_info:
                        raw_event['image'] = event_info['thumbnail']
                    
                    events.append(raw_event)
    
    # Insert events into event_raw
    inserted_count = 0
    
    try:
        with conn.cursor() as cur:
            for event in events:
                # Check if event already exists (by URL)
                if event.get('url'):
                    cur.execute("""
                        SELECT id FROM event_raw 
                        WHERE raw_json->>'url' = %s
                    """, (event['url'],))
                    if cur.fetchone():
                        continue
                
                cur.execute("""
                    INSERT INTO event_raw 
                    (raw_json, metro_id, created_at) 
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                """, (Json(event), city_id))
                inserted_count += 1
            
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error inserting events: {e}")
    
    return inserted_count

def run_data_collection(args, conn):
    """Run the data collection stage of the pipeline."""
    print("=== Starting DataForSEO data collection ===")
    
    # Load cities
    cities = load_cities(conn)
    if not cities:
        print("No cities found, aborting data collection")
        return
        
    # Define query types
    query_types = [
        'events', 'concerts', 'exhibitions', 'festivals',
        'shows', 'performances', 'workshops', 'sporting_events', 'meetups'
    ]
    
    # Setup progress tracking
    setup_progress_tracking(conn)
    create_dataforseo_tasks(cities, query_types, conn)
    
    # Get progress
    completed, total = get_query_progress(conn)
    if total and completed == total:
        print(f"All {total} queries already completed")
        return
    elif total:
        print(f"Progress: {completed}/{total} queries completed")
    
    # Calculate batch size and delay
    batch_size = min(args.batch_size, 10)  # Don't make too many concurrent requests
    delay_between_batches = 10  # seconds
    
    # Process batches of tasks
    cities_dict = {city['id']: city for city in cities}
    
    while True:
        pending_tasks = get_pending_tasks(conn, batch_size)
        if not pending_tasks:
            break
            
        print(f"Processing batch of {len(pending_tasks)} tasks...")
        
        for task in pending_tasks:
            task_id, city_id, query_type = task
            
            if city_id not in cities_dict:
                update_task_status(conn, task_id, 'error', 'City not found')
                continue
                
            city = cities_dict[city_id]
            
            print(f"Querying for '{query_type}' in {city['name']}...")
            
            # Make API request
            response = make_dataforseo_query(city, query_type)
            
            if response:
                # Process response and insert events
                inserted = process_dataforseo_response(response, city_id, query_type, conn)
                print(f"Inserted {inserted} events from '{query_type}' in {city['name']}")
                update_task_status(conn, task_id, 'completed')
            else:
                update_task_status(conn, task_id, 'error', 'API request failed')
        
        # Check if we're done
        completed, total = get_query_progress(conn)
        if total:
            print(f"Progress: {completed}/{total} queries completed ({(completed/total)*100:.1f}%)")
            
            if completed == total:
                print("All queries completed")
                break
        
        # Delay between batches to avoid rate limiting
        print(f"Waiting {delay_between_batches} seconds before next batch...")
        time.sleep(delay_between_batches)
    
    print("=== DataForSEO data collection completed ===")

# Processing/Normalization Functions
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

def normalize_raw_event(raw_event):
    """
    Transform raw event data into JSON-LD format expected by the normalizer.
    
    Args:
        raw_event: Dict with raw event data
        
    Returns:
        Dict with normalized event data in JSON-LD format
    """
    if not raw_event or not isinstance(raw_event, dict):
        return None
    
    # Basic JSON-LD structure
    normalized = {
        "@context": "https://schema.org",
        "@type": "Event"
    }
    
    # Add basic fields
    if 'name' in raw_event:
        normalized['name'] = raw_event['name']
        
    if 'description' in raw_event:
        normalized['description'] = raw_event['description']
        
    if 'url' in raw_event:
        normalized['url'] = raw_event['url']
        
    if 'image' in raw_event:
        normalized['image'] = raw_event['image']
        
    # Handle date and time
    if 'date' in raw_event:
        date_str = raw_event['date']
        # Simple date parsing for the example
        # In a real implementation, use a robust date parser
        normalized['startDate'] = date_str
        
    # Handle location
    if 'location' in raw_event and isinstance(raw_event['location'], dict):
        location = raw_event['location']
        normalized['location'] = {
            "@type": "Place"
        }
        
        if 'name' in location:
            normalized['location']['name'] = location['name']
            
        if 'address' in location:
            normalized['location']['address'] = location['address']
    
    return normalized

def process_raw_events(conn, limit=100):
    """Process raw events and update them with normalized data."""
    pending = get_pending_normalizations(conn, limit)
    if not pending:
        print("No pending normalizations")
        return 0
        
    processed_count = 0
    for record in pending:
        record_id = record[0]
        raw_json = record[1]
        
        try:
            # Normalize the raw event
            normalized = normalize_raw_event(raw_json)
            
            if normalized:
                # Update the record with normalized data
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE event_raw
                        SET raw_json = %s::jsonb,
                            normalization_status = 'pending'
                        WHERE id = %s
                    """, (Json(normalized), record_id))
                    
                processed_count += 1
            else:
                # Mark as error
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE event_raw
                        SET normalization_status = 'error',
                            error_message = 'Failed to normalize data'
                        WHERE id = %s
                    """, (record_id,))
                    
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error processing record {record_id}: {e}")
            
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE event_raw
                        SET normalization_status = 'error',
                            error_message = %s
                        WHERE id = %s
                    """, (str(e)[:255], record_id))
                    conn.commit()
            except:
                conn.rollback()
    
    return processed_count

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
            
        return response_payload.get('extracted_data')
        
    except Exception as e:
        print(f"Error invoking Qwen 3 Lambda: {e}")
        return None

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

def process_pipeline(args, conn, lambda_client):
    """Run the event processing pipeline."""
    if not conn:
        print("Failed to connect to database, exiting.")
        return
        
    # Ensure venue cache table exists
    setup_venue_cache_table(conn)
    
    try:
        # Step 1: Process raw events into normalized format
        if not args.skip_normalize:
            print("\n=== Processing Raw Events ===")
            pending_records = get_pending_normalizations(conn, args.batch_size)
            if pending_records:
                print(f"Found {len(pending_records)} records needing normalization")
                processed = process_raw_events(conn, args.batch_size)
                print(f"Processed {processed} raw events")
                
                # Trigger normalizers to process the normalized data
                run_normalizers()
            else:
                print("No records pending normalization")
        
        # Step 2: Enrich event_clean records using Qwen 3
        if not args.skip_enrichment and lambda_client:
            print("\n=== Enriching Events with Qwen 3 ===")
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
            print("\n=== Fixing Geocoding Errors ===")
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

def run_unified_pipeline(args):
    """Run the unified event discovery and processing pipeline."""
    # Initialize database connection
    conn = initialize_db()
    if not conn:
        print("Failed to connect to database, exiting.")
        return
    
    # Initialize Lambda client for Qwen 3
    lambda_client = None
    if not args.skip_enrichment:
        lambda_client = initialize_lambda()
        if not lambda_client:
            print("Warning: Lambda client initialization failed. Skipping enrichment steps.")
            args.skip_enrichment = True
    
    try:
        # Stage 1: Data Collection
        if args.collect or args.full_run:
            run_data_collection(args, conn)
        
        # Stage 2: Data Processing
        if args.process or args.full_run:
            process_pipeline(args, conn, lambda_client)
        
        print("\n=== Unified pipeline completed ===")
    
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Event Discovery and Processing Pipeline")
    
    # Operation mode
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-run", action="store_true", help="Run the complete pipeline")
    group.add_argument("--collect", action="store_true", help="Only collect data from DataForSEO")
    group.add_argument("--process", action="store_true", help="Only process existing data")
    
    # Processing options
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records to process in each step")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip normalization step")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip Qwen enrichment step")
    parser.add_argument("--skip-geocoding", action="store_true", help="Skip geocoding fixes with Places API")
    
    args = parser.parse_args()
    run_unified_pipeline(args) 