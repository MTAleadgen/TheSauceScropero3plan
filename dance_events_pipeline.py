#!/usr/bin/env python3
"""
dance_events_pipeline.py

Specialized event discovery pipeline focusing on dance/dancing events:
1. Uses DataForSEO's events endpoint specifically (not organic search)
2. Makes 2 queries per city: "dance in [city]" and "dancing in [city]"
3. Configures to retrieve up to 100 results per query
4. Normalizes raw data into event_clean
5. Enriches events with AI for missing fields

Usage:
  python dance_events_pipeline.py --collect     # Only collect data from DataForSEO
  python dance_events_pipeline.py --process     # Only process existing data
  python dance_events_pipeline.py --full-run    # Run the complete pipeline
"""

import os
import sys
import json
import time
import datetime
import argparse
import subprocess

# Check for required dependencies
required_packages = [
    'requests',
    'psycopg2',
    'psycopg2-binary',
    'python-dotenv',
    'boto3'
]

def check_and_install_dependencies():
    """Check for required packages and install them if missing."""
    print("Checking for required dependencies...")
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
            print("Dependencies installed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install dependencies: {e}")
            sys.exit(1)
    else:
        print("All required dependencies are installed.")

# Check for GPU availability
def setup_gpu_if_available():
    """Check if GPU is available and set it up for TensorFlow/PyTorch if needed."""
    try:
        # For TensorFlow
        try:
            import tensorflow as tf
            gpus = tf.config.list_physical_devices('GPU')
            if gpus:
                print(f"TensorFlow detected {len(gpus)} GPU(s)")
                # Set memory growth to avoid allocation errors
                for gpu in gpus:
                    tf.config.experimental.set_memory_growth(gpu, True)
                return True
        except ImportError:
            pass
        
        # For PyTorch
        try:
            import torch
            if torch.cuda.is_available():
                print(f"PyTorch detected {torch.cuda.device_count()} GPU(s)")
                device = torch.device("cuda")
                return True
        except ImportError:
            pass
            
        print("No GPU detected or required ML libraries not installed")
        return False
    except Exception as e:
        print(f"Error checking for GPU: {e}")
        return False

# Main execution starts here
if __name__ == "__main__":
    # Check and install dependencies first
    check_and_install_dependencies()
    
    # Now import the dependencies
    import requests
    import psycopg2
    import random
    import base64
    from psycopg2.extras import DictCursor, Json
    from dotenv import load_dotenv
    from concurrent.futures import ThreadPoolExecutor
    import boto3

# Load environment variables
load_dotenv()

# DataForSEO API credentials
DATAFORSEO_API_LOGIN = os.environ.get('DATAFORSEO_LOGIN')
DATAFORSEO_API_PASSWORD = os.environ.get('DATAFORSEO_PASSWORD')
if not DATAFORSEO_API_LOGIN or not DATAFORSEO_API_PASSWORD:
    print("Error: DataForSEO API credentials not set in environment variables")
    print("Make sure your .env file contains DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD")
    sys.exit(1)

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

# Initialize AWS Lambda client for AI enrichment
def initialize_lambda():
    """Initialize AWS Lambda client for AI access."""
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

def load_cities(conn, limit=None):
    """
    Load cities from the database.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of cities to load (for testing)
        
    Returns:
        List of city dictionaries
    """
    try:
        with conn.cursor() as cur:
            # Try to get cities from cities table if it exists
            try:
                query = "SELECT id, name, country FROM cities ORDER BY id"
                if limit:
                    query += f" LIMIT {limit}"
                
                cur.execute(query)
                cities = cur.fetchall()
                if cities:
                    return [{'id': city[0], 'name': city[1], 'country': city[2]} for city in cities]
            except psycopg2.Error:
                # Table might not exist
                pass
                
            # Alternative: Query distinct metro_id values from event_clean
            try:
                query = "SELECT DISTINCT metro_id FROM event_clean"
                if limit:
                    query += f" LIMIT {limit}"
                
                cur.execute(query)
                metro_ids = [row[0] for row in cur.fetchall() if row[0]]
                if metro_ids:
                    return [{'id': metro_id, 'name': f'Metro {metro_id}'} for metro_id in metro_ids]
            except psycopg2.Error:
                # Table might not exist or have the column
                pass
        
        # If we can't get cities from the database, use a default list
        print("Warning: Using default test cities list. Replace with your actual city data.")
        default_cities = [
            {'id': 1, 'name': 'New York', 'country': 'US'},
            {'id': 2, 'name': 'Los Angeles', 'country': 'US'},
            {'id': 3, 'name': 'Chicago', 'country': 'US'},
            {'id': 4, 'name': 'London', 'country': 'UK'},
            {'id': 5, 'name': 'Paris', 'country': 'FR'},
        ]
        
        if limit:
            return default_cities[:limit]
        return default_cities
        
    except Exception as e:
        print(f"Error loading cities: {e}")
        return []

def setup_progress_tracking(conn):
    """Set up table for tracking query progress."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dance_query_progress (
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

def create_dance_tasks(cities, query_types, conn):
    """Create tasks for dance-related DataForSEO queries."""
    try:
        with conn.cursor() as cur:
            # Check if tasks already exist
            cur.execute("SELECT COUNT(*) FROM dance_query_progress")
            if cur.fetchone()[0] > 0:
                print("Tasks already created, continuing with existing tasks")
                return
            
            # Create tasks for each city and query type
            for city in cities:
                for query_type in query_types:
                    cur.execute("""
                        INSERT INTO dance_query_progress 
                        (city_id, query_type) 
                        VALUES (%s, %s)
                    """, (city['id'], query_type))
            
            conn.commit()
            print(f"Created {len(cities) * len(query_types)} query tasks")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating dance query tasks: {e}")

def get_query_progress(conn):
    """Get the current progress of dance-related DataForSEO queries."""
    try:
        with conn.cursor() as cur:
            # Check progress from tracking table
            try:
                cur.execute("""
                    SELECT COUNT(*) FROM dance_query_progress 
                    WHERE status = 'completed'
                """)
                completed = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM dance_query_progress")
                total = cur.fetchone()[0]
                
                return completed, total
            except psycopg2.Error:
                # Table might not exist
                pass
                
            # Alternative: Count records in event_raw for dance/dancing queries
            cur.execute("""
                SELECT COUNT(*) FROM event_raw 
                WHERE raw_json->>'query_type' IN ('dance', 'dancing')
            """)
            count = cur.fetchone()[0]
            return count, None
    except Exception as e:
        print(f"Error getting query progress: {e}")
        return 0, 0

def get_pending_tasks(conn, limit=10):
    """Get pending dance-related DataForSEO query tasks."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, city_id, query_type 
                FROM dance_query_progress 
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
                    UPDATE dance_query_progress 
                    SET status = %s, completed_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (status, task_id))
            elif status == 'error':
                cur.execute("""
                    UPDATE dance_query_progress 
                    SET status = %s, error_message = %s 
                    WHERE id = %s
                """, (status, error_message, task_id))
            else:
                cur.execute("""
                    UPDATE dance_query_progress 
                    SET status = %s 
                    WHERE id = %s
                """, (status, task_id))
            
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error updating task status: {e}")

def make_dataforseo_events_query(city, query_type):
    """
    Make a query to DataForSEO Events API.
    
    Args:
        city: Dict with city info
        query_type: Either 'dance' or 'dancing'
        
    Returns:
        API response or None on error
    """
    headers = get_dataforseo_client()
    # Use the events endpoint instead of organic search
    endpoint = "https://api.dataforseo.com/v3/serp/google/events/live/advanced"
    
    # Build the query based on type
    if query_type == 'dance':
        keyword = f"dance in {city['name']}"
    else:  # dancing
        keyword = f"dancing in {city['name']}"
    
    # Get location code based on city
    location_code = get_location_code_for_city(city['name'])
    
    # Configure the request for max results
    data = {
        "keyword": keyword,
        "location_code": location_code,
        "language_code": "en",
        "device": "desktop",
        "os": "windows",
        "depth": 1,  # Retrieve 100 events (100 events per page)
        "date_range": "next_week",  # Focus on upcoming events
        "priority": 1  # Higher priority for faster processing (1 = highest)
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

def get_location_code_for_city(city_name):
    """Get the DataForSEO location code for a city."""
    # Default to US (2840) if we don't have a specific code
    default_code = 2840
    
    # Map common city names to their DataForSEO location codes
    location_codes = {
        "new york": 1023191,
        "los angeles": 1013962,
        "chicago": 1016367,
        "london": 1006886,
        "paris": 1006094
    }
    
    return location_codes.get(city_name.lower(), default_code)

def process_dataforseo_events_response(response, city_id, query_type, conn):
    """
    Process response from DataForSEO events endpoint and insert into event_raw.
    
    Args:
        response: API response
        city_id: ID of the city
        query_type: Type of query ('dance' or 'dancing')
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
                # Transform to our raw_json format
                raw_event = {
                    'name': event_data.get('title'),
                    'description': event_data.get('description', ''),
                    'date': event_data.get('date'),
                    'url': event_data.get('url'),
                    'location': {
                        'name': event_data.get('venue'),
                        'address': event_data.get('address')
                    },
                    'metro_id': city_id,
                    'source': 'dataforseo_events',
                    'query_type': query_type
                }
                
                # Add thumbnail if available
                if 'thumbnail' in event_data:
                    raw_event['image'] = event_data['thumbnail']
                
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

def run_dance_data_collection(args, conn):
    """Run the dance-specific data collection using the events endpoint."""
    print("=== Starting Dance Events data collection ===")
    
    # Load cities (with optional limit for testing)
    cities = load_cities(conn, args.city_limit)
    if not cities:
        print("No cities found, aborting data collection")
        return
        
    # Define query types - just dance and dancing
    query_types = ['dance', 'dancing']
    
    # Setup progress tracking
    setup_progress_tracking(conn)
    create_dance_tasks(cities, query_types, conn)
    
    # Get progress
    completed, total = get_query_progress(conn)
    if total and completed == total:
        print(f"All {total} queries already completed")
        return
    elif total:
        print(f"Progress: {completed}/{total} queries completed")
    
    # Calculate batch size and delay
    batch_size = min(args.batch_size, 10)  # Limit concurrent requests
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
            
            # Make API request using events endpoint
            response = make_dataforseo_events_query(city, query_type)
            
            if response:
                # Process response and insert events
                inserted = process_dataforseo_events_response(response, city_id, query_type, conn)
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
    
    print("=== Dance Events data collection completed ===")

# Processing/Normalization Functions - these can be largely reused from the unified pipeline
def get_pending_normalizations(conn, limit=100):
    """Get dance-related records from event_raw that need normalization."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, raw_json 
                FROM event_raw 
                WHERE normalization_status IS NULL
                AND raw_json->>'query_type' IN ('dance', 'dancing')
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
    """Process raw dance events and update them with normalized data."""
    pending = get_pending_normalizations(conn, limit)
    if not pending:
        print("No pending dance event normalizations")
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

def invoke_ai_enrichment(lambda_client, event_data):
    """
    Invoke AI via Lambda to extract missing fields from event description.
    
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
        # Get the event description
        description = event_data['json_data'].get('description', '')
        if not description:
            print(f"No description available for event ID {event_data['id']}")
            return None
        
        # Prepare the specialized prompt for dance events
        prompt = f"""Extract structured data from the event description below. Return only a valid JSON object with these exact fields:

- start_time (ISO 8601 format if available)
- end_time (ISO 8601 format if available)
- venue
- address
- price (e.g. "$10", "Free", etc.)
- live_band (true or false)
- class_before (true or false)

Event description:
\"\"\"
{description}
\"\"\"
Respond only with the JSON object. Do not include any commentary or formatting."""
        
        # Prepare payload for Lambda function
        payload = {
            "prompt": prompt,
            "model": "anthropic.claude-3-haiku-20240307",  # Or your preferred model
            "max_tokens": 1000,
            "temperature": 0.1  # Low temperature for more deterministic response
        }
        
        # Invoke Lambda function
        lambda_function_name = os.environ.get('AI_LAMBDA_FUNCTION', 'event-enrichment-ai')
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
        
        # Extract and parse the AI response
        ai_response = response_payload.get('body', '')
        try:
            # Extract JSON from response (in case there's any extra text)
            import re
            json_match = re.search(r'(\{.*\})', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                extracted_data = json.loads(json_str)
            else:
                extracted_data = json.loads(ai_response)
            
            return extracted_data
        except json.JSONDecodeError as e:
            print(f"Failed to parse AI response: {e}")
            print(f"Response was: {ai_response}")
            return None
        
    except Exception as e:
        print(f"Error invoking AI Lambda: {e}")
        return None

def get_events_needing_enrichment(conn, limit=50):
    """
    Get dance-related records from event_clean that need AI enrichment.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ec.id, ec.json_data 
                FROM event_clean ec
                JOIN event_raw er ON ec.raw_event_id = er.id
                WHERE er.raw_json->>'query_type' IN ('dance', 'dancing')
                AND ec.json_data->>'description' IS NOT NULL 
                AND ec.json_data->>'description' != '' 
                AND (
                    ec.json_data->>'price' IS NULL OR
                    ec.json_data->>'eventAttendanceMode' IS NULL OR
                    ec.json_data->>'eventStatus' IS NULL OR
                    ec.json_data->'location'->>'address' IS NULL OR
                    ec.json_data->'organizer'->>'name' IS NULL OR
                    ec.json_data->>'dance_style' IS NULL
                )
                AND ec.enrichment_status IS NULL
                ORDER BY ec.id 
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error fetching events needing enrichment: {e}")
        return []

def update_event_with_enrichment(conn, event_id, enrichment_data):
    """Update event_clean record with enrichment data from AI."""
    if not enrichment_data:
        return False
        
    try:
        with conn.cursor() as cur:
            # First, check if an entry already exists in event_clean
            cur.execute("""
                SELECT * FROM event_clean WHERE id = %s
            """, (event_id,))
            existing_record = cur.fetchone()
            
            if not existing_record:
                # Get the raw event data to create a new record
                cur.execute("""
                    SELECT id, raw_json, metro_id FROM event_raw 
                    WHERE id = (
                        SELECT event_raw_id FROM event_clean WHERE id = %s
                    )
                """, (event_id,))
                raw_record = cur.fetchone()
                if not raw_record:
                    return False
                
                raw_id, raw_json, metro_id = raw_record
                
                # Map the enriched data to event_clean fields
                updates = {}
                
                # Only update fields that don't already have data
                
                # Handle start and end times
                if 'start_time' in enrichment_data and enrichment_data['start_time'] and not existing_record['start_ts']:
                    try:
                        # Attempt to parse the ISO 8601 date
                        updates['start_ts'] = enrichment_data['start_time']
                    except:
                        pass
                
                if 'end_time' in enrichment_data and enrichment_data['end_time'] and not existing_record['end_ts']:
                    try:
                        # Attempt to parse the ISO 8601 date
                        updates['end_ts'] = enrichment_data['end_time']
                    except:
                        pass
                
                # Handle venue and address
                if 'venue' in enrichment_data and enrichment_data['venue'] and not existing_record['venue_name']:
                    updates['venue_name'] = enrichment_data['venue']
                    
                if 'address' in enrichment_data and enrichment_data['address'] and not existing_record['venue_address']:
                    updates['venue_address'] = enrichment_data['address']
                
                # Handle price
                if 'price' in enrichment_data and enrichment_data['price'] and not existing_record['price_val']:
                    price_str = enrichment_data['price']
                    
                    # Extract numeric value and currency
                    import re
                    price_match = re.search(r'(\$|€|£)?(\d+(?:\.\d+)?)', price_str)
                    if price_match:
                        currency = price_match.group(1) or '$'
                        amount = float(price_match.group(2))
                        
                        # Map currency symbols to 3-letter codes
                        currency_map = {
                            '$': 'USD',
                            '€': 'EUR',
                            '£': 'GBP'
                        }
                        
                        updates['price_val'] = amount
                        updates['price_ccy'] = currency_map.get(currency, 'USD')
                
                # Handle dance-specific fields (store in tags as JSON)
                tags = {}
                if 'live_band' in enrichment_data:
                    tags['live_band'] = enrichment_data['live_band']
                    
                if 'class_before' in enrichment_data:
                    tags['class_before'] = enrichment_data['class_before']
                
                if tags and (not existing_record['tags'] or existing_record['tags'] == '{}'):
                    updates['tags'] = Json(tags)
                elif tags and existing_record['tags']:
                    # Merge with existing tags
                    existing_tags = existing_record['tags']
                    for key, value in tags.items():
                        if key not in existing_tags:
                            existing_tags[key] = value
                    updates['tags'] = Json(existing_tags)
                
                # Only update if we have changes to make
                if updates:
                    # Build the SQL query dynamically based on what needs updating
                    fields = []
                    values = []
                    
                    for field, value in updates.items():
                        fields.append(f"{field} = %s")
                        values.append(value)
                    
                    # Add the ID to the values list for the WHERE clause
                    values.append(event_id)
                    
                    # Execute the update
                    update_query = f"""
                        UPDATE event_clean
                        SET {", ".join(fields)}
                        WHERE id = %s
                    """
                    
                    cur.execute(update_query, values)
                    conn.commit()
                    return True
                else:
                    print(f"No fields to update for event {event_id}")
                    return False
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error updating event with enrichment: {e}")
        return False

def run_normalizers():
    """Run the normalization worker to process event_raw records into event_clean."""
    # In a real implementation, this might start a worker process
    # or send a message to a queue to trigger normalization
    print("Running normalizers to process raw dance events...")
    # Simulate processing time
    time.sleep(2)
    print("Normalization workers triggered")

def process_pipeline(args, conn, lambda_client):
    """Run the dance event processing pipeline."""
    if not conn:
        print("Failed to connect to database, exiting.")
        return
        
    try:
        # Step 1: Process raw events into normalized format
        if not args.skip_normalize:
            print("\n=== Processing Raw Dance Events ===")
            pending_records = get_pending_normalizations(conn, args.batch_size)
            if pending_records:
                print(f"Found {len(pending_records)} dance records needing normalization")
                processed = process_raw_events(conn, args.batch_size)
                print(f"Processed {processed} raw dance events")
                
                # Trigger normalizers to process the normalized data
                run_normalizers()
            else:
                print("No dance records pending normalization")
        
        # Step 2: Enrich event_clean records using AI
        if not args.skip_enrichment and lambda_client:
            print("\n=== Enriching Dance Events with AI ===")
            enrichment_records = get_events_needing_enrichment(conn, args.batch_size)
            if enrichment_records:
                print(f"Found {len(enrichment_records)} dance records needing enrichment")
                enriched_count = 0
                
                for record in enrichment_records:
                    event_id = record[0]
                    event_data = {
                        'id': event_id,
                        'json_data': record[1]
                    }
                    
                    print(f"Enriching dance event ID {event_id}...")
                    enrichment_data = invoke_ai_enrichment(lambda_client, event_data)
                    
                    if enrichment_data:
                        success = update_event_with_enrichment(conn, event_id, enrichment_data)
                        if success:
                            enriched_count += 1
                            print(f"  Successfully enriched dance event {event_id}")
                        else:
                            print(f"  Failed to update dance event {event_id} with enrichment data")
                    else:
                        print(f"  Failed to get enrichment data for dance event {event_id}")
                        
                        # Mark as processed even if we couldn't enrich it
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE event_clean
                                SET enrichment_status = 'failed',
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (event_id,))
                            conn.commit()
                
                print(f"Enriched {enriched_count} out of {len(enrichment_records)} dance records")
            else:
                print("No dance records needing enrichment")
                
        print("\nDance pipeline processing complete")
    
    except Exception as e:
        print(f"Error in dance pipeline processing: {e}")

def run_dance_pipeline(args):
    """Run the unified dance event discovery and processing pipeline."""
    # Initialize database connection
    conn = initialize_db()
    if not conn:
        print("Failed to connect to database, exiting.")
        return
    
    # Initialize Lambda client for AI enrichment
    lambda_client = None
    if not args.skip_enrichment:
        lambda_client = initialize_lambda()
        if not lambda_client:
            print("Warning: Lambda client initialization failed. Skipping enrichment steps.")
            args.skip_enrichment = True
    
    try:
        # Stage 1: Data Collection
        if args.collect or args.full_run:
            run_dance_data_collection(args, conn)
        
        # Stage 2: Data Processing
        if args.process or args.full_run:
            process_pipeline(args, conn, lambda_client)
        
        print("\n=== Dance events pipeline completed ===")
    
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dance Events Discovery and Processing Pipeline")
    
    # Operation mode
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-run", action="store_true", help="Run the complete pipeline")
    group.add_argument("--collect", action="store_true", help="Only collect data from DataForSEO")
    group.add_argument("--process", action="store_true", help="Only process existing data")
    
    # Processing options
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records to process in each step")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip normalization step")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip AI enrichment step")
    parser.add_argument("--city-limit", type=int, help="Limit number of cities (for testing)")
    
    args = parser.parse_args()
    run_dance_pipeline(args) 