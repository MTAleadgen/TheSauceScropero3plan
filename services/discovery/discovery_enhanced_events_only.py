import os
import redis
import requests
import pandas as pd
import time
import logging
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import csv
from datetime import datetime, timezone, timedelta
import base64
import prometheus_client
from prometheus_client import Counter, Gauge, Histogram
import psycopg2
from psycopg2.extras import Json
import hashlib
import re
import sys  # Added for command line arguments

# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Setup file logging for discovered URLs
url_file_logger = logging.getLogger('discovered_urls_file_logger')
url_file_logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("discovered_urls.txt", mode='w')
formatter = logging.Formatter('%(message)s')
file_handler.setFormatter(formatter)
url_file_logger.addHandler(file_handler)
url_file_logger.propagate = False

# Setup file logging for dance style statistics
dance_stats_logger = logging.getLogger('dance_stats_logger')
dance_stats_logger.setLevel(logging.INFO)
stats_file_handler = logging.FileHandler("dance_style_stats.csv", mode='w')
stats_formatter = logging.Formatter('%(message)s')
stats_file_handler.setFormatter(stats_formatter)
dance_stats_logger.addHandler(stats_file_handler)
dance_stats_logger.propagate = False
# Write CSV header for dance style stats
dance_stats_logger.info("city,dance_style,url_count")

# --- Configuration ---
load_dotenv()

# Global dictionary to store task metadata
task_metadata_map = {}

# DataForSEO credentials
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD")
# Default to the service name when running in Docker via docker-compose
REDIS_URL = os.getenv("REDIS_URL", "redis://redis_queue:6379/0")
REDIS_URL_QUEUE = "url_queue"
REDIS_SEEN_URLS_SET = "seen_urls_discovery"
# Use local path if file doesn't exist at Docker path
DOCKER_METRO_CSV_PATH = "/app/geonames_na_sa_eu_top1785.csv"
LOCAL_METRO_CSV_PATH = "./geonames_na_sa_eu_top1785.csv"
METRO_CSV_PATH = DOCKER_METRO_CSV_PATH if os.path.exists(DOCKER_METRO_CSV_PATH) else LOCAL_METRO_CSV_PATH
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", 2.0))
RESULTS_PER_PAGE = 100
MAX_PAGES_PER_QUERY = int(os.getenv("MAX_PAGES_PER_QUERY", 1))
MAX_CITIES = int(os.getenv("MAX_CITIES", 1))  # Default to 1 (one city) for cities
DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data_raw")

# Ensure data_raw directory exists
Path(DATA_RAW_DIR).mkdir(parents=True, exist_ok=True)
Path(os.path.join(DATA_RAW_DIR, "dance_queries_enhanced")).mkdir(parents=True, exist_ok=True)

# Setup Prometheus metrics
# Create a registry
registry = prometheus_client.CollectorRegistry()

# Define metrics
serp_requests_total = Counter('serp_requests_total', 'Total number of SERP API requests', ['city', 'dance_style'], registry=registry)
serp_request_errors = Counter('serp_request_errors', 'Number of SERP API request errors', ['city', 'dance_style'], registry=registry)
urls_discovered = Counter('urls_discovered', 'Number of URLs discovered', ['city', 'dance_style'], registry=registry)
unique_urls_added = Counter('unique_urls_added', 'Number of unique URLs added to queue', ['city'], registry=registry)
api_latency = Histogram('api_latency_seconds', 'API request latency in seconds', ['city', 'dance_style'], registry=registry)
dance_style_url_count = Gauge('dance_style_url_count', 'Number of URLs found for each dance style', ['city', 'dance_style'], registry=registry)

# Updated list of 14 dance styles
TARGET_DANCE_STYLES_FOR_DISCOVERY = [
    "salsa",
    "bachata",
    "kizomba",
    "zouk",
    "cumbia",
    "rumba",
    "tango",
    "hustle",
    "chacha",
    "coast swing",
    "lambada",
    "samba",
    "ballroom",
    "forro"
]

# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL not set in environment. Exiting.")
    exit()
# --- END DATABASE SETUP ---

# --- Helper Functions ---
def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ["http", "https"]
    except ValueError:
        return False

def extract_urls_from_item(item, source_type="unknown"):
    """Recursively extract all URLs from a result item"""
    found_urls = []
    
    # Base case: item is None or not a dict
    if item is None or not isinstance(item, dict):
        return found_urls
    
    # Get direct URL if available
    if 'url' in item and item['url']:
        url = item['url']
        if url.startswith('http'):
            found_urls.append(url)
    
    # Get domain URL if available
    if 'domain' in item and item['domain']:
        domain = item['domain']
        domain_url = f"https://{domain}" if not domain.startswith('http') else domain
        found_urls.append(domain_url)
    
    # Look for links
    if 'links' in item and isinstance(item['links'], list):
        for link in item['links']:
            if isinstance(link, dict) and 'url' in link and link['url']:
                url = link['url']
                if url.startswith('http'):
                    found_urls.append(url)
    
    # Look for items array
    if 'items' in item and isinstance(item['items'], list):
        for sub_item in item['items']:
            sub_results = extract_urls_from_item(sub_item, f"{source_type}_item")
            found_urls.extend(sub_results)
    
    # Check all other dictionaries recursively (but avoid infinite recursion)
    for key, value in item.items():
        if key not in ['url', 'domain', 'links', 'items'] and isinstance(value, dict):
            sub_results = extract_urls_from_item(value, f"{key}")
            found_urls.extend(sub_results)
        elif key not in ['url', 'domain', 'links', 'items'] and isinstance(value, list):
            for sub_item in value:
                if isinstance(sub_item, dict):
                    sub_results = extract_urls_from_item(sub_item, f"{key}_item")
                    found_urls.extend(sub_results)
    
    return found_urls

def extract_urls_from_result(result):
    """Extract all URLs from an API result with recursive approach"""
    urls = []
    
    if result.get('status_code') == 20000:  # Success
        tasks = result.get('tasks', [])
        
        for task in tasks:
            if 'result' in task and task['result']:
                for item in task['result']:
                    # Start with standard items
                    items = item.get('items', [])
                    
                    # Process all item types
                    for result_item in items:
                        item_type = result_item.get('type', 'unknown')
                        # Extract all URLs from this item through recursive function
                        extracted_urls = extract_urls_from_item(result_item, item_type)
                        
                        for url in extracted_urls:
                            if url not in urls and is_valid_url(url):
                                urls.append(url)
                    
                    # Also check if there are direct URLs in the result (not in items)
                    direct_extracted = extract_urls_from_item(item, "direct_result")
                    for url in direct_extracted:
                        if url not in urls and is_valid_url(url):
                            urls.append(url)
    
    return urls

def get_dataforseo_results_for_dance_style(city_info, dance_style, dataforseo_login, dataforseo_password, redis_client, db_conn, redis_available):
    """Fetches search results from DataForSEO API for a specific dance style."""
    all_urls_from_query = set()
    raw_api_response_json = None
    
    # Format the search query
    query = f"{dance_style} in {city_info['name']}"
    # Handle coast swing specially
    if dance_style == "coast swing":
        query = f'"coast swing" in {city_info["name"]}'
    
    # DataForSEO API endpoint for Google Events Search (updated from organic)
    endpoint = "https://api.dataforseo.com/v3/serp/google/events/live/advanced"
    
    # Create auth header
    auth_header = base64.b64encode(f"{dataforseo_login}:{dataforseo_password}".encode()).decode()
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/json'
    }
    
    # Create the search task with Events-specific parameters
    data = [{
        "keyword": query,
        "location_code": int(city_info['location_code']) if pd.notna(city_info.get('location_code')) else None,
        "language_name": "English",
        "depth": RESULTS_PER_PAGE,  # Request 100 results
        "results_on_page": 100, # Specify 100 results per page structure
        "se_domain": "google.com"
    }]
    
    # If location_code is not available, fall back to location_name
    if data[0]["location_code"] is None:
        logger.warning(f"No location_code for {city_info['name']}, falling back to location_name instead")
        data[0].pop("location_code", None)
        data[0]["location_name"] = city_info['name']
    
    try:
        # Track metrics
        with api_latency.labels(city=city_info['name'], dance_style=dance_style).time():
            serp_requests_total.labels(city=city_info['name'], dance_style=dance_style).inc()
            
            # Make the API request
            logger.info(f"Querying: '{query}' in {city_info['name']} using events endpoint")
            response = requests.post(endpoint, json=data, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            # Store the raw API response if successful
            if result.get('status_code') == 20000:
                raw_api_response_json = result 
            else: # Log API error if not 20000
                logger.error(f"DataForSEO API error for \'{query}\' in {city_info['name']}. Status: {result.get('status_code')}, Message: {result.get('status_message')}")
                serp_request_errors.labels(city=city_info['name'], dance_style=dance_style).inc()
                return all_urls_from_query, raw_api_response_json

            # Process the results using the enhanced extraction
            if result.get('status_code') == 20000:  # Success code
                logger.info(f"DataForSEO request successful for '{query}' in {city_info['name']}")
                
                # Extract URLs with the enhanced method
                urls = extract_urls_from_result(result)
                
                # Add all discovered URLs to the set
                for url in urls:
                    if url not in all_urls_from_query and is_valid_url(url):
                        all_urls_from_query.add(url)
                        urls_discovered.labels(city=city_info['name'], dance_style=dance_style).inc()
                
                # Record metrics for this dance style
                dance_style_url_count.labels(city=city_info['name'], dance_style=dance_style).set(len(all_urls_from_query))
                # Log per-style count to CSV logger
                dance_stats_logger.info(f"{city_info['name']},{dance_style},{len(all_urls_from_query)}")
            else:
                logger.error(f"DataForSEO API request failed for \'{query}\' in {city_info['name']}. Status: {response.status_code}")
                serp_request_errors.labels(city=city_info['name'], dance_style=dance_style).inc()
                # Return empty set and None if HTTP error before JSON parsing
                return all_urls_from_query, None

            # Store the raw API response in event_raw
            if db_conn and raw_api_response_json:
                try:
                    # Determine metro_id (geonameid)
                    current_metro_id = city_info.get('geonameid')
                    if current_metro_id is None:
                        logger.warning(f"Missing 'geonameid' for city: {city_info.get('name', 'Unknown City')}. Skipping DB insert for raw events for {dance_style}.")
                    else:
                        current_metro_id = int(current_metro_id) # Ensure it's an int
                        # Construct a unique source_event_id for this SERP query
                        serp_query_identifier = f"{current_metro_id}_{dance_style}_events_live_{int(time.time())}"
                        source_event_id_for_serp = hashlib.sha1(serp_query_identifier.encode()).hexdigest()[:20]

                        # Insert into event_raw table directly
                        script_meta_for_db = {
                            "city_name_context": city_info.get('name', 'Unknown'),
                            "dance_style_context": dance_style,
                            "search_type": "events_live_advanced"
                        }
                        
                        insert_into_event_raw(
                            conn=db_conn,
                            source='dataforseo_events_raw',
                            source_event_id=source_event_id_for_serp, 
                            metro_id=current_metro_id,
                            raw_data_payload=result,
                            script_metadata=script_meta_for_db
                        )
                except Exception as e_db:
                    logger.error(f"Error during raw DataForSEO response insertion for {city_info.get('name', 'Unknown')} / {dance_style}: {e_db}")

            # Add to Redis queue if available and URLs found
            if redis_client and redis_available and all_urls_from_query:
                current_metro_id = city_info.get('geonameid')
                if current_metro_id is not None:
                    current_metro_id = int(current_metro_id) # Ensure it's an int
                    for found_url in all_urls_from_query:
                        if not redis_client.sismember(REDIS_SEEN_URLS_SET, found_url):
                            # Create a package with URL and context
                            url_package = {
                                "url": found_url,
                                "metro_id": current_metro_id,
                                "dance_style_context": dance_style
                            }
                            redis_client.rpush(REDIS_URL_QUEUE, json.dumps(url_package))
                            redis_client.sadd(REDIS_SEEN_URLS_SET, found_url)
                else:
                    logger.warning(f"Cannot add URLs to queue for {dance_style} in {city_info.get('name', 'Unknown')} due to missing 'geonameid'.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception for \'{query}\' in {city_info['name']}: {e}")
        serp_request_errors.labels(city=city_info['name'], dance_style=dance_style).inc()
        return all_urls_from_query, None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for \'{query}\' in {city_info['name']}: {e}. Response text: {response.text if 'response' in locals() else 'Response object not available'}")
        serp_request_errors.labels(city=city_info['name'], dance_style=dance_style).inc()
        return all_urls_from_query, None
    
    return all_urls_from_query, raw_api_response_json

def load_metros_from_csv(csv_path):
    """Load metros from CSV file, or create a test file if none exists"""
    metros = []
    default_slug_column = "slug" # The column name we expect for slugs

    # Check if file exists
    if not os.path.exists(csv_path):
        logger.warning(f"Metro CSV file not found at {csv_path}. Creating a test file with sample cities.")
        # Create a test file with sample cities
        test_metros_data = [
            {"geonameid": 2643743, "name": "London", "asciiname": "London", "country_code": "GB", "population": 8908081, "timezone": "Europe/London", "latitude": 51.5073219, "longitude": -0.1276474, "slug": "london", "admin1_code": "ENG"},
            {"geonameid": 5128581, "name": "New York City", "asciiname": "New York City", "country_code": "US", "population": 8804190, "timezone": "America/New_York", "latitude": 40.7127281, "longitude": -74.0060152, "slug": "new_york_city", "admin1_code": "NY"},
            {"geonameid": 2950159, "name": "Berlin", "asciiname": "Berlin", "country_code": "DE", "population": 3644826, "timezone": "Europe/Berlin", "latitude": 52.5200066, "longitude": 13.404954, "slug": "berlin", "admin1_code": "16"},
            {"geonameid": 2988507, "name": "Paris", "asciiname": "Paris", "country_code": "FR", "population": 2140526, "timezone": "Europe/Paris", "latitude": 48.8566969, "longitude": 2.3514616, "slug": "paris", "admin1_code": "11"},
            {"geonameid": 3173435, "name": "Rome", "asciiname": "Rome", "country_code": "IT", "population": 2872800, "timezone": "Europe/Rome", "latitude": 41.8905203, "longitude": 12.4942486, "slug": "rome", "admin1_code": "07"},
        ]
        # Create a more comprehensive test DataFrame
        test_df = pd.DataFrame(test_metros_data)
        # Use a different path for the generated test file to avoid overwriting a user's file if it was just misplaced
        # test_csv_path = "./test_metros_generated.csv" 
        # For simplicity, let's assume if it's not found, we use this in-memory df
        # test_df.to_csv(test_csv_path, index=False)
        # logger.info(f"Created test file with {len(test_metros_data)} sample cities at {test_csv_path}")
        logger.info(f"Using in-memory DataFrame with {len(test_metros_data)} sample cities as {csv_path} was not found.")
        # Ensure slug is created if somehow missing in test data
        if default_slug_column not in test_df.columns and 'name' in test_df.columns:
            test_df[default_slug_column] = test_df['name'].apply(lambda x: str(x).lower().replace(" ", "_").replace("-", "_"))
        return test_df
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"Loaded {len(df)} rows from {csv_path}")

        # Ensure 'slug' column exists, create it if not
        if default_slug_column not in df.columns:
            if 'name' in df.columns:
                logger.warning(f"'{default_slug_column}' column not found in {csv_path}. Creating from 'name' column.")
                df[default_slug_column] = df['name'].apply(lambda x: str(x).lower().replace(" ", "_").replace("-", "_"))
            elif 'asciiname' in df.columns: # Fallback to asciiname for slug creation
                logger.warning(f"'{default_slug_column}' and 'name' columns not found. Creating from 'asciiname' column.")
                df[default_slug_column] = df['asciiname'].apply(lambda x: str(x).lower().replace(" ", "_").replace("-", "_"))
            else:
                logger.error(f"Cannot create '{default_slug_column}' as neither 'name' nor 'asciiname' columns are present in {csv_path}.")
                # Return an empty DataFrame or raise an error if slug is absolutely critical
                return pd.DataFrame()
        
        # Ensure other critical columns have fallbacks or are handled if missing
        if 'asciiname' not in df.columns:
            if 'name' in df.columns:
                logger.warning("'asciiname' column not found. Using 'name' column as fallback for API calls.")
                df['asciiname'] = df['name'] # Use name if asciiname is missing
            else:
                logger.error("Neither 'asciiname' nor 'name' columns found. API calls for city names will likely fail.")
                return pd.DataFrame() # Or handle error appropriately
        
        if 'country_code' not in df.columns:
            logger.warning("'country_code' column not found. Location specificity for API calls might be reduced.")
            df['country_code'] = "" # Add empty string if missing

        if 'admin1_code' not in df.columns:
            logger.warning("'admin1_code' column not found. US state-level specificity for API calls will be unavailable.")
            df['admin1_code'] = "" # Add empty string if missing

        # Ensure 'language_code' column exists, default to 'en' if not
        if 'language_code' not in df.columns:
            logger.warning("'language_code' column not found in CSV. Defaulting to 'en' (English) for all metros. Add this column for better local language targeting.")
            df['language_code'] = 'en'
        else:
            # Fill any missing language_code values with 'en' as a fallback
            df['language_code'] = df['language_code'].fillna('en')
            logger.info("'language_code' column loaded. Ensure it contains valid 2-letter ISO language codes (e.g., 'es', 'fr').")

        # Filter by population if the column exists, otherwise log warning and proceed
        if 'population' in df.columns:
            # Attempt to convert population to numeric, coercing errors to NaN
            df['population'] = pd.to_numeric(df['population'], errors='coerce')
            df = df[df['population'] > 10000]
            logger.info(f"{len(df)} metros remaining after population filter (> 10000).")
        else:
            logger.warning("'population' column not found. Cannot filter by population.")
            
        return df

    except FileNotFoundError: # This case should be caught by os.path.exists now, but good to keep
        logger.error(f"Metro CSV file not found at {csv_path}. This shouldn't happen if os.path.exists check passed.")
        return load_metros_from_csv(None) # Recurse to create test data
    except Exception as e:
        logger.error(f"Error loading or processing CSV from {csv_path}: {e}. Attempting to use test data.")
        return load_metros_from_csv(None) # Recurse to create test data
    
# --- Database Helper Functions ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Database connection established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def create_event_raw_table_if_not_exists(db_conn):
    """Creates the event_raw table IF IT DOES NOT EXIST, matching the provided schema.
       It is generally better to manage schema with migrations, but this is a safeguard.
    """
    if not db_conn:
        logger.error("No database connection available for table creation check.")
        return False
    
    # Schema based on user-provided \d event_raw output
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS event_raw (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        source_event_id TEXT, -- Can be NULL, but part of a UNIQUE constraint with source
        metro_id INTEGER,
        raw_json JSONB NOT NULL,
        discovered_at TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        parsed_at TIMESTAMP(3) WITHOUT TIME ZONE,
        normalized_at TIMESTAMP(3) WITHOUT TIME ZONE,
        normalization_status TEXT,
        event_id UUID,
        CONSTRAINT event_raw_event_id_unique UNIQUE (event_id),
        CONSTRAINT source_event_idx UNIQUE (source, source_event_id),
        CONSTRAINT event_raw_metro_id_metro_geonameid_fk FOREIGN KEY (metro_id) REFERENCES metro(geonameid)
    );
    """
    # Note: Sequence for ID (event_raw_id_seq) is implicitly handled by SERIAL.
    # We are not creating indexes explicitly here if CREATE TABLE IF NOT EXISTS is used,
    # as it won't add them if the table exists. Assumes they are managed by migrations if table exists.
    try:
        with db_conn.cursor() as cur:
            cur.execute(create_table_sql)
            db_conn.commit()
            logger.info("Ensured 'event_raw' table exists (or was created to match specified schema if missing).")
        return True
    except psycopg2.Error as e:
        logger.error(f"Error creating/checking 'event_raw' table: {e}")
        db_conn.rollback()
        return False
    except Exception as e_gen:
        logger.error(f"Generic error during table creation/check for 'event_raw': {e_gen}")
        if db_conn:
            db_conn.rollback()
        return False

def insert_into_event_raw(conn, source, source_event_id, metro_id, raw_data_payload, script_metadata):
    """Inserts data into the event_raw table.
    Args:
        script_metadata (dict): Contains original file_name, file_path, file_type, city, dance_style from script.
                                This will be embedded into the raw_json.
    """
    if not conn:
        logger.error("No database connection available for insertion into event_raw.")
        return False

    # Embed script_metadata into the raw_data_payload
    # Make a copy to avoid modifying the original dict if it's reused
    final_raw_json = dict(raw_data_payload)
    final_raw_json["_script_discovery_metadata"] = script_metadata
    
    sql_insert = """
        INSERT INTO event_raw (source, source_event_id, metro_id, raw_json, discovered_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(3))
        ON CONFLICT (source, source_event_id) DO NOTHING;
    """
    # We use DO NOTHING. If an update is needed, the conflict target and SET clause would change.
    # For DO NOTHING to work effectively, source_event_id should be reliably unique for a given source.
    # If source_event_id is NULL, ON CONFLICT might not behave as expected for multiple NULLs.

    try:
        with conn.cursor() as cur:
            cur.execute(sql_insert, (source, source_event_id, metro_id, Json(final_raw_json)))
            conn.commit()
            if cur.rowcount > 0:
                logger.info(f"Successfully inserted into event_raw: source_id={source_event_id}, source={source}.")
            else:
                # This case means either (source, source_event_id) matched an existing record, 
                # or source_event_id was NULL and the insert proceeded without conflict but also without unique match on NULL.
                # More precise logging might be needed if NULL source_event_id is common.
                logger.info(f"Data for source_id={source_event_id}, source={source} may already exist or skipped due to conflict. Rows affected: {cur.rowcount}.")
            return True
    except psycopg2.Error as e:
        logger.error(f"DB insert error for event_raw (source_id={source_event_id}): {e}")
        conn.rollback()
        return False
    except Exception as e_gen:
        logger.error(f"Generic error during DB op for event_raw (source_id={source_event_id}): {e_gen}")
        if conn:
            conn.rollback()
        return False

# --- NEW DATAFORSEO EVENTS API FUNCTIONS ---

def enrich_metros_with_location_codes(metros_df, dataforseo_login, dataforseo_password):
    """
    Enriches the metros DataFrame with 'location_code' from DataForSEO API
    for Google Events, filtering for 'City' type locations.
    Caches the full list of locations to avoid repeated large API calls.
    """
    logger.info("Starting enrichment of metros with location codes for Google Events.")
    cache_dir = Path("C:/temp/dataforseo_cache") # Consider making this configurable
    cache_dir.mkdir(parents=True, exist_ok=True)
    locations_cache_file = cache_dir / "google_events_locations_cache.json"

    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}

    if 'location_code' not in metros_df.columns:
        metros_df['location_code'] = pd.NA
    metros_df['location_code'] = metros_df['location_code'].astype('object')


    all_locations_filtered = []

    if locations_cache_file.exists():
        logger.info(f"Loading Google Events locations from cache: {locations_cache_file}")
        try:
            with open(locations_cache_file, "r") as f:
                cached_data = json.load(f)
            # Assuming cached_data is the direct list of locations or the API response structure
            if isinstance(cached_data, list): # If cache stores the direct list
                all_locations_unfiltered = cached_data
            elif isinstance(cached_data, dict) and cached_data.get("tasks") and data["tasks"][0].get("result"): # if cache stores full API response
                all_locations_unfiltered = cached_data["tasks"][0]["result"]
            else:
                all_locations_unfiltered = []

            for loc in all_locations_unfiltered:
                if loc.get("location_type", "").strip().lower() == "city":
                    all_locations_filtered.append(loc)
            logger.info(f"Successfully loaded and filtered {len(all_locations_filtered)} 'City' type Google Events locations from cache.")
            if not all_locations_filtered and all_locations_unfiltered:
                 logger.warning("Cache contained locations, but none were of type 'City'.")

        except json.JSONDecodeError:
            logger.error("Failed to decode locations cache. Fetching from API.")
            all_locations_filtered = None # Signal to fetch from API
    else:
        logger.info("Google Events locations cache not found. Will fetch from API.")
        all_locations_filtered = None # Signal to fetch from API

    if all_locations_filtered is None: # Fetch from API if cache missed or was invalid
        all_locations_filtered = [] # Reset to empty list for appending
        locations_endpoint = "https://api.dataforseo.com/v3/serp/google/events/locations"
        logger.info(f"Fetching all Google Events locations from {locations_endpoint}...")
        try:
            response = requests.get(locations_endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()

            if (data.get("status_code") == 20000 and
                data.get("tasks") and
                isinstance(data.get("tasks"), list) and
                len(data.get("tasks")) > 0 and
                data["tasks"][0].get("status_code") == 20000 and
                data["tasks"][0].get("result") and
                isinstance(data["tasks"][0].get("result"), list)):
                
                all_locations_unfiltered_api = data["tasks"][0]["result"]
                logger.info(f"Successfully fetched {len(all_locations_unfiltered_api)} total Google Events locations from API.")
                
                # Filter for 'City' type and save only these to cache
                city_locations_to_cache = []
                for loc in all_locations_unfiltered_api:
                    if loc.get("location_type", "").strip().lower() == "city":
                        all_locations_filtered.append(loc)
                        city_locations_to_cache.append(loc)
                
                if city_locations_to_cache:
                    with open(locations_cache_file, "w") as f:
                        json.dump(city_locations_to_cache, f) # Cache only city type
                    logger.info(f"Saved {len(city_locations_to_cache)} 'City' type Google Events locations to cache: {locations_cache_file}")
                else:
                    logger.warning("Fetched locations from API, but no 'City' type locations found to cache.")
            else:
                logger.error(
                    f"Failed to fetch Google Events locations or data was empty/malformed. API Status: {data.get('status_code')}, Message: {data.get('status_message')}"
                )
                logger.error(f"Full response from /events/locations: {json.dumps(data, indent=2)}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Google Events locations: {e}")
            return metros_df.copy()
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from Google Events locations API: {e}")
            return metros_df.copy()

    if not all_locations_filtered:
        logger.error("No 'City' type Google Events locations could be loaded or fetched. Cannot map location_code accurately.")
        return metros_df.copy()

    location_codes_assigned = []
    for index, row in metros_df.iterrows():
        city_name_csv = str(row["name"]).strip().lower()
        country_csv = str(row.get("country_code", "")).strip().lower()
        admin1_csv = str(row.get("admin1_code", "")).strip().lower()
        found_code = pd.NA
        best_match_candidate = None

        for loc_api in all_locations_filtered: # Already filtered for 'City'
            api_loc_name_full = str(loc_api.get("location_name", "")).strip().lower()
            api_country_code = str(loc_api.get("country_iso_code", "")).strip().lower()
            # Assuming subdivision_name for state/province, adjust if API uses a different field
            api_subdivision = str(loc_api.get("subdivision_name", "")).strip().lower() 
            api_city_part = api_loc_name_full.split(',')[0].strip()

            if city_name_csv == api_city_part:
                match_score = 10 # Base score for city name match
                if country_csv and api_country_code:
                    if country_csv == api_country_code:
                        match_score += 5
                    else: continue # Country mismatch, skip
                if admin1_csv and api_subdivision:
                    if admin1_csv == api_subdivision or (len(admin1_csv) <= 3 and api_subdivision.startswith(admin1_csv)):
                        match_score += 3
                    # else: continue # Optional: stricter admin1 match

                if best_match_candidate is None or match_score > best_match_candidate['score']:
                    best_match_candidate = {'data': loc_api, 'score': match_score}
        
        if best_match_candidate:
            found_code = best_match_candidate['data']["location_code"]
            logger.info(f"Matched: {city_name_csv} (CSV) with {best_match_candidate['data'].get('location_name')} (API) -> location_code: {found_code} (Score: {best_match_candidate['score']})")
        else:
             # Fallback: Broader search if no primary match was found for the city
            for loc_api in all_locations_filtered:
                api_loc_name_full = str(loc_api.get("location_name", "")).strip().lower()
                api_city_part = api_loc_name_full.split(',')[0].strip()
                api_country_code = str(loc_api.get("country_iso_code", "")).strip().lower()
                if city_name_csv in api_city_part: # CSV city is part of API city name
                    if country_csv and api_country_code and country_csv != api_country_code:
                        continue
                    found_code = loc_api["location_code"]
                    logger.info(f"Partial Match: {city_name_csv} (CSV) with {api_loc_name_full} (API) -> location_code: {found_code}")
                    break 
        
        if pd.isna(found_code):
            logger.warning(f"Location code (type 'City') still not found for city: {row['name']} (CSV: '{city_name_csv}'). Will be NaN.")
        location_codes_assigned.append(found_code)

    metros_df["location_code"] = location_codes_assigned
    logger.info("Finished enriching metros with Google Events location codes.")
    logger.info("Cities with location_code:")
    logger.info(metros_df[metros_df['location_code'].notna()][['name', 'location_code']].to_string())
    logger.info("Cities WITHOUT location_code:")
    logger.info(metros_df[metros_df['location_code'].isna()][['name', 'admin1_code', 'country_code']].to_string())
    
    return metros_df.copy()

def batch_api_tasks(cities_df, broad_search_terms, dataforseo_login, dataforseo_password):
    """
    Submits batch tasks to DataForSEO API for Google Events using broad search terms.
    broad_search_terms: A list of terms like ["dance", "dancing"] to be used with "in [city]".
    """
    global task_metadata_map
    
    logger.info(f"Preparing to submit BROAD Google Events tasks for {len(cities_df)} cities using terms: {broad_search_terms}.")
    post_data_array = []
    # Initialize task IDs list to return
    all_task_ids_from_submission = []
    # NEW: Also track all tags to enable /id_list retrieval
    all_tags_from_submission = {} 

    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}
    
    endpoint = "https://api.dataforseo.com/v3/serp/google/events/task_post"

    for index, city_info in cities_df.iterrows():
        if pd.isna(city_info.get("location_code")):
            logger.warning(f"Skipping city {city_info['name']} for BROAD EVENTS task due to missing location_code.")
            continue
        
        city_lang_code = city_info.get('language_code', 'en') # Default to 'en' if somehow still missing

        for term in broad_search_terms:
            query = f"{term} in {city_info['name']}"
            # Note: Consider if broad terms like "dance" need special quoting like "coast swing" did.
            # For now, assuming simple concatenation is fine for single broad terms.

            tag_content = f"city_{city_info['geonameid']}_term_{term.replace(' ', '_')}_lang_{city_lang_code}_run_{int(time.time())}"

            task_payload = {
                "keyword": query,
                "location_code": int(city_info["location_code"]),
                "language_code": city_lang_code, # Use dynamic language code
                "depth": RESULTS_PER_PAGE, # Max 100 for events
                "tag": tag_content
                # Removed 'date_range' - it was causing "Invalid Field" errors
            }
            post_data_array.append(task_payload)
    
    if not post_data_array:
        logger.warning("No tasks to submit after filtering for location_codes.")
        return []

    submitted_task_ids = []
    logger.info(f"Submitting {len(post_data_array)} tasks to {endpoint}...")
    
    # DataForSEO batch limit is 100 tasks per array in POST.
    # If post_data_array is larger, it needs to be chunked.
    # For now, assuming len(post_data_array) <= 100. Add chunking if necessary.
    # Max 1000 tasks per POST request, each array in POST can have up to 100 tasks.
    # So, if we have 250 tasks, we'd send [{"POST_ARRAY_1"}, {"POST_ARRAY_2"}, {"POST_ARRAY_3_len_50"}]
    # The structure for multi-array POST is a list of these arrays:
    # [ [{"keyword":..}, {"keyword":..}], [{"keyword":..}] ]
    # But our current structure for post_data_array is already a list of task_payloads.
    # The API expects a direct list of up to 100 task objects if sending a single array.
    # If sending multiple arrays (up to 1000 total tasks), it's a list of lists.
    # Let's stick to single array of up to 100 tasks for simplicity first.

    MAX_TASKS_PER_POST_ARRAY = 100
    all_task_ids_from_submission = []

    for i in range(0, len(post_data_array), MAX_TASKS_PER_POST_ARRAY):
        chunk = post_data_array[i:i + MAX_TASKS_PER_POST_ARRAY]
        try:
            response = requests.post(endpoint, json=chunk, headers=headers)
            response.raise_for_status()
            result = response.json()

            if result.get("status_code") == 20000 and result.get("tasks_count") > 0:
                logger.info(f"Successfully submitted a chunk of {len(chunk)} tasks. API tasks_count: {result.get('tasks_count')}.")
                for task_info in result.get("tasks", []):
                    task_id = task_info.get("id")
                    if task_id: # If an ID is present, we consider it a task to track
                        original_payload_tag = task_info.get("data", {}).get("tag", "no_tag_in_response")
                        all_task_ids_from_submission.append(task_id)
                        
                        task_item_status_code = task_info.get("status_code")
                        task_item_status_message = task_info.get("status_message", "No status message")

                        if task_item_status_code == 20100: # Standard "Task Created."
                            logger.info(f"Task {task_id} created successfully. Tag: {original_payload_tag}. Status: {task_item_status_code} ({task_item_status_message})")
                        elif task_item_status_code == 20000: # Should ideally be 20100 for creation
                             logger.info(f"Task {task_id} reported with status 20000 (Ok) at creation. Tag: {original_payload_tag}. Status: {task_item_status_code} ({task_item_status_message})")
                        else: # Log other statuses for individual tasks if they are not the expected "Task Created"
                            logger.warning(f"Task {task_id} created but with unexpected status. Tag: {original_payload_tag}. Status: {task_item_status_code} ({task_item_status_message}). Payload: {task_info.get('data')}")

                        # Reconstruct metadata from tag if possible
                        try:
                            parts = original_payload_tag.split('_')
                            metro_id_from_tag = int(parts[1])
                            # Tag: city_METROID_term_TERM_lang_LANG_run_TIMESTAMP
                            term_idx_start = parts.index("term") + 1
                            lang_idx_start = parts.index("lang")
                            term_from_tag = "_".join(parts[term_idx_start:lang_idx_start])

                            city_name_from_tag = cities_df[cities_df['geonameid'] == metro_id_from_tag]['name'].iloc[0] \
                                               if metro_id_from_tag in cities_df['geonameid'].values else "UnknownCity"

                            task_metadata_map[task_id] = {
                                "metro_id": metro_id_from_tag,
                                "city_name": city_name_from_tag,
                                "dance_style": term_from_tag, # Storing the broad term as dance_style for map consistency
                                "search_type": "event", 
                                "original_tag": original_payload_tag
                            }
                            
                            # NEW: Store metadata by tag for /id_list retrieval
                            all_tags_from_submission[original_payload_tag] = {
                                "metro_id": metro_id_from_tag,
                                "city_name": city_name_from_tag,
                                "dance_style": term_from_tag,
                                "search_type": "event",
                                "task_id": task_id  # Store the original task_id
                            }
                        except (ValueError, IndexError, KeyError) as e_tag_parse: # Combined exceptions
                            logger.error(f"Could not parse tag for EVENT task {task_id} ('{original_payload_tag}'): {e_tag_parse}")
                            task_metadata_map[task_id] = {
                                "metro_id": None, "city_name": "UnknownFromTagParse", 
                                "dance_style": "UnknownTermFromTagParse", "search_type": "event", 
                                "original_tag": original_payload_tag
                            }
                        # No change to debug log, it's fine as is
                        logger.debug(f"Task {task_id} submitted. Tag: {original_payload_tag}. Mapped metadata: {task_metadata_map[task_id]}")
                    else: # No task_id in the task_info item
                        logger.error(f"Task submission issue: No ID returned for a task item. Full item: {task_info}")
            else:
                logger.error(f"Batch task submission failed. Status: {result.get('status_code')}, Message: {result.get('status_message')}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception during batch task submission: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error during batch task submission: {e}. Response text: {response.text if 'response' in locals() else 'N/A'}")
    
    if all_task_ids_from_submission:
        logger.info(f"Total tasks submitted across all chunks: {len(all_task_ids_from_submission)}. Task IDs: {all_task_ids_from_submission}")
    else:
        logger.warning("No tasks were successfully submitted or no task IDs were returned.")
        
    # Return both the task IDs and the tag map
    return all_task_ids_from_submission, all_tags_from_submission


def batch_organic_style_tasks(cities_df, specific_dance_styles, dataforseo_login, dataforseo_password):
    """
    Submits batch tasks to DataForSEO API for Google Organic Search for specific dance styles.
    specific_dance_styles: A list of styles like ["salsa", "bachata"].
    """
    global task_metadata_map
    
    logger.info(f"Preparing to submit SPECIFIC ORGANIC tasks for {len(cities_df)} cities and {len(specific_dance_styles)} dance styles.")
    post_data_array = []
    # Initialize task IDs list to return
    all_task_ids_from_submission = []

    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}
    
    # Endpoint for Google Organic Search tasks
    endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_post"

    for index, city_info in cities_df.iterrows():
        # For Organic, location_name is generally used. location_code might not be supported or behave differently.
        # We will use asciiname which should be robust. If not available, fallback to name.
        location_name_for_api = city_info.get('asciiname', city_info.get('name'))
        if pd.isna(location_name_for_api):
            logger.warning(f"Skipping city {city_info.get('name', 'N/A')} for ORGANIC tasks due to missing location name.")
            continue
        
        city_lang_code = city_info.get('language_code', 'en')
        country_code = city_info.get("country_code", "")
        se_domain_for_api = country_code.lower() + ".google.com" if country_code else "google.com"
        if country_code.lower() == "us": # For US, use google.com
             se_domain_for_api = "google.com"

        for style in specific_dance_styles:
            query = f"{style} in {location_name_for_api}" 
            if style == "coast swing": # Ensure quotes for multi-word style
                query = f'"coast swing" in {location_name_for_api}'

            tag_content = f"city_{city_info['geonameid']}_style_{style.replace(' ', '_')}_organic_lang_{city_lang_code}_run_{int(time.time())}"

            task_payload = {
                "keyword": query,
                "location_name": location_name_for_api, # Using location_name for Organic
                "language_code": city_lang_code,
                "depth": 7,  # Number of SERP pages to crawl, aiming for ~70-100 results. Max for organic is often 700 results (depth 70 * 10 results/page or depth 7 * 100 results/page)
                "results_on_page": 100, # Request 100 results per page to minimize pages needed if API supports this for tasks
                "se_domain": se_domain_for_api,
                "tag": tag_content 
            }
            post_data_array.append(task_payload)
    
    if not post_data_array:
        logger.warning("No ORGANIC tasks to submit.")
        return []

    # Submission logic (chunking and requests) is similar to batch_api_tasks
    # This part can be refactored into a common helper if it grows more complex
    MAX_TASKS_PER_POST_ARRAY = 100
    all_task_ids_from_submission = []

    for i in range(0, len(post_data_array), MAX_TASKS_PER_POST_ARRAY):
        chunk = post_data_array[i:i + MAX_TASKS_PER_POST_ARRAY]
        logger.info(f"Submitting {len(chunk)} ORGANIC tasks to {endpoint} (chunk {i // MAX_TASKS_PER_POST_ARRAY + 1})...")
        try:
            response = requests.post(endpoint, json=chunk, headers=headers)
            response.raise_for_status()
            result = response.json()

            if result.get("status_code") == 20000 and result.get("tasks_count", 0) > 0:
                logger.info(f"Successfully submitted a chunk of {len(chunk)} ORGANIC tasks. API tasks_count: {result.get('tasks_count')}.")
                for task_info in result.get("tasks", []):
                    task_id = task_info.get("id")
                    if task_id:
                        original_payload_tag = task_info.get("data", {}).get("tag", "no_tag_in_response")
                        all_task_ids_from_submission.append(task_id)
                        
                        task_item_status_code = task_info.get("status_code")
                        task_item_status_message = task_info.get("status_message", "No status message")

                        if task_item_status_code == 20100: # Standard "Task Created."
                            logger.info(f"ORGANIC Task {task_id} created successfully. Tag: {original_payload_tag}.")
                        else: 
                            logger.warning(f"ORGANIC Task {task_id} created with status {task_item_status_code} ({task_item_status_message}). Tag: {original_payload_tag}.")

                        try:
                            parts = original_payload_tag.split('_')
                            metro_id_from_tag = int(parts[1])
                            style_idx = parts.index("style") + 1
                            organic_idx = parts.index("organic")
                            style_from_tag = "_".join(parts[style_idx:organic_idx])
                            city_name_from_tag = cities_df[cities_df['geonameid'] == metro_id_from_tag]['name'].iloc[0] \
                                               if metro_id_from_tag in cities_df['geonameid'].values else "UnknownCity"

                            task_metadata_map[task_id] = {
                                "metro_id": metro_id_from_tag,
                                "city_name": city_name_from_tag,
                                "dance_style": style_from_tag, # Storing the specific style
                                "search_type": "organic", # Differentiate from event searches
                                "original_tag": original_payload_tag
                            }
                        except (ValueError, IndexError, KeyError) as e_tag_parse:
                            logger.error(f"Could not parse tag for ORGANIC task {task_id} ('{original_payload_tag}'): {e_tag_parse}")
                            task_metadata_map[task_id] = {
                                "metro_id": None, "city_name": "UnknownFromTagParse", 
                                "dance_style": "UnknownFromTagParse", "search_type": "organic", 
                                "original_tag": original_payload_tag }
                        logger.debug(f"ORGANIC Task {task_id} submitted. Tag: {original_payload_tag}. Mapped metadata: {task_metadata_map.get(task_id)}")
                    else:
                        logger.error(f"ORGANIC Task submission issue: No ID returned. Item: {task_info}")
            else:
                logger.error(f"ORGANIC batch task submission failed. Status: {result.get('status_code')}, Message: {result.get('status_message')}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception during ORGANIC batch task submission: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error during ORGANIC batch task submission: {e}. Response: {response.text if 'response' in locals() else 'N/A'}")
    
    if all_task_ids_from_submission:
        logger.info(f"Total ORGANIC tasks submitted: {len(all_task_ids_from_submission)}.")
    else:
        logger.warning("No ORGANIC tasks were successfully submitted or no task IDs returned.")
        
    return all_task_ids_from_submission


def poll_task_results(task_ids_with_metadata_map, dataforseo_login, dataforseo_password, db_conn):
    """
    Polls for completed tasks and retrieves results.
    task_ids_with_metadata_map: dict mapping task_id to {metro_id, city_name, dance_style, search_type}
    """
    if not task_ids_with_metadata_map:
        logger.info("No task IDs to poll.")
        return

    logger.info(f"Starting to poll for {len(task_ids_with_metadata_map)} task(s).")
    logger.info(f"Script is waiting for these Task IDs (from task_metadata_map): {list(task_ids_with_metadata_map.keys())}")

    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}"}

    tasks_ready_endpoint = "https://api.dataforseo.com/v3/serp/google/events/tasks_ready"
    
    # Set of task IDs we are waiting for
    pending_task_ids = set(task_ids_with_metadata_map.keys())
    
    # Exponential backoff parameters
    initial_poll_interval_seconds = 10  # Start with 10 seconds
    max_poll_interval_seconds = 60      # Cap at 60 seconds
    poll_interval_multiplier = 1.5      # Increase interval by 50% each time
    max_poll_attempts = 30              # Aim for roughly 27 mins max polling time

    current_poll_interval_seconds = initial_poll_interval_seconds
    attempts = 0

    while pending_task_ids and attempts < max_poll_attempts:
        attempts += 1
        logger.info(f"Polling attempt {attempts}/{max_poll_attempts}. {len(pending_task_ids)} task(s) still pending.")
        logger.info(f"Waiting for {current_poll_interval_seconds:.1f} seconds before next poll.")
        time.sleep(current_poll_interval_seconds)
        current_poll_interval_seconds = min(current_poll_interval_seconds * poll_interval_multiplier, max_poll_interval_seconds)

        try: # Outer try for /tasks_ready polling
            response_ready = requests.get(tasks_ready_endpoint, headers=headers)
            response_ready.raise_for_status()
            ready_data = response_ready.json()

            if ready_data.get("status_code") == 20000 and ready_data.get("tasks_count", 0) > 0:
                tasks_info = ready_data.get("tasks", [])
                ready_ids_from_api = {task.get("id") for task in tasks_info if task.get("id")}
                logger.info(f"/tasks_ready reported these IDs: {list(ready_ids_from_api) if ready_ids_from_api else 'None'}")
                
                actually_ready_and_mine = pending_task_ids.intersection(ready_ids_from_api)

                if actually_ready_and_mine:
                    logger.info(f"Found {len(actually_ready_and_mine)} task(s) matching our pending list: {list(actually_ready_and_mine)}")
                    
                    for task_id_to_get in list(actually_ready_and_mine): 
                        metadata = task_ids_with_metadata_map.get(task_id_to_get, {})
                        search_type = metadata.get("search_type", "event")
                        
                        # Determine the endpoint based on search_type
                        if search_type == "organic":
                            current_task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced"
                            db_source_string_prefix = "dataforseo_organic_item_polled"
                        else: 
                            current_task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/events/task_get/advanced"
                            db_source_string_prefix = "dataforseo_event_item_polled"

                        task_get_url = f"{current_task_get_endpoint}/{task_id_to_get}"
                        logger.info(f"Fetching [{search_type.upper()}] results for task: {task_id_to_get} from {task_get_url}")
                        
                        try: # Inner try for individual task_get
                            response_get = requests.get(task_get_url, headers=headers)
                            response_get.raise_for_status()
                            task_result_data = response_get.json()

                            if (task_result_data.get("status_code") == 20000 and
                                task_result_data.get("tasks_count", 0) > 0 and
                                task_result_data.get("tasks")):
                                
                                task_detail = task_result_data["tasks"][0]
                                if task_detail.get("status_code") == 20000 and task_detail.get("result"):
                                    logger.info(f"Successfully retrieved [{search_type.upper()}] results for task {task_id_to_get}. Processing items...")
                                    result_blocks = task_detail["result"] 
                                    metro_id = metadata.get("metro_id")
                                    city_name_ctx = metadata.get("city_name", "Unknown")
                                    style_or_term_ctx = metadata.get("dance_style", "Unknown") 

                                    if metro_id is None: 
                                        logger.warning(f"Metro_id is None for task {task_id_to_get} ([{search_type.upper()}]). Items will have NULL metro_id.")
                                    
                                    script_meta_for_db = {
                                        "dataforseo_api_task_id": task_id_to_get,
                                        "city_name_context": city_name_ctx,
                                        "style_or_term_context": style_or_term_ctx,
                                        "original_tag": metadata.get("original_tag"),
                                        "search_type_context": search_type
                                    }

                                    items_inserted_for_this_task = 0
                                    for res_block in result_blocks:
                                        if res_block and isinstance(res_block, dict) and res_block.get("items"):
                                            for item_data in res_block.get("items", []):
                                                if not item_data or not isinstance(item_data, dict):
                                                    continue
                                                
                                                item_id_for_db = None
                                                payload_for_db = {}
                                                
                                                if search_type == "event":
                                                    item_id_for_db = create_event_item_id(item_data)
                                                    payload_for_db = {
                                                        "event_item_data": item_data, 
                                                        "api_task_info_context": {
                                                            "task_id": task_detail.get("id"),
                                                            "keyword": task_detail.get("data", {}).get("keyword"),
                                                            "location_code": task_detail.get("data", {}).get("location_code"),
                                                            "language_code": task_detail.get("data", {}).get("language_code"),
                                                            "tag": task_detail.get("data", {}).get("tag")
                                                        }
                                                    }
                                                elif search_type == "organic":
                                                    item_id_for_db = item_data.get("url") or hashlib.md5(str(item_data.get("title", "") + str(item_data.get("description", ""))).encode()).hexdigest()
                                                    payload_for_db = {
                                                        "organic_item_data": item_data,
                                                        "api_task_info_context": {
                                                            "task_id": task_detail.get("id"),
                                                            "keyword": task_detail.get("data", {}).get("keyword"),
                                                            "location_name": task_detail.get("data", {}).get("location_name"),
                                                            "language_code": task_detail.get("data", {}).get("language_code"),
                                                            "tag": task_detail.get("data", {}).get("tag")
                                                        }
                                                    }
                                                
                                                if not item_id_for_db:
                                                    logger.warning(f"Skipping [{search_type.upper()}] item in task {task_id_to_get}: {item_data.get('title', 'N/A')} due to missing ID.")
                                                    continue
                                                
                                                if insert_into_event_raw(
                                                    conn=db_conn,
                                                    source=db_source_string_prefix,
                                                    source_event_id=str(item_id_for_db),
                                                    metro_id=metro_id,
                                                    raw_data_payload=payload_for_db,
                                                    script_metadata=script_meta_for_db
                                                ):
                                                    items_inserted_for_this_task += 1
                                    
                                    if items_inserted_for_this_task > 0:
                                        logger.info(f"Inserted {items_inserted_for_this_task} [{search_type.upper()}] items from task {task_id_to_get}.")
                                    else:
                                        logger.info(f"No [{search_type.upper()}] items inserted from task {task_id_to_get} (none found or all duplicates/failed ID).")
                                    
                                    pending_task_ids.remove(task_id_to_get)
                                else:
                                    logger.error(f"Task GET for {task_id_to_get} ([{search_type.upper()}]) was 20000 but result empty/errored. Detail: {task_detail.get('status_message')}")
                            else:
                                logger.error(f"Failed to get valid results for task {task_id_to_get} ([{search_type.upper()}]). API status: {task_result_data.get('status_code')}, Msg: {task_result_data.get('status_message')}")
                        
                        except requests.exceptions.RequestException as e_get:
                            logger.error(f"Request exception for task {task_id_to_get} ([{search_type.upper()}]): {e_get}")
                        except json.JSONDecodeError as e_json_get:
                            logger.error(f"JSON decode error for task {task_id_to_get} ([{search_type.upper()}]): {e_json_get}")
                        
                        # After attempting to process a task (successfully or not), pause.
                        logger.debug(f"Pausing for {REQUEST_DELAY:.1f}s after attempting task {task_id_to_get} before next in batch (if any).")
                        time.sleep(REQUEST_DELAY)
                
                else: # No tasks from *our* batch were in the /tasks_ready response
                    if ready_ids_from_api: # Some tasks were ready, but not ours
                        logger.info(f"{len(ready_ids_from_api)} task(s) reported in /tasks_ready response, but none match our pending tasks. Pending: {len(pending_task_ids)}. API Ready: {list(ready_ids_from_api)}")
                    else: # No tasks at all in /tasks_ready
                        logger.info(f"No tasks reported as ready in /tasks_ready response (out of {len(pending_task_ids)} pending).")
            
            elif ready_data.get("status_code") != 20000:
                logger.error(f"Polling /tasks_ready failed. Status: {ready_data.get('status_code')}, Message: {ready_data.get('status_message')}")
            else: # Status 20000 but tasks_count is 0 or tasks array missing/empty
                logger.info(f"Polling /tasks_ready: 0 tasks ready. {len(pending_task_ids)} still pending.")

        except requests.exceptions.RequestException as e_ready:
            logger.error(f"Error polling /tasks_ready: {e_ready}")
        except json.JSONDecodeError as e_json_ready:
            logger.error(f"JSON decode error polling /tasks_ready: {e_json_ready}")

        if pending_task_ids: # Only sleep if there are still pending tasks
            time.sleep(current_poll_interval_seconds)

    if attempts >= max_poll_attempts and pending_task_ids:
        logger.warning(f"Max polling attempts ({max_poll_attempts}) reached. {len(pending_task_ids)} task(s) are still pending: {list(pending_task_ids)}")

    if pending_task_ids:
        logger.warning(f"Finished polling attempts. {len(pending_task_ids)} tasks remain uncompleted: {list(pending_task_ids)}")
    else:
        logger.info("All tasks successfully polled and results processed.")

# --- END NEW DATAFORSEO EVENTS API FUNCTIONS ---

# --- Main Execution ---

# Function to retrieve specific, known tasks
def retrieve_known_tasks(known_task_ids, dataforseo_login, dataforseo_password, db_conn):
    """
    Directly retrieves results for specific DataForSEO task IDs.
    Use this for manual retrieval of previously submitted tasks.
    
    Args:
        known_task_ids: List of task IDs to retrieve
        dataforseo_login: DataForSEO API login
        dataforseo_password: DataForSEO API password
        db_conn: Database connection
    """
    logger.info(f"Starting direct retrieval for {len(known_task_ids)} specific task IDs.")
    if not known_task_ids:
        logger.warning("No known task IDs provided for retrieval.")
        return

    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}"}
    
    # Default to events endpoint, as this script now only handles events
    task_get_base_endpoint = "https://api.dataforseo.com/v3/serp/google/events/task_get/advanced"
    search_type = "event"
    
    total_items_inserted = 0
    tasks_with_results = 0
    tasks_failed_or_empty = 0

    for task_id in known_task_ids:
        # Endpoint and search_type are now fixed to "event"
        
        metadata_for_task = {
            "metro_id": None, 
            "city_name": "Unknown (Retrieved by ID)",
            "dance_style": "Unknown (Retrieved by ID)", # This will be the broad term like "dance" or "dancing"
            "search_type": search_type,
            "original_tag": f"retrieved_by_id_{task_id}"
        }
        
        task_get_url = f"{task_get_base_endpoint}/{task_id}"
        logger.info(f"Directly fetching EVENT results for task_id: {task_id} from {task_get_url}")
        
        try:
            response_get = requests.get(task_get_url, headers=headers)
            response_get.raise_for_status()
            task_result_data = response_get.json()

            if (task_result_data.get("status_code") == 20000 and
                task_result_data.get("tasks_count") > 0 and
                task_result_data.get("tasks")):
                
                task_detail = task_result_data["tasks"][0]
                if task_detail.get("status_code") == 20000 and task_detail.get("result"):
                    logger.info(f"Successfully retrieved results for task {task_id}. Processing items...")
                    
                    # Try to extract more detailed metadata from the task data
                    task_data = task_detail.get("data", {})
                    if task_data:
                        task_keyword = task_data.get("keyword", "")
                        city_name_match = re.search(r'in\s+([^"]+)$', task_keyword)
                        if city_name_match:
                            metadata_for_task["city_name"] = city_name_match.group(1).strip()
                        
                        dance_style_match = re.search(r'^([^"]+?)\s+in\s', task_keyword)
                        if dance_style_match:
                            metadata_for_task["dance_style"] = dance_style_match.group(1).strip()
                    
                    items_inserted_for_this_task = 0
                    result_blocks = task_detail["result"] 
                    metro_id = metadata_for_task.get("metro_id")
                    
                    script_meta_for_db = {
                        "dataforseo_api_task_id": task_id,
                        "city_name_context": metadata_for_task["city_name"],
                        "dance_style_context": metadata_for_task["dance_style"],
                        "search_type": metadata_for_task["search_type"],
                        "original_tag": metadata_for_task["original_tag"]
                    }

                    for result_block in result_blocks:
                        if result_block and isinstance(result_block, dict) and result_block.get("items"):
                            for item_data in result_block.get("items", []):
                                if not item_data or not isinstance(item_data, dict):
                                    continue

                                # Generate a unique ID based on search type (always event)
                                item_id = create_event_item_id(item_data)
                                
                                if not item_id:
                                    logger.warning(f"Skipping item due to missing ID in task {task_id}: {item_data.get('title', 'No title')}")
                                    continue
                                
                                # Prepare the payload (always event)
                                payload_for_db = {
                                    "event_item_data": item_data,
                                    "api_task_info_context": { 
                                        "task_id": task_detail.get("id"),
                                        "keyword": task_data.get("keyword"),
                                        "location_code": task_data.get("location_code"),
                                        # "location_name": task_data.get("location_name"), # Not relevant for events task
                                        "language_code": task_data.get("language_code"),
                                        "tag": task_data.get("tag")
                                    }
                                }

                                # Insert into event_raw table
                                if insert_into_event_raw(
                                    conn=db_conn,
                                    source=f"dataforseo_event_item_manual", # Fixed source
                                    source_event_id=str(item_id), 
                                    metro_id=metro_id, 
                                    raw_data_payload=payload_for_db,
                                    script_metadata=script_meta_for_db 
                                ):
                                    items_inserted_for_this_task += 1
                   
                    if items_inserted_for_this_task > 0:
                        tasks_with_results += 1
                        total_items_inserted += items_inserted_for_this_task
                        logger.info(f"Inserted {items_inserted_for_this_task} event items from task {task_id}.")
                    else:
                        logger.info(f"No event items inserted from task {task_id} (either no items found or all were duplicates/failed ID generation).")
                        tasks_failed_or_empty += 1
                else:
                    logger.error(f"Task GET for {task_id} was status 20000 but result was empty or errored. Task detail: {task_detail.get('status_message')}")
                    tasks_failed_or_empty += 1
            else:
                logger.error(f"Failed to get valid results for task {task_id}. API status: {task_result_data.get('status_code')}, Message: {task_result_data.get('status_message')}")
                tasks_failed_or_empty += 1
        except requests.exceptions.RequestException as e_get:
            logger.error(f"Error fetching result for task {task_id}: {e_get}")
            tasks_failed_or_empty += 1
        except json.JSONDecodeError as e_json_get:
            logger.error(f"JSON decode error fetching result for task {task_id}: {e_json_get}")
            tasks_failed_or_empty += 1
        
        time.sleep(REQUEST_DELAY if REQUEST_DELAY > 0.5 else 0.5) 

    logger.info(f"Finished direct retrieval of known tasks. Tasks with results: {tasks_with_results}, Tasks failed/empty: {tasks_failed_or_empty}, Total individual items inserted: {total_items_inserted}.")

# Helper function to create a unique ID for an event item
def create_event_item_id(event_item):
    if not event_item or not isinstance(event_item, dict):
        return None
    # Prefer a direct event_id if provided by API, or a unique URL
    event_specific_id = event_item.get("event_id") # Check if API provides this
    if event_specific_id:
        return str(event_specific_id)
    
    url = event_item.get("url") # Primary event URL
    if url:
        # Normalize URL slightly by removing query parameters for common event sites if they cause too much uniqueness
        parsed_url = urlparse(url)
        if parsed_url.netloc in ["allevents.in", "eventbrite.com", "facebook.com"]:
            return str(parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path)
        return str(url)

    # Fallback: create a hash from key properties if no direct ID/URL
    title = event_item.get("title", "")
    start_datetime = event_item.get("event_dates", {}).get("start_datetime", "")
    # location_name = event_item.get("location_info", {}).get("name", "") # Location can be too variable
    
    if title and start_datetime: # Require at least title and start time for a meaningful hash
        hash_input = f"{title}|{start_datetime}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    logger.warning(f"Could not generate a reliable unique ID for event item: {event_item.get('title', 'Unknown title')}")
    return None # Cannot generate a good ID

def get_results_by_id_list(dataforseo_login, dataforseo_password, task_tags_with_metadata_map, db_conn):
    """
    Use the /id_list endpoint to get all completed tasks from the last hour by their tag
    This helps retrieve results even if the task IDs from /tasks_ready don't match our submitted IDs.
    
    Args:
        dataforseo_login: DataForSEO API login
        dataforseo_password: DataForSEO API password
        task_tags_with_metadata_map: dict mapping tag values to metadata about the task
        db_conn: Database connection for storing results
    
    Returns:
        List of task IDs from /id_list that were successfully retrieved
    """
    if not task_tags_with_metadata_map:
        logger.info("No task tags provided to check in /id_list.")
        return []
        
    logger.info(f"Checking /id_list for tasks with {len(task_tags_with_metadata_map)} tags...")
    
    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}
    
    # Set date_from to 1 hour ago to get recent tasks
    date_from = datetime.now(timezone.utc) - timedelta(hours=1)
    date_from_str = date_from.strftime("%Y-%m-%d %H:%M:%S %z")
    
    # Use the id_list endpoint for SERP API to get all completed tasks
    id_list_endpoint = "https://api.dataforseo.com/v3/serp/id_list"
    payload = {"date_from": date_from_str}

    try:
        response = requests.post(id_list_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("status_code") != 20000:
            logger.error(f"Error from /id_list: {result.get('status_code')}: {result.get('status_message')}")
            return []
            
        tasks = result.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            logger.info("No tasks found in /id_list response.")
            return []
            
        id_list_results = tasks[0].get("result", [])
        logger.info(f"Found {len(id_list_results)} tasks in /id_list from the past hour.")
        
        # Match tasks by their tag
        matched_tasks = []
        for task_result in id_list_results:
            # Skip if not an events API task - we only care about events tasks
            if not task_result.get("endpoint") or "events" not in task_result.get("endpoint"):
                continue
                
            result_tag = task_result.get("tag")
            if not result_tag:
                continue
                
            # Check if this tag is one of ours
            if result_tag in task_tags_with_metadata_map:
                logger.info(f"Found matching task with tag: {result_tag}")
                logger.info(f"Task details: ID: {task_result.get('id')}, Status: {task_result.get('status')}, Cost: {task_result.get('cost')}")
                
                # If task has a result_id, it's completed and has results we can fetch
                if task_result.get("result_id"):
                    # Extract task metadata from our tag map
                    metadata = task_tags_with_metadata_map[result_tag]
                    search_type = metadata.get("search_type", "event")  # Default to "event"
                    metro_id = metadata.get("metro_id")
                    
                    # Determine the endpoint based on search_type
                    if search_type == "organic":
                        task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced"
                        db_source_string_prefix = "dataforseo_organic_item_idlist"
                    else:  # Default to "event"
                        task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/events/task_get/advanced"
                        db_source_string_prefix = "dataforseo_event_item_idlist"
                    
                    # Get the result using the result_id (not the task ID!)
                    result_id = task_result.get("result_id")
                    task_get_url = f"{task_get_endpoint}/{result_id}"
                    
                    logger.info(f"Fetching [{search_type.upper()}] results for task with tag '{result_tag}' using result_id: {result_id}")
                    
                    try:
                        task_response = requests.get(task_get_url, headers=headers)
                        task_response.raise_for_status()
                        task_data = task_response.json()
                        
                        if (task_data.get("status_code") == 20000 and 
                            task_data.get("tasks") and 
                            task_data["tasks"][0].get("result")):
                            
                            task_detail = task_data["tasks"][0]
                            city_name_ctx = metadata.get("city_name", "Unknown")
                            style_or_term_ctx = metadata.get("dance_style", "Unknown") 
                            
                            script_meta_for_db = {
                                "dataforseo_api_result_id": result_id,
                                "dataforseo_api_task_id": task_result.get("id"),
                                "city_name_context": city_name_ctx,
                                "style_or_term_context": style_or_term_ctx,
                                "original_tag": result_tag,
                                "search_type_context": search_type,
                                "id_list_retrieval": True
                            }
                            
                            items_inserted = 0
                            result_blocks = task_detail["result"]
                            
                            for result_block in result_blocks:
                                if result_block and isinstance(result_block, dict) and result_block.get("items"):
                                    for item_data in result_block.get("items", []):
                                        if not item_data or not isinstance(item_data, dict):
                                            continue
                                            
                                        item_id_for_db = None
                                        payload_for_db = {}
                                        
                                        if search_type == "event":
                                            item_id_for_db = create_event_item_id(item_data)
                                            payload_for_db = {
                                                "event_item_data": item_data,
                                                "api_task_info_context": {
                                                    "result_id": result_id,
                                                    "task_id": task_result.get("id"),
                                                    "keyword": task_detail.get("data", {}).get("keyword"),
                                                    "location_code": task_detail.get("data", {}).get("location_code"),
                                                    "language_code": task_detail.get("data", {}).get("language_code"),
                                                    "tag": result_tag
                                                }
                                            }
                                        elif search_type == "organic":
                                            item_id_for_db = item_data.get("url") or hashlib.md5(str(item_data.get("title", "") + str(item_data.get("description", ""))).encode()).hexdigest()
                                            payload_for_db = {
                                                "organic_item_data": item_data,
                                                "api_task_info_context": {
                                                    "result_id": result_id,
                                                    "task_id": task_result.get("id"),
                                                    "keyword": task_detail.get("data", {}).get("keyword"),
                                                    "location_name": task_detail.get("data", {}).get("location_name"),
                                                    "language_code": task_detail.get("data", {}).get("language_code"),
                                                    "tag": result_tag
                                                }
                                            }
                                        
                                        if not item_id_for_db:
                                            logger.warning(f"Skipping [{search_type.upper()}] item from /id_list task {result_id} for tag '{result_tag}' due to missing ID.")
                                            continue
                                            
                                        if insert_into_event_raw(
                                            conn=db_conn,
                                            source=db_source_string_prefix,
                                            source_event_id=str(item_id_for_db),
                                            metro_id=metro_id,
                                            raw_data_payload=payload_for_db,
                                            script_metadata=script_meta_for_db
                                        ):
                                            items_inserted += 1
                                            
                            if items_inserted > 0:
                                logger.info(f"Successfully inserted {items_inserted} items from task with tag '{result_tag}'")
                                matched_tasks.append(result_id)
                            else:
                                logger.info(f"No new items inserted from task with tag '{result_tag}' (either no items or all duplicates)")
                                
                    except Exception as e:
                        logger.error(f"Error fetching results for task with tag '{result_tag}': {e}")
                        
                    # Sleep between task_get calls
                    time.sleep(REQUEST_DELAY)
                else:
                    logger.info(f"Task with tag '{result_tag}' has no result_id yet, status: {task_result.get('status')}")
        
        if matched_tasks:
            logger.info(f"Successfully retrieved and processed {len(matched_tasks)} tasks from /id_list")
        else:
            logger.info("No matching tasks found in /id_list or no results could be retrieved")
            
        return matched_tasks
            
    except Exception as e:
        logger.error(f"Error getting task results from /id_list: {e}")
        return []

def direct_retrieve_results(result_ids, dataforseo_login, dataforseo_password, db_conn):
    """
    Directly retrieves results using provided task IDs, bypassing the polling mechanism completely.
    This is useful when you can see completed tasks in the dashboard but the polling mechanism isn't working.
    
    Args:
        result_ids: List of task IDs to retrieve (the IDs visible in your DataForSEO dashboard),
                   or the special value "id_list" to get all completed tasks from the last hour
        dataforseo_login: DataForSEO API login
        dataforseo_password: DataForSEO API password
        db_conn: Database connection
    """
    if not result_ids:
        logger.error("No task IDs provided for direct retrieval.")
        return
        
    # Handle the special "id_list" case to get all completed tasks from the last hour
    if len(result_ids) == 1 and result_ids[0].lower() == "id_list":
        logger.info("Using id_list to fetch all completed tasks from the last hour")
        fetch_all_from_id_list(dataforseo_login, dataforseo_password, db_conn, 60)
        return
        
    logger.info(f"Starting direct retrieval for {len(result_ids)} task IDs: {result_ids}")
    
    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}"}
    
    # Always use the events endpoint since that's what we're focusing on
    task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/events/task_get/advanced"
    
    total_items_retrieved = 0
    for task_id in result_ids:
        task_get_url = f"{task_get_endpoint}/{task_id}"
        logger.info(f"Directly retrieving results for task ID {task_id} from {task_get_url}")
        
        try:
            response = requests.get(task_get_url, headers=headers)
            response.raise_for_status()
            task_data = response.json()
            
            if task_data.get("status_code") != 20000:
                logger.error(f"Error retrieving results for task ID {task_id}: {task_data.get('status_code')}: {task_data.get('status_message')}")
                continue
                
            tasks = task_data.get("tasks", [])
            if not tasks or not tasks[0].get("result"):
                logger.warning(f"No results found for task ID {task_id}")
                continue
                
            task_detail = tasks[0]
            result_blocks = task_detail.get("result", [])
            
            # Try to extract metadata from the task
            task_data_obj = task_detail.get("data", {})
            keyword = task_data_obj.get("keyword", "Unknown query")
            location_code = task_data_obj.get("location_code")
            tag = task_data_obj.get("tag", "")
            
            # Try to parse city and search term from keyword
            city_name = "Unknown"
            search_term = "Unknown"
            keyword_parts = keyword.split(" in ", 1)
            if len(keyword_parts) > 1:
                search_term = keyword_parts[0].strip()
                city_name = keyword_parts[1].strip()
            
            # Try to find metro_id from location_code if possible
            metro_id = None
            
            # Context for database
            script_meta_for_db = {
                "dataforseo_api_task_id": task_id,
                "city_name_context": city_name,
                "style_or_term_context": search_term,
                "original_tag": tag,
                "direct_retrieval": True
            }
            
            items_inserted = 0
            for result_block in result_blocks:
                if result_block and isinstance(result_block, dict) and result_block.get("items"):
                    for item_data in result_block.get("items", []):
                        if not item_data or not isinstance(item_data, dict):
                            continue
                            
                        item_id = create_event_item_id(item_data)
                        if not item_id:
                            logger.warning(f"Skipping event item from task {task_id} due to missing ID")
                            continue
                            
                        payload_for_db = {
                            "event_item_data": item_data,
                            "api_task_info_context": {
                                "task_id": task_id,
                                "keyword": keyword,
                                "location_code": location_code,
                                "tag": tag
                            }
                        }
                        
                        if insert_into_event_raw(
                            conn=db_conn,
                            source="dataforseo_event_item_direct",
                            source_event_id=str(item_id),
                            metro_id=metro_id,
                            raw_data_payload=payload_for_db,
                            script_metadata=script_meta_for_db
                        ):
                            items_inserted += 1
            
            if items_inserted > 0:
                logger.info(f"Successfully inserted {items_inserted} items from task ID {task_id}")
                total_items_retrieved += items_inserted
            else:
                logger.warning(f"No new items inserted from task ID {task_id} (either no items or all duplicates)")
                
            time.sleep(REQUEST_DELAY)  # Pause between requests
            
        except Exception as e:
            logger.error(f"Error processing task ID {task_id}: {e}")
    
    logger.info(f"Direct retrieval completed. Total items retrieved: {total_items_retrieved}")

def fetch_all_from_id_list(dataforseo_login, dataforseo_password, db_conn, minutes_ago=60):
    """
    Uses the id_list endpoint to fetch all completed tasks from the specified time period
    and retrieve their results.
    
    Args:
        dataforseo_login: DataForSEO API login
        dataforseo_password: DataForSEO API password
        db_conn: Database connection
        minutes_ago: How many minutes to look back for tasks (default: 60)
    """
    logger.info(f"Fetching all completed tasks from the last {minutes_ago} minutes using id_list...")
    
    auth_header = base64.b64encode(
        f"{dataforseo_login}:{dataforseo_password}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}
    
    # Set date_from to X minutes ago to get recent tasks
    date_from = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    date_from_str = date_from.strftime("%Y-%m-%d %H:%M:%S %z")
    
    # Use the id_list endpoint for SERP API to get all completed tasks
    id_list_endpoint = "https://api.dataforseo.com/v3/serp/id_list"
    payload = [{"date_from": date_from_str}]  # Must be an array of tasks

    try:
        response = requests.post(id_list_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("status_code") != 20000:
            logger.error(f"Error from /id_list: {result.get('status_code')}: {result.get('status_message')}")
            return
            
        tasks = result.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            logger.info("No tasks found in /id_list response.")
            return
            
        id_list_results = tasks[0].get("result", [])
        logger.info(f"Found {len(id_list_results)} tasks in /id_list from the past {minutes_ago} minutes.")
        
        # Filter for just events tasks
        events_tasks = [task for task in id_list_results 
                        if task.get("endpoint") and "events" in task.get("endpoint") 
                        and task.get("status") == "done"]
        
        logger.info(f"Filtered to {len(events_tasks)} completed Google Events tasks")
        
        # Check if we can find any of our submitted task IDs in the results
        # This additional logging will help diagnose issues with task ID matching
        if task_metadata_map:
            our_task_ids = list(task_metadata_map.keys())
            logger.info(f"Checking if any of our {len(our_task_ids)} submitted task IDs match completed tasks")
            
            for our_id in our_task_ids:
                for task in events_tasks:
                    task_id = task.get("id", "")
                    # Check if our task ID is a prefix of a task in the results
                    # This accounts for potential ID format differences
                    if our_id and task_id and our_id.startswith(task_id[:16]):
                        logger.info(f"Found match! Our task ID: {our_id} matches result task ID: {task_id}")
        
        task_ids_to_retrieve = []
        for task in events_tasks:
            task_id = task.get("id")
            if task_id:
                task_ids_to_retrieve.append(task_id)
                logger.info(f"Will retrieve task ID: {task_id}, Tag: {task.get('tag')}, Status: {task.get('status')}")
        
        if task_ids_to_retrieve:
            # Use our direct_retrieve_results function to get the actual results
            direct_retrieve_results(task_ids_to_retrieve, dataforseo_login, dataforseo_password, db_conn)
        else:
            logger.warning("No valid task IDs found to retrieve.")
            
    except Exception as e:
        logger.error(f"Error fetching from id_list: {e}")

def main():
    global task_metadata_map
    
    logger.info("Starting enhanced discovery service...")
    time.sleep(1) # Small delay

    # Check for direct result ID retrieval from command line
    direct_result_ids = []
    if len(sys.argv) > 1:
        if sys.argv[1] == "--result-ids" and len(sys.argv) > 2:
            direct_result_ids = sys.argv[2].split(',')
            logger.info(f"Found {len(direct_result_ids)} result IDs from command line: {direct_result_ids}")
    
    # Create Prometheus metrics for event metrics
    events_found_total = Counter('events_found_total', 'Total number of events found', ['city', 'dance_style'], registry=registry)
    serp_credits_total = Counter('serp_credits_total', 'Total number of SERP credits used', ['endpoint'], registry=registry)

    # Critical check for DataForSEO credentials early
    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        logger.error("CRITICAL: DATAFORSEO_LOGIN or DATAFORSEO_PASSWORD not found in environment. Exiting.")
        return
    else:
        logger.info("DataForSEO credentials loaded successfully.")
    
    # Initialize Redis connection
    redis_client = None 
    redis_available = False
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Successfully connected to Redis.")
        redis_available = True
    except redis.exceptions.ConnectionError as e:
        logger.warning(f"Failed to connect to Redis: {e}. Proceeding without Redis-dependent features.")

    # Initialize Database connection
    db_conn = get_db_connection()
    if not db_conn:
        logger.error("Failed to establish database connection. Exiting.")
        return

    if not create_event_raw_table_if_not_exists(db_conn): # Ensure table exists
        logger.error("Failed to ensure 'event_raw' table exists. Exiting.")
        if db_conn: db_conn.close()
        return
    
    # If direct result IDs were provided for specific task IDs (not the special 'id_list' value),
    # retrieve only those specific results and exit
    if direct_result_ids and not (len(direct_result_ids) == 1 and direct_result_ids[0].lower() == "id_list"):
        logger.info("Using direct result ID retrieval mode for specific task IDs.")
        direct_retrieve_results(direct_result_ids, DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, db_conn)
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")
        return
    
    # Continue with normal workflow
    metros_df = load_metros_from_csv(METRO_CSV_PATH)
    if metros_df.empty:
        logger.error("No metro data loaded. Exiting.")
        if db_conn: db_conn.close()
        return
    
    if 'geonameid' not in metros_df.columns:
        logger.error("'geonameid' column is missing from metros_df. Cannot link to event_raw.metro_id. Exiting.")
        if db_conn: db_conn.close()
        return

    # --- DataForSEO API Workflow ---
    logger.info("--- Starting DataForSEO API Discovery Workflow ---")
    
    # 1. Enrich metros with Google Events specific location_codes
    metros_df_enriched = enrich_metros_with_location_codes(metros_df, DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    
    cities_for_tasks = metros_df_enriched[metros_df_enriched['location_code'].notna()].copy()
    if MAX_CITIES > 0:
        cities_for_tasks = cities_for_tasks.head(MAX_CITIES)
        logger.info(f"Limiting API tasks to {len(cities_for_tasks)} cities with location_codes (due to MAX_CITIES={MAX_CITIES}).")
    else:
        logger.info(f"Processing API tasks for {len(cities_for_tasks)} cities with location_codes.")

    if cities_for_tasks.empty:
        logger.warning("No cities have valid location_codes after enrichment. Cannot proceed with API tasks.")
    else:
        # Define search terms
        broad_event_search_terms = ["salsa", "kizomba", "bachata", "zouk", "coast swing", "ballroom"]
        
        # 2. Submit batch tasks for broad event search terms
        all_task_ids = []
        
        # Track if we want to include organic search - now hardcoded to False effectively
        INCLUDE_ORGANIC_SEARCH = os.getenv("INCLUDE_ORGANIC_SEARCH", "false").lower() == "true"
        # Track if we want to use batch processing
        USE_BATCH_PROCESSING = os.getenv("USE_BATCH_PROCESSING", "true").lower() == "true"
        
        if USE_BATCH_PROCESSING:
            # Clear the global task_metadata_map for fresh use
            task_metadata_map.clear()
            
            # Submit tasks for broad event search
            event_task_ids, event_tags_map = batch_api_tasks(
                cities_for_tasks, 
                broad_event_search_terms, 
                DATAFORSEO_LOGIN, 
                DATAFORSEO_PASSWORD
            )
            all_task_ids = event_task_ids
            
            # Wait longer for tasks to be processed
            wait_seconds = int(os.getenv("TASK_PROCESSING_WAIT", "30"))
            logger.info(f"Waiting {wait_seconds} seconds for submitted tasks to be processed before retrieval...")
            time.sleep(wait_seconds)
            
            # FIRST ATTEMPT: Try to retrieve the specific task IDs we just submitted
            if all_task_ids:
                logger.info(f"First attempting to directly retrieve the {len(all_task_ids)} tasks we just submitted...")
                direct_retrieve_results(all_task_ids, DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, db_conn)
            
            # SECOND ATTEMPT: Use id_list to get all completed tasks from the last hour
            logger.info("Also using id_list to retrieve all completed tasks from the last hour...")
            fetch_all_from_id_list(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, db_conn, 60)
            
            # FALLBACK: Only if specifically requested, try the old polling mechanism
            # This is kept only for compatibility and may be removed in the future
            if os.getenv("USE_LEGACY_POLLING", "false").lower() == "true" and all_task_ids:
                logger.info(f"FALLBACK: Also trying legacy polling for {len(all_task_ids)} tasks...")
                poll_task_results(task_metadata_map, DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, db_conn)
        else:
            # Sequential processing without batching
            logger.info("Using sequential processing mode (no batching).")
            for index, city_info in cities_for_tasks.iterrows():
                city_name = city_info['name']
                metro_id = city_info['geonameid']
                
                for term in broad_event_search_terms: # This loop will run for "dance" and "dancing"
                    all_urls, raw_api_response = get_dataforseo_results_for_dance_style(
                        city_info, term, DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, 
                        redis_client, db_conn, redis_available
                    )
                    logger.info(f"Found {len(all_urls)} URLs for '{term}' in {city_name}")
                    
                    # Update metrics
                    events_found_total.labels(city=city_name, dance_style=term).inc(len(all_urls))
                    serp_credits_total.labels(endpoint="events/live/advanced").inc(1)
    
    logger.info("--- DataForSEO API Discovery Workflow Finished ---")

    if db_conn:
        db_conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main() 