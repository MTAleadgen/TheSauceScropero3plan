import os
import requests
import psycopg2
import json
import time
import logging
import pandas as pd
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

EVENTBRITE_API_TOKEN = os.getenv("EVENTBRITE_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
METRO_CSV_PATH = "geonames_na_sa_eu_top1785.csv" # Updated base path
REQUEST_DELAY = 1.0 # Delay between Eventbrite API calls
EVENTBRITE_API_URL = "https://www.eventbriteapi.com/v3/events/search/"
DB_TABLE = "event_raw"
SOURCE_NAME = "eventbrite"

# Define relevant Eventbrite category IDs (can be expanded)
# Find IDs: https://www.eventbrite.com/platform/api#/v3/call/get_categories
# Examples:
# 103: Music
# 105: Performing & Visual Arts -> Dance might be here
# 119: Hobbies -> Dance classes
# 108: Health & Wellness -> Ecstatic dance?
EVENTBRITE_CATEGORIES = ["103", "105", "119", "108"]

# Define dance style keywords for text search
DANCE_KEYWORDS = [
    "Salsa", "Bachata", "Kizomba", "Zouk", "Tango", "Swing", "West Coast Swing",
    "Lindy Hop", "Blues dance", "Fusion dance", "Contact Improvisation", 
    "Ecstatic Dance", "Latin dance", "Ballroom dance", "Contra Dance", "Square Dance", "dance class", "dance social"
]

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logging.info("Database connection established.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None

def insert_raw_event(conn, source, source_id, event_data, metro_id=None):
    """Inserts raw event JSON into the database."""
    sql = f"""
        INSERT INTO {DB_TABLE} (source, source_event_id, raw_json, metro_id, discovered_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (source, source_event_id) DO NOTHING;
        """
    try:
        with conn.cursor() as cur:
            # Convert event_data dict to JSON string
            raw_json_str = json.dumps(event_data)
            cur.execute(sql, (source, source_id, raw_json_str, metro_id))
        conn.commit()
        return True
    except psycopg2.Error as e:
        logging.error(f"Database insert error for source_id {source_id}: {e}")
        conn.rollback() # Rollback the transaction on error
        return False
    except Exception as e:
        logging.error(f"Error converting event data to JSON for source_id {source_id}: {e}")
        conn.rollback()
        return False

# --- Eventbrite API Functions ---
def fetch_eventbrite_events(api_token, parameters):
    """Fetches events from Eventbrite API with pagination handling."""
    headers = {"Authorization": f"Bearer {api_token}"}
    all_events = []
    page = 1
    max_pages = 5 # Limit pagination to avoid excessive calls

    while page <= max_pages:
        params = parameters.copy()
        params['page'] = page
        try:
            response = requests.get(EVENTBRITE_API_URL, headers=headers, params=params)
            response.raise_for_status() # Raise HTTPError for bad responses
            data = response.json()
            events = data.get("events", [])
            if not events:
                logging.debug(f"No more events found on page {page} for params: {params}")
                break # No more events on this page
            
            all_events.extend(events)
            logging.info(f"Fetched {len(events)} events from page {page} for params: {params.get('location.address')}")

            # Check pagination
            pagination = data.get("pagination", {})
            if not pagination.get("has_more_items", False):
                logging.debug("No more items indicated by pagination.")
                break # No more pages

            page += 1
            time.sleep(REQUEST_DELAY)

        except requests.exceptions.RequestException as e:
            logging.error(f"Eventbrite API request failed (page {page}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response status: {e.response.status_code}, Response text: {e.response.text[:200]}")
            time.sleep(REQUEST_DELAY * 3) # Longer delay on error
            break # Stop pagination for this query on error
        except Exception as e:
            logging.error(f"Error processing Eventbrite response (page {page}): {e}")
            break
            
    return all_events

# --- Main Logic ---
def main():
    if not EVENTBRITE_API_TOKEN:
        logging.error("EVENTBRITE_API_TOKEN environment variable not set. Exiting.")
        return
    if not DATABASE_URL:
        logging.error("DATABASE_URL environment variable not set. Exiting.")
        return

    conn = get_db_connection()
    if not conn:
        return

    # Load metro data
    try:
        metros_df = pd.read_csv(METRO_CSV_PATH)
        if 'geonameid' not in metros_df.columns or 'latitude' not in metros_df.columns or 'longitude' not in metros_df.columns:
             logging.error(f"CSV file '{METRO_CSV_PATH}' missing required columns ('geonameid', 'latitude', 'longitude'). Exiting.")
             return
        logging.info(f"Loaded {len(metros_df)} metros from {METRO_CSV_PATH}")
    except FileNotFoundError:
        logging.error(f"Metro CSV file not found at '{METRO_CSV_PATH}'. Exiting.")
        return
    except Exception as e:
        logging.error(f"Error reading metro CSV file: {e}. Exiting.")
        return

    total_events_processed = 0
    total_events_inserted = 0

    # Iterate through metros
    for index, metro in metros_df.iterrows():
        metro_id = metro['geonameid']
        latitude = metro['latitude']
        longitude = metro['longitude']
        metro_name = metro['name']
        logging.info(f"Processing metro: {metro_name} ({metro_id}) ({index + 1}/{len(metros_df)})")
        
        # Construct parameters for Eventbrite Search
        # Search by location (lat/lon), categories, and keywords
        base_params = {
            "location.latitude": latitude,
            "location.longitude": longitude,
            "location.within": "50km", # Search radius (adjust as needed)
            "categories": ",".join(EVENTBRITE_CATEGORIES),
            "q": " OR ".join(DANCE_KEYWORDS), # Combine keywords
            "start_date.range_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), # Search from now
            "expand": "venue,organizer,ticket_availability" # Get more details
        }

        events = fetch_eventbrite_events(EVENTBRITE_API_TOKEN, base_params)
        total_events_processed += len(events)

        for event in events:
            source_id = event.get('id')
            if not source_id:
                logging.warning("Found event without ID, skipping.")
                continue
                
            if insert_raw_event(conn, SOURCE_NAME, source_id, event, metro_id):
                total_events_inserted += 1
            else:
                logging.warning(f"Failed to insert event {source_id} for metro {metro_id}")
        
        # Optional: Add a delay between metros if needed
        # time.sleep(REQUEST_DELAY)

    logging.info("Eventbrite collection completed.")
    logging.info(f"Total events processed: {total_events_processed}")
    logging.info(f"Total unique events inserted/updated: {total_events_inserted}")

    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    # Make sure the CSV path is accessible
    # If running in Docker, METRO_CSV_PATH needs to be the path INSIDE the container
    if not os.path.exists(METRO_CSV_PATH):
        # Attempt to find it relative to the script directory if not at root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        alt_path = os.path.join(script_dir, '..', METRO_CSV_PATH) # Assumes script is in services/ directory
        if os.path.exists(alt_path):
            METRO_CSV_PATH = alt_path
        else:
             # If run from docker-compose context, ../ might not work as expected.
             # Rely on the volume mount path provided in docker-compose.yml which is /app/geonames... 
             container_path = "/app/geonames_na_sa_eu_top1785.csv" # Updated container path filename
             if os.path.exists(container_path):
                 METRO_CSV_PATH = container_path
             else:
                logging.warning(f"Default CSV path {METRO_CSV_PATH} and alternative path {alt_path} not found.")
                logging.warning(f"Attempting container path {container_path}")
                # Proceed, hoping the container_path exists
                METRO_CSV_PATH = container_path
                if not os.path.exists(METRO_CSV_PATH):
                     logging.error(f"Cannot find metro data CSV at expected paths. Exiting.")
                     exit()

    main() 