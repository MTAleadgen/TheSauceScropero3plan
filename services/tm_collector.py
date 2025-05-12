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

TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
METRO_CSV_PATH = "geonames_na_sa_eu_top1785.csv" # Updated base path
REQUEST_DELAY = 0.5 # Ticketmaster rate limits are often per second
TICKETMASTER_API_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
DB_TABLE = "event_raw"
SOURCE_NAME = "ticketmaster"

# Define relevant Ticketmaster Classification IDs or keywords
# Find classifications: https://developer.ticketmaster.com/products-and-docs/apis/discovery/v2/#search-events-v2
# Examples:
# KZFZCstvk1ZF6akje: Dance/Electronic Music
# KZFZCstvk1ZF6akjC: Arts & Theatre -> Dance
TM_CLASSIFICATION_IDS = ["KZFZCstvk1ZF6akjC", "KZFZCstvk1ZF6akje"]

# Define dance style keywords for text search
DANCE_KEYWORDS = [
    "Salsa", "Bachata", "Kizomba", "Zouk", "Tango", "Swing", "West Coast Swing",
    "Lindy Hop", "Blues dance", "Fusion dance", "Contact Improvisation", 
    "Ecstatic Dance", "Latin dance", "Ballroom dance", "Contra Dance", "Square Dance", "dance class", "dance social"
]

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions (Reusing logic) ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logging.info("Database connection established.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None

def insert_raw_event(conn, source, source_id, event_data, metro_id=None):
    sql = f"""
        INSERT INTO {DB_TABLE} (source, source_event_id, raw_json, metro_id, discovered_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (source, source_event_id) DO NOTHING;
        """
    try:
        with conn.cursor() as cur:
            raw_json_str = json.dumps(event_data)
            cur.execute(sql, (source, source_id, raw_json_str, metro_id))
        conn.commit()
        return True
    except psycopg2.Error as e:
        logging.error(f"Database insert error for source_id {source_id}: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"Error converting event data to JSON for source_id {source_id}: {e}")
        conn.rollback()
        return False

# --- Ticketmaster API Functions ---
def fetch_ticketmaster_events(api_key, parameters):
    """Fetches events from Ticketmaster Discovery API with pagination."""
    all_events = []
    page = 0 # Ticketmaster uses 0-based page numbering
    max_pages = 5 # Limit pagination

    while page < max_pages:
        params = parameters.copy()
        params['apikey'] = api_key
        params['page'] = page
        params['size'] = 50 # Request a reasonable number of results per page (max 200)

        try:
            response = requests.get(TICKETMASTER_API_URL, params=params)
            response.raise_for_status() # Raise HTTPError for bad responses
            data = response.json()
            
            events = data.get("_embedded", {}).get("events", [])
            if not events:
                logging.debug(f"No more events found on page {page} for params: {params.get('geoPoint')}")
                break
            
            all_events.extend(events)
            logging.info(f"Fetched {len(events)} events from page {page} for geoPoint: {params.get('geoPoint')}")

            # Check pagination (if totalPages is available and current page exceeds it)
            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", max_pages) # Use max_pages if not provided
            if page >= total_pages - 1:
                logging.debug("Reached last page indicated by API.")
                break

            page += 1
            time.sleep(REQUEST_DELAY) # Adhere to rate limits

        except requests.exceptions.RequestException as e:
            logging.error(f"Ticketmaster API request failed (page {page}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response status: {e.response.status_code}, Response text: {e.response.text[:200]}")
            time.sleep(REQUEST_DELAY * 5) # Longer delay on error
            break # Stop pagination for this query on error
        except Exception as e:
            logging.error(f"Error processing Ticketmaster response (page {page}): {e}")
            break
            
    return all_events

# --- Main Logic ---
def main():
    if not TICKETMASTER_API_KEY:
        logging.error("TICKETMASTER_API_KEY environment variable not set. Exiting.")
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
            logging.error(f"CSV file '{METRO_CSV_PATH}' missing required columns. Exiting.")
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

        # Construct parameters for Ticketmaster Search
        geo_point = f"{latitude},{longitude}"
        base_params = {
            "geoPoint": geo_point,
            "radius": "50", # Radius in miles/km depends on unit param
            "unit": "km",
            "classificationId": TM_CLASSIFICATION_IDS, # List or comma-separated string
            "keyword": " OR ".join(DANCE_KEYWORDS), # Combine keywords
            "startDateTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), # Search from now
            "sort": "date,asc"
        }

        events = fetch_ticketmaster_events(TICKETMASTER_API_KEY, base_params)
        total_events_processed += len(events)

        for event in events:
            source_id = event.get('id')
            if not source_id:
                logging.warning("Found Ticketmaster event without ID, skipping.")
                continue
                
            if insert_raw_event(conn, SOURCE_NAME, source_id, event, metro_id):
                total_events_inserted += 1
            else:
                logging.warning(f"Failed to insert Ticketmaster event {source_id} for metro {metro_id}")
        
        # Optional: Add a delay between metros if needed
        # time.sleep(REQUEST_DELAY * 2) # Potentially longer delay between metros

    logging.info("Ticketmaster collection completed.")
    logging.info(f"Total events processed: {total_events_processed}")
    logging.info(f"Total unique events inserted/updated: {total_events_inserted}")

    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    # Make sure the CSV path is accessible (similar check as eventbrite collector)
    if not os.path.exists(METRO_CSV_PATH):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        alt_path = os.path.join(script_dir, '..', METRO_CSV_PATH) # This will use the updated filename
        container_path = "/app/geonames_na_sa_eu_top1785.csv"   # Updated container path filename
        if os.path.exists(alt_path):
            METRO_CSV_PATH = alt_path
        elif os.path.exists(container_path):
            METRO_CSV_PATH = container_path
        else:
            logging.error(f"Cannot find metro data CSV at expected paths. Exiting.")
            exit()
            
    main() 