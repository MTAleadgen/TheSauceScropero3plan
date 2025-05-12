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

MEETUP_API_KEY = os.getenv("MEETUP_OAUTH_TOKEN") # Assuming OAuth2 Token, rename if using API Key
DATABASE_URL = os.getenv("DATABASE_URL")
METRO_CSV_PATH = "geonames_na_sa_eu_top1785.csv" # Updated base path
REQUEST_DELAY = 1.5 # Delay between Meetup API calls
MEETUP_GRAPHQL_URL = "https://api.meetup.com/gql"
DB_TABLE = "event_raw"
SOURCE_NAME = "meetup"

# Define dance style keywords for text search (might need adjustment for Meetup)
DANCE_KEYWORDS = [
    "Salsa", "Bachata", "Kizomba", "Zouk", "Tango", "Swing", "West Coast Swing",
    "Lindy Hop", "Blues dance", "Fusion dance", "Contact Improvisation", 
    "Ecstatic Dance", "Latin dance", "Ballroom dance", "Contra Dance", "Square Dance", "dance class", "dance social"
]

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions (Reusing from eventbrite collector logic) ---
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

# --- Meetup GraphQL API Functions ---

def build_meetup_query(keywords, lat, lon, radius_miles=30, first=50, after_cursor=None):
    """Constructs the GraphQL query string for Meetup event search."""
    # Combine keywords into a search string suitable for Meetup
    topic_string = " OR ".join(keywords)
    
    # Basic query structure - Needs refinement based on Meetup Schema exploration!
    # This is a conceptual example.
    query = """
    query($lat: Float!, $lon: Float!, $radius: Int!, $topic: String!, $first: Int!, $after: String) {
      keywordSearch(filter: { query: $topic, lat: $lat, lon: $lon, radius: $radius, source: EVENTS }, first: $first, after: $after) {
        count
        pageInfo {
          endCursor
          hasNextPage
        }
        edges {
          node {
            result {
              ... on Event {
                id
                title
                description
                dateTime
                endTime
                duration
                eventUrl
                venue {
                  id
                  name
                  address
                  city
                  state
                  country
                  lat
                  lon
                }
                group {
                  id
                  name
                  urlname
                  # Add other group fields if needed
                }
                # Add other event fields like timezone, hosts, fees, etc. as needed
              }
            }
          }
        }
      }
    }
    """
    variables = {
        "lat": lat,
        "lon": lon,
        "radius": radius_miles,
        "topic": topic_string, 
        "first": first
    }
    if after_cursor:
        variables["after"] = after_cursor
        
    return query, variables

def fetch_meetup_events(api_key, graphql_url, variables, query):
    """Fetches events from Meetup GraphQL API."""
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        response = requests.post(graphql_url, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Meetup API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status: {e.response.status_code}, Response text: {e.response.text[:200]}")
        return None
    except Exception as e:
        logging.error(f"Error processing Meetup response: {e}")
        return None

# --- Main Logic ---
def main():
    if not MEETUP_API_KEY:
        logging.error("MEETUP_OAUTH_TOKEN environment variable not set. Exiting.")
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

        # --- Meetup API Call for this metro --- 
        after_cursor = None
        has_next_page = True
        pages_fetched = 0
        max_pages = 3 # Limit pagination per metro

        while has_next_page and pages_fetched < max_pages:
            pages_fetched += 1
            query, variables = build_meetup_query(DANCE_KEYWORDS, latitude, longitude, after_cursor=after_cursor)
            data = fetch_meetup_events(MEETUP_API_KEY, MEETUP_GRAPHQL_URL, variables, query)
            
            if not data or "errors" in data:
                logging.error(f"GraphQL query failed for metro {metro_id}. Response: {data}")
                break # Stop processing this metro

            search_results = data.get("data", {}).get("keywordSearch", {})
            if not search_results:
                logging.warning(f"No 'keywordSearch' results in response for metro {metro_id}. Response: {data}")
                break

            events = search_results.get("edges", [])
            total_events_processed += len(events)

            for edge in events:
                event_node = edge.get("node", {}).get("result", {})
                if not event_node or event_node.get("__typename") != "Event":
                    continue

                source_id = event_node.get('id')
                if not source_id:
                    logging.warning("Found Meetup event without ID, skipping.")
                    continue
                    
                if insert_raw_event(conn, SOURCE_NAME, source_id, event_node, metro_id):
                    total_events_inserted += 1
                else:
                    logging.warning(f"Failed to insert Meetup event {source_id} for metro {metro_id}")
            
            # Pagination
            page_info = search_results.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            if not has_next_page:
                 logging.debug(f"No more pages for Meetup query on metro {metro_id}.")
            
            time.sleep(REQUEST_DELAY) # Delay between pages/requests
        # --- End Meetup API Call for this metro --- 

    logging.info("Meetup collection completed.")
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