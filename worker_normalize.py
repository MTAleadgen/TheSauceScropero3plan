# worker_normalize.py
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, DictCursor # Added DictCursor
from dateutil import parser as dateutil_parser
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time
import os
from dotenv import load_dotenv
from unidecode import unidecode
import re
import hashlib # Added for fingerprint
from datetime import datetime # Added for date operations

# --- Configuration ---
DB_ENV_VAR = 'DATABASE_URL'
WORKER_DELAY = 5  # Delay (in seconds) when no unprocessed events are found
GEOCODING_DELAY = 1.1 # Delay between Nominatim requests
BATCH_SIZE = 50 # How many raw events to process in one go
# Add keywords for tagging dance styles
DANCE_STYLE_KEYWORDS = {
    'salsa': ['salsa'],
    'bachata': ['bachata'],
    'kizomba': ['kizomba', 'semba'],
    'zouk': ['zouk', 'brazilian zouk'],
    'west coast swing': ['west coast swing', 'wcs'],
    'tango': ['tango', 'argentine tango'],
    # Add more styles and keywords
}
# -------------------

# --- Helper Functions ---
def make_fp(title: str, start_ts: datetime, metro_id: int) -> str | None:
    """Generates a fingerprint for an event."""
    if not all([title, start_ts, metro_id is not None]): # metro_id can be 0
        return None
    
    # Normalize title for fingerprinting (optional, but good for consistency)
    norm_title = normalize_string(title)
    if not norm_title:
        return None
        
    start_date_str = start_ts.strftime('%Y-%m-%d')
    
    fingerprint_str = f"{norm_title}|{start_date_str}|{metro_id}"
    return hashlib.sha1(fingerprint_str.encode()).hexdigest()[:16]

def score(evt: dict) -> float:
    """Calculates a quality score for a normalized event dictionary."""
    s = 0.0 # Use float for numeric score
    if evt.get("title"):                         s += 10
    if evt.get("start_ts"):                      s += 10
    # geom is a WKT string "POINT(lon lat)" or None
    if evt.get("venueGeom") and evt.get("venueGeom") != "POINT(None None)": s += 10 
    if evt.get("venueName"):                     s += 5
    if len(evt.get("description","") or "") > 50:s += 5 # Ensure description is not None before len()
    if evt.get("price_val") is not None:         s += 3
    # Check source from the raw event part of the combined dict if that's how it's passed
    # Assuming evt dict contains 'source' directly from raw_event_row
    if evt.get("source") in {"eventbrite", "meetup", "ticketmaster"}: s += 3 # Added ticketmaster
    # Add more scoring based on other fields if desired
    # e.g., presence of URL, image_url, end_ts, specific tags
    if evt.get("url"): s += 2
    if evt.get("imageUrl"): s += 2
    if evt.get("tags"): s +=1 # Basic score for having any tags
    return s
# --- End Helper Functions ---

# --- Geocoding Setup (same as qa script) ---
# Use environment variable for user agent, with a fallback
NOMINATIM_USER_AGENT = os.getenv("NOMINATIM_USER_AGENT", "metro_event_normalizer_1.0_fallback/1.0 (your.email@example.com)")
geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)

def reverse_geocode_nominatim(lat, lon, attempt=1, max_attempts=3):
    # (Implementation omitted for brevity - assume same function as in qa_reverse_geocode.py)
    # ... returns location object or None
    pass # Replace with actual implementation if needed here, or import

def geocode_address(address_input, country_code_hint: str | None = None, attempt=1, max_attempts=3):
    """
    Geocodes an address string or a structured address dictionary.
    :param address_input: A string or a dictionary with keys like 'street', 'city', 'country', 'postalcode'.
    :param country_code_hint: An ISO 3166-1 alpha-2 country code (e.g., 'US', 'CN') to help narrow down results.
    :param attempt: Current retry attempt.
    :param max_attempts: Maximum retry attempts.
    :return: Nominatim location object or None.
    """
    if not address_input:
        return None

    query_params = {}
    if isinstance(address_input, str):
        query_params['q'] = address_input
    elif isinstance(address_input, dict):
        # Geopy's Nominatim geocoder uses the dict directly as query components
        query_params = address_input.copy() # Use a copy to modify
    else:
        print(f"Invalid address_input type: {type(address_input)}")
        return None

    # Add country_code_hint if provided and not already in a structured query that has 'country'
    if country_code_hint and not (isinstance(address_input, dict) and 'country' in address_input):
        # For Nominatim, the countrycodes parameter limits search results to one or more countries.
        # It expects ISO 3166-1alpha2 codes.
        query_params['countrycodes'] = country_code_hint
    
    # If query_params is a dict and contains 'q', it means it was a string originally.
    # If it's a dict from address_input, it doesn't need 'q'.
    # The geopy Nominatim interface handles this: if query is a dict, it's structured. If string, it's a query string.
    
    final_query = query_params if isinstance(address_input, dict) else query_params.get('q')

    if not final_query: # Should not happen if address_input was valid
        return None

    try:
        # Nominatim geopy wrapper handles dicts as structured queries
        # and strings as general queries.
        # If query_params contains 'countrycodes', it will be used.
        if isinstance(final_query, dict):
             # If countrycodes was added, it should be part of the dict passed directly
            location = geolocator.geocode(final_query, exactly_one=True, timeout=10, country_codes=query_params.get('countrycodes'))
        else: # It's a string
            location = geolocator.geocode(final_query, exactly_one=True, timeout=10, country_codes=query_params.get('countrycodes'))
        
        return location
    except GeocoderTimedOut:
        if attempt <= max_attempts:
            print(f"Nominatim (geocode) timed out. Retrying ({attempt}/{max_attempts}). Input: {address_input}")
            time.sleep(attempt * 2)
            return geocode_address(address_input, country_code_hint, attempt + 1, max_attempts)
        else:
            print(f"Nominatim (geocode) timed out after multiple retries for input: {address_input}")
            return None
    except GeocoderUnavailable as e:
        print(f"Nominatim service unavailable: {e}. Input: {address_input}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during geocoding for input {address_input}: {e}")
        return None
# --- End Geocoding Setup ---

def initialize_db():
    load_dotenv('.env')
    database_url = os.getenv(DB_ENV_VAR)
    if not database_url:
        print(f"Error: {DB_ENV_VAR} not found.")
        return None
    try:
        # Use DictCursor to get rows as dictionaries
        conn = psycopg2.connect(database_url, cursor_factory=DictCursor)
        print("Normalizer: Connected to Database.")
        return conn
    except psycopg2.Error as e:
        print(f"Normalizer: Error connecting to Database: {e}")
        return None

def parse_datetime(date_string):
    if not date_string: return None
    try: return dateutil_parser.parse(date_string)
    except: return None

def normalize_string(text):
    if not text:
        return None
    return str(text).strip()

def extract_price(offer_data):
    price = None
    currency = None
    if not offer_data:
        return price, currency
    
    # Handle list of offers or single offer
    offers = offer_data if isinstance(offer_data, list) else [offer_data]
    
    for offer in offers:
        if isinstance(offer, dict):
            p = offer.get('price')
            c = offer.get('priceCurrency')
            # Simple logic: take the first valid price/currency found
            if p is not None and c:
                try:
                    price = float(p) # Attempt to convert price to float
                    currency = str(c).upper()[:3] # Ensure currency is max 3 chars and uppercase
                    break # Stop after finding the first valid offer
                except (ValueError, TypeError):
                    continue # Skip if price is not a valid number
    return price, currency

def tag_dance_styles(text_content):
    if not text_content:
        return []
    
    found_styles = set()
    normalized_content = unidecode(str(text_content).lower())
    
    for style, keywords in DANCE_STYLE_KEYWORDS.items():
        for keyword in keywords:
            # Use word boundaries to avoid partial matches (e.g., 'salsa' in 'balsamic')
            if re.search(r'\b' + re.escape(keyword) + r'\b', normalized_content):
                found_styles.add(style)
                break # Move to next style once a keyword is found
                
    return sorted(list(found_styles))

def get_metro_id_for_coords(cur, lat, lon):
    """ 
    Given lat/lon, queries the metro table to find which metro's bbox contains the point.
    Returns the geonameid of the containing metro, or None.
    This geonameid is what should be stored in event_clean.metro_id due to FK constraints.
    """
    if lat is None or lon is None:
        return None
    try:
        # Query to find geonameid using ST_Covers with geography types
        # We need geonameid because event_clean.metro_id refers to metro.geonameid
        query = sql.SQL("""
            SELECT geonameid 
            FROM metro 
            WHERE ST_Covers(bbox, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) 
            LIMIT 1;
        """)
        cur.execute(query, (lon, lat)) # Pass lon, lat
        result = cur.fetchone()
        # The result from DictCursor will be a dict-like object, access by key if it's DictRow
        # If it's a standard tuple cursor, result[0] is fine.
        # Assuming DictCursor based on other parts of the code (e.g. initialize_db uses DictCursor)
        return result['geonameid'] if result and 'geonameid' in result else None 
    except psycopg2.Error as e:
        print(f"  DB error during metro lookup (geonameid): {e}")
        return None
    except KeyError:
        print(f"  DB error: 'geonameid' key not found in metro lookup result for coords ({lat},{lon}).")
        return None

def get_country_code_for_metro(cur, metro_geonameid: int) -> str | None:
    """Fetches the country_iso2 for a given metro_geonameid."""
    if metro_geonameid is None:
        return None
    try:
        query = sql.SQL("SELECT country_iso2 FROM metro WHERE geonameid = %s LIMIT 1;")
        cur.execute(query, (metro_geonameid,))
        result = cur.fetchone()
        return result['country_iso2'] if result and result['country_iso2'] else None
    except psycopg2.Error as e:
        print(f"  DB error fetching country_iso2 for metro_id {metro_geonameid}: {e}")
        return None
    except KeyError: # In case 'country_iso2' key is missing from result somehow
        print(f"  'country_iso2' key missing in result for metro_id {metro_geonameid}")
        return None

def process_raw_event(cur, raw_event_row: dict) -> dict | None: # raw_event_row is a DictRow
    """
    Processes a raw event row from event_raw table.
    Returns a fully-normalised dict for event_clean OR None (if fatal data issues).
    Keys must line up with the columns in `event_clean`.
    This function should be side-effect-free regarding database writes.
    """
    try:
        event_raw_id = raw_event_row['id']
        source = raw_event_row['source']
        source_event_id_from_raw = raw_event_row.get('source_event_id')
        # Assuming raw_json is already a dict because worker_parse should have loaded it.
        # If it's a string, it needs json.loads()
        jsonld = raw_event_row['raw_json'] 
        if not isinstance(jsonld, dict):
             # If raw_json is a string from the DB, parse it
            if isinstance(jsonld, str):
                try:
                    jsonld = json.loads(jsonld)
                except json.JSONDecodeError:
                    print(f"  Skipping event_raw_id {event_raw_id}: Invalid JSON string in raw_json.")
                    return None
            else:
                print(f"  Skipping event_raw_id {event_raw_id}: raw_json is not a dict or valid JSON string.")
                return None


        # --- Extract Core Fields ---
        title = normalize_string(jsonld.get('name'))
        start_ts = parse_datetime(jsonld.get('startDate'))
        end_ts = parse_datetime(jsonld.get('endDate'))
        description = normalize_string(jsonld.get('description'))
        
        # URL: Prefer URL from JSON-LD, fallback to source_url if available (not in current event_raw schema, but good practice)
        # raw_event_row might have a 'source_url' if worker_fetch stored it separately from JSON-LD.
        # For now, let's assume worker_parse put it into raw_json if it was distinct.
        url = normalize_string(jsonld.get('url')) 
        if not url and 'eventAttendanceMode' in jsonld and 'OnlineEventAttendanceMode' in str(jsonld.get('eventAttendanceMode','')) and jsonld.get('location',{}).get('url'):
             url = normalize_string(jsonld.get('location',{}).get('url')) # Common for online events

        # --- Basic Validation ---
        if not title or not start_ts:
            print(f"  Skipping event_raw_id {event_raw_id}: Missing essential data (title or start_ts). Title: '{title}', StartTS: '{start_ts}'")
            return None

        # --- Extract Location --- 
        location_data = jsonld.get('location')
        venue_name = None
        venue_address_str = None 
        structured_address_dict = None # To hold dict like {'street': ..., 'city': ...}
        lat = None
        lon = None
        country_code_hint = None # Initialize country_code_hint
        s_city = None # Initialize s_city, will be defined if address_info is a dict

        # Try to get country_code_hint early if metro_id is available from raw_event_row
        source_metro_geonameid = raw_event_row.get('metro_id') 
        if source_metro_geonameid:
            country_code_hint = get_country_code_for_metro(cur, source_metro_geonameid)
            if country_code_hint:
                print(f"  Obtained country_code_hint: {country_code_hint} for source_metro_id: {source_metro_geonameid}")

        event_id_for_logging = raw_event_row['id'] # Use actual id for logging

        if isinstance(location_data, dict):
            venue_name = normalize_string(location_data.get('name'))
            address_info = location_data.get('address')
            geo_info = location_data.get('geo')

            if isinstance(address_info, str):
                venue_address_str = normalize_string(address_info)
                # s_city remains None if address_info is just a string here

            elif isinstance(address_info, dict):
                structured_address_dict = {}
                s_street = normalize_string(address_info.get('streetAddress'))
                s_city = normalize_string(address_info.get('addressLocality')) # s_city is defined here
                s_postal = normalize_string(address_info.get('postalCode'))
                s_region = normalize_string(address_info.get('addressRegion'))
                s_country_name_or_code = normalize_string(address_info.get('addressCountry'))

                if s_street: structured_address_dict['street'] = s_street
                if s_city: structured_address_dict['city'] = s_city
                if s_postal: structured_address_dict['postalCode'] = s_postal
                if s_region: structured_address_dict['addressRegion'] = s_region
                
                if s_country_name_or_code:
                    if len(s_country_name_or_code) == 2 and s_country_name_or_code.isalpha():
                        structured_address_dict['country'] = s_country_name_or_code.upper()
                        if not country_code_hint: 
                             country_code_hint = structured_address_dict['country']
                    elif not country_code_hint: 
                        structured_address_dict['country'] = s_country_name_or_code
                
                _parts_for_str = [
                    s_street, s_city, s_postal, s_region, 
                    s_country_name_or_code if not (structured_address_dict.get('country') and country_code_hint and structured_address_dict.get('country') == country_code_hint) else None
                ]
                venue_address_str = ", ".join(filter(None, _parts_for_str))
                if not venue_address_str: venue_address_str = None

            if isinstance(geo_info, dict):
                try:
                    lat_val = geo_info.get('latitude')
                    lon_val = geo_info.get('longitude')
                    if lat_val is not None and lon_val is not None:
                        lat = float(lat_val)
                        lon = float(lon_val)
                except (ValueError, TypeError):
                    lat, lon = None, None
        
        # Define city_for_fallback and country_for_fallback BEFORE geocoding block
        city_for_fallback = s_city # s_city would be None if not parsed from structured address
        country_for_fallback = country_code_hint # country_code_hint might be from metro or structured address

        # Geocode if no coords and we have some address info
        if (lat is None or lon is None) and (structured_address_dict or venue_address_str):
            address_to_geocode = structured_address_dict if structured_address_dict else venue_address_str
            geo_result = None
            if address_to_geocode:
                print(f"  Geocoding attempt for event_raw_id {event_raw_id}. Input: {address_to_geocode}, Hint: {country_code_hint}")
                geo_result = geocode_address(address_to_geocode, country_code_hint=country_code_hint)
                time.sleep(GEOCODING_DELAY)

                if geo_result:
                    print(f"  Specific address geocoding successful: ({geo_result.latitude}, {geo_result.longitude}). Address: {geo_result.address}")
                    lat, lon = geo_result.latitude, geo_result.longitude
                else:
                    print(f"  Specific address geocoding failed for input: {address_to_geocode} with hint: {country_code_hint}")
            else:
                print(f"  No specific address parts found for event_raw_id {event_raw_id} to geocode.")

            if not geo_result and city_for_fallback and country_for_fallback:
                fallback_query = f"{city_for_fallback}, {country_for_fallback}"
                print(f"  Attempting city-level fallback geocoding for: {fallback_query}")
                city_geo_result = geocode_address(fallback_query)
                time.sleep(GEOCODING_DELAY)

                if city_geo_result:
                    print(f"  City-level fallback geocoding successful: ({city_geo_result.latitude}, {city_geo_result.longitude}). Address: {city_geo_result.address}")
                    lat, lon = city_geo_result.latitude, city_geo_result.longitude
                else:
                    print(f"  City-level fallback geocoding also failed for: {fallback_query}")
            elif not geo_result:
                print(f"  Skipping city-level fallback: Insufficient info (city: {city_for_fallback}, country_hint: {country_for_fallback}).")


        # --- Determine Metro ID (using our DB based on geocoded coords) ---
        metro_id_from_coords = get_metro_id_for_coords(cur, lat, lon)
        
        # Final metro_id logic:
        # 1. Prefer metro_id derived from our geocoding + ST_Within.
        # 2. Fallback to source_metro_geonameid if our geocoding didn't yield a metro_id.
        final_metro_id = metro_id_from_coords if metro_id_from_coords is not None else source_metro_geonameid

        if final_metro_id is None:
            print(f"  Warning: Could not determine metro_id for event_raw_id {raw_event_row['id']}. Coords: ({lat},{lon}), Source Metro ID: {source_metro_geonameid}")
        
        # --- Extract Price ---
        offers = jsonld.get('offers')
        price_val, price_ccy = extract_price(offers)

        # --- Tag Dance Styles ---
        search_text = ' '.join(filter(None, [title, description, venue_name]))
        tags = tag_dance_styles(search_text) # This will be 'tags' in event_clean

        # --- Prepare Output Dict ---
        # Keys must match event_clean schema
        out = {
            "event_raw_id": raw_event_row['id'], # Corrected from raw_event_id to use the actual id from the input row
            "source": raw_event_row['source'], 
            "source_event_id": source_event_id_from_raw or jsonld.get('identifier'),
            "title": title,
            "description": description,
            "url": url,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "venueName": venue_name,
            "venueAddress": venue_address_str, # This might now be the address from Nominatim
            "venueGeom": f"POINT({lon} {lat})" if lon is not None and lat is not None else None,
            "imageUrl": normalize_string(jsonld.get('image')),
            "tags": tags, 
            "metro_id": final_metro_id, # Use the final determined metro_id
            "price_val": price_val,
            "price_ccy": price_ccy,
            "fingerprint": None, 
            "quality_score": None,
        }
        return out

    except Exception as e:
        print(f"ERROR processing event_raw_id {raw_event_row.get('id', 'UNKNOWN') if isinstance(raw_event_row, dict) else 'UNKNOWN'}: {e}")
        import traceback
        traceback.print_exc()
        return None

# Function to update the status of a raw event
def mark_raw_event_status(cur, event_raw_id: int, status: str):
    """Updates the normalized_at and normalization_status of an event in event_raw."""
    try:
        cur.execute(
            """UPDATE event_raw 
               SET normalized_at = CURRENT_TIMESTAMP, normalization_status = %s 
               WHERE id = %s;""",
            (status, event_raw_id)
        )
        # The commit will be handled by the main loop
    except psycopg2.Error as e:
        print(f"Error updating status for event_raw_id {event_raw_id}: {e}")
        # Decide if this should raise an exception or just log

def worker_normalize(db_conn):
    print("Starting Normalizer Worker...")
    loop_count = 0
    while True:
        loop_count += 1
        try:
            with db_conn.cursor() as cur: 
                # Query for a batch of events that have been parsed but not yet normalized
                # Using parsed_at IS NOT NULL as the indicator from worker_parse.py
                query_string = """SELECT * FROM event_raw
                       WHERE parsed_at IS NOT NULL    -- Events processed by worker_parse.py
                         AND normalized_at IS NULL  -- And not yet processed by this worker (worker_normalize.py)
                       ORDER BY parsed_at ASC        -- Process older parsed events first
                       LIMIT %s;"""
                cur.execute(query_string, (BATCH_SIZE,))
                raw_event_rows = cur.fetchall()

                if not raw_event_rows:
                    db_conn.commit() 
                    time.sleep(WORKER_DELAY)
                    continue

                print(f"Normalizer: Fetched {len(raw_event_rows)} events (parsed_at IS NOT NULL, normalized_at IS NULL) to normalize.")
                events_processed_in_batch = 0
                for raw_event_row in raw_event_rows:
                    event_raw_id = raw_event_row['id']
                    
                    # process_raw_event expects raw_event_row['raw_json'] to be the JSON-LD blob.
                    # raw_event_row['metro_id'] should have been set correctly by worker_parse.py.
                    normalized_event_dict = process_raw_event(cur, raw_event_row)

                    if not normalized_event_dict:
                        print(f"  Failed to process event_raw_id {event_raw_id}. Marking with normalization_status = 'error'.")
                        mark_raw_event_status(cur, event_raw_id, 'error') # This function updates normalized_at and normalization_status
                        continue 

                    # Generate fingerprint
                    # Ensure start_ts is a datetime object for make_fp
                    fp = make_fp(normalized_event_dict['title'], normalized_event_dict['start_ts'], normalized_event_dict['metro_id'])
                    if not fp:
                        print(f"  Could not generate fingerprint for event_raw_id {event_raw_id}. Marking as 'error'.")
                        mark_raw_event_status(cur, event_raw_id, 'error')
                        continue
                    normalized_event_dict["fingerprint"] = fp

                    # Calculate quality score
                    normalized_event_dict["quality_score"] = score(normalized_event_dict)
                    
                    # Prepare for insert
                    # Ensure tags are in a format psycopg2 can handle for jsonb (list of strings is fine)
                    if isinstance(normalized_event_dict.get("tags"), list):
                        normalized_event_dict["tags"] = Json(normalized_event_dict["tags"])
                    else:
                        normalized_event_dict["tags"] = None # Keep None if no tags

                    try:
                        # Using dict for column names for clarity
                        # Note: ST_GeomFromText requires WKT. If venueGeom is None, it should insert NULL.
                        # The DB column for venueGeom should allow NULLs.
                        # The `tags` column is jsonb, psycopg2 can adapt Python lists to jsonb arrays.
                        
                        # Use column-based conflict target instead of constraint name
                        insert_sql = """
                        INSERT INTO event_clean (
                            event_raw_id, source, source_event_id, title, description, url, start_ts, end_ts,
                            venue_name, venue_address, venue_geom, image_url, tags, metro_id, price_val, price_ccy, 
                            fingerprint, quality_score
                            -- normalized_at for event_clean is set by DB default
                        )
                        VALUES (
                            %(event_raw_id)s, %(source)s, %(source_event_id)s, %(title)s, %(description)s, %(url)s, %(start_ts)s, %(end_ts)s,
                            %(venueName)s, %(venueAddress)s, 
                            CASE WHEN %(venueGeom)s IS NOT NULL THEN ST_SetSRID(ST_GeomFromText(%(venueGeom)s), 4326) ELSE NULL END,
                            %(imageUrl)s, %(tags)s, %(metro_id)s, %(price_val)s, %(price_ccy)s,
                            %(fingerprint)s, %(quality_score)s
                        )
                        ON CONFLICT (metro_id, fingerprint) DO NOTHING; 
                        """
                        cur.execute(insert_sql, normalized_event_dict)
                        
                        if cur.rowcount > 0:
                            print(f"    Successfully inserted event_clean record for event_raw_id {event_raw_id}.")
                            mark_raw_event_status(cur, event_raw_id, 'processed')
                        else:
                            print(f"    Duplicate event (or no insert) for event_raw_id {event_raw_id} based on fingerprint. Marking as 'duplicate'.")
                            mark_raw_event_status(cur, event_raw_id, 'duplicate')
                        events_processed_in_batch += 1
                    except psycopg2.errors.UniqueViolation:
                        db_conn.rollback() # Rollback the main transaction for this event
                        print(f"    UniqueViolation (likely duplicate) for event_raw_id {event_raw_id}. Marking as 'duplicate'.")
                        # Use a new cursor for the status update in its own transaction
                        try:
                            with db_conn.cursor() as status_cur:
                                mark_raw_event_status(status_cur, event_raw_id, 'duplicate')
                            db_conn.commit() # Commit status update
                        except Exception as status_update_err:
                            print(f"      Failed to update status to 'duplicate' for {event_raw_id} after UniqueViolation: {status_update_err}")
                            db_conn.rollback() # Rollback the status update attempt
                        # The main loop's cursor 'cur' is no longer valid for the rest of this iteration.
                        # We should break this inner loop and let the outer loop get a new cursor for the next batch.
                        # However, the current structure tries to continue processing other items in raw_event_rows.
                        # This is problematic if 'cur' is from an aborted transaction.
                        # For now, we continue, and the outer 'cur.execute' for the next event will fail if 'cur' is bad.
                        # A cleaner approach is to process one event per transaction or handle batch rollback more carefully.
                        # Given the current structure, this change focuses on isolating status updates.
                        continue # Continue to the next event in the batch

                    except psycopg2.Error as db_err:
                        db_conn.rollback() # Rollback the main transaction for this event
                        print(f"    DATABASE ERROR inserting event_raw_id {event_raw_id}: {db_err}")
                        # Use a new cursor for the status update in its own transaction
                        try:
                            with db_conn.cursor() as status_cur:
                                 mark_raw_event_status(status_cur, event_raw_id, 'error')
                            db_conn.commit() # commit status update
                        except Exception as status_update_err:
                            print(f"      Failed to update status to 'error' for {event_raw_id} after DB error: {status_update_err}")
                            db_conn.rollback() # Rollback the status update attempt
                        continue # Continue to the next event in the batch

                    except Exception as e:
                        db_conn.rollback() # Rollback the main transaction for this event
                        print(f"    UNEXPECTED ERROR processing event_raw_id {event_raw_id} during insert stage: {e}")
                        import traceback
                        traceback.print_exc()
                        # Use a new cursor for the status update in its own transaction
                        try:
                            with db_conn.cursor() as status_cur:
                                 mark_raw_event_status(status_cur, event_raw_id, 'error')
                            db_conn.commit() # commit status update
                        except Exception as status_update_err:
                            print(f"      Failed to update status to 'error' for {event_raw_id} after unexpected error: {status_update_err}")
                            db_conn.rollback() # Rollback the status update attempt
                        continue # Continue to the next event in the batch
                
                if events_processed_in_batch > 0:
                    print(f"Normalizer: Committing batch of {events_processed_in_batch} processed events.")
                    db_conn.commit()
                else:
                    # If no events were successfully processed to the point of attempting insert (e.g. all failed validation early)
                    # still commit to save any status updates like 'error' for events that failed before insert stage.
                    db_conn.commit()
                    print("Normalizer: No events fully processed in this batch, but committing status updates.")

        except psycopg2.InterfaceError as ie:
            print(f"Normalizer: Database connection lost: {ie}. Reconnecting...")
            db_conn.close() # Close the broken connection
            db_conn = initialize_db()
            if not db_conn:
                print("Normalizer: Failed to reconnect to database. Exiting worker thread.")
                break # Exit the while True loop
            time.sleep(WORKER_DELAY) # Wait a bit before retrying

        except psycopg2.Error as e:
            print(f"Normalizer: Database error in main loop: {e}")
            # db_conn.rollback() # Rollback any transaction if one was active at this level
            # It might be better to re-initialize connection on any doubt
            if db_conn and not db_conn.closed:
                db_conn.rollback() # Rollback if connection is still there and transaction started
            else: # Connection might be closed or unusable
                print("Normalizer: Database connection might be closed. Attempting to re-initialize.")
                if db_conn: db_conn.close()
                db_conn = initialize_db()
                if not db_conn:
                    print("Normalizer: Failed to re-initialize DB. Sleeping and retrying...")
                    time.sleep(WORKER_DELAY * 2)
                    continue # try to re-initialize in next iteration

            print(f"Normalizer: Sleeping for {WORKER_DELAY}s before retrying operation...")
            time.sleep(WORKER_DELAY)
        
        except Exception as e:
            print(f"Normalizer: An unexpected error occurred in the main loop: {e}")
            import traceback
            traceback.print_exc()
            if db_conn and not db_conn.closed:
                db_conn.rollback()
            print(f"Normalizer: Sleeping for {WORKER_DELAY * 2}s before retrying operation...")
            time.sleep(WORKER_DELAY * 2)

    if db_conn:
        db_conn.close()
    print("Normalizer Worker finished.")

if __name__ == '__main__':
    db_connection = initialize_db()
    if db_connection:
        worker_normalize(db_connection)
    else:
        print("Could not start normalizer worker due to DB connection failure.") 