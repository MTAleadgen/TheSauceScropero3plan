import os
import psycopg2
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time
import random
from unidecode import unidecode # For accent stripping

# Initialize Nominatim geocoder
# Using a custom user_agent is good practice as per Nominatim's usage policy
geolocator = Nominatim(user_agent="metro_qa_script_1.0")

def reverse_geocode_nominatim(lat, lon, attempt=1, max_attempts=3):
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True, language='en', timeout=10)
        return location
    except GeocoderTimedOut:
        if attempt <= max_attempts:
            print(f"Nominatim timed out. Retrying ({attempt}/{max_attempts})...")
            time.sleep(attempt * 2) # Exponential backoff
            return reverse_geocode_nominatim(lat, lon, attempt + 1, max_attempts)
        else:
            print("Nominatim timed out after multiple retries.")
            return None
    except GeocoderUnavailable as e:
        print(f"Nominatim service unavailable: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during reverse geocoding: {e}")
        return None

def normalize_name(name):
    if not name:
        return ""
    # Convert to lowercase, strip accents, and remove common noise words/characters
    name = str(name).lower()
    name = unidecode(name) # Strips accents, e.g., São Paulo -> Sao Paulo
    # Add more normalization if needed (e.g., removing punctuation, specific suffixes)
    return name.strip()

def run_qa_test(sample_size=20):
    load_dotenv('.env.local')
    database_url = os.getenv('DATABASE_URL')

    if not database_url:
        print("Error: DATABASE_URL not found in .env.local")
        return

    conn = None
    matches = 0
    mismatches = 0
    failures = 0

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        print("Successfully connected to the database.")

        # Fetch a random sample of metros
        # Using TABLESAMPLE SYSTEM (1) might be faster for very large tables if approx percentage is ok
        # For 1000 rows, ORDER BY RANDOM() LIMIT N is generally fine.
        cur.execute(f"""
            SELECT geonameid, name, asciiname, latitude, longitude 
            FROM metro 
            ORDER BY RANDOM() 
            LIMIT {sample_size};
        """)
        sample_metros = cur.fetchall()
        print(f"Fetched {len(sample_metros)} random metros for testing.")

        for row_num, record in enumerate(sample_metros):
            geonameid, db_name, db_asciiname, lat, lon = record
            print(f"\nProcessing record {row_num + 1}/{len(sample_metros)}: geonameid={geonameid}, name='{db_name}', lat={lat}, lon={lon}")

            location_obj = reverse_geocode_nominatim(lat, lon)

            if location_obj and location_obj.raw.get('address'):
                address = location_obj.raw['address']
                # Try to get city, town, or village from the address components
                nominatim_city = address.get('city') or address.get('town') or address.get('village') or address.get('county')
                
                if nominatim_city:
                    normalized_db_name = normalize_name(db_name)
                    normalized_db_asciiname = normalize_name(db_asciiname)
                    normalized_nominatim_city = normalize_name(nominatim_city)
                    
                    print(f"  DB Name (norm): '{normalized_db_name}', Nominatim City (norm): '{normalized_nominatim_city}'")
                    
                    # Check for a match (can be made more sophisticated)
                    if normalized_nominatim_city in normalized_db_name or \
                       normalized_nominatim_city in normalized_db_asciiname or \
                       normalized_db_name in normalized_nominatim_city or \
                       normalized_db_asciiname in normalized_nominatim_city:
                        print("  Status: MATCH (or partial match)")
                        matches += 1
                    else:
                        print(f"  Status: MISMATCH. Nominatim returned: {nominatim_city} (Full address: {location_obj.address})")
                        mismatches += 1
                else:
                    print(f"  Status: FAILED (City not found in Nominatim response). Full address: {location_obj.address}")
                    failures += 1
            else:
                print("  Status: FAILED (Reverse geocoding failed or no address found)")
                failures += 1
            
            # Respect Nominatim's usage policy: max 1 request per second
            time.sleep(1.1)

        cur.close()

    except psycopg2.Error as e_db:
        print(f"Database error: {e_db}")
    except Exception as e_general:
        print(f"An unexpected error occurred: {e_general}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

    print("\n--- QA Summary ---")
    print(f"Total records tested: {len(sample_metros) if 'sample_metros' in locals() and sample_metros else 0}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {mismatches}")
    print(f"Failures (geocoding/no city): {failures}")

if __name__ == '__main__':
    # For unidecode, you might need to install it: pip install unidecode
    run_qa_test(sample_size=20) # Test 20 random metros 