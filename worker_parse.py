# worker_parse.py
import json
import redis
import psycopg2
from psycopg2 import sql
from psycopg2 import extras # For execute_values if needed, though inserting one-by-one here
from psycopg2.extras import Json # Adapt dict to jsonb
from dateutil import parser as dateutil_parser
from uuid import uuid4
import time
import os
from dotenv import load_dotenv

# --- Configuration ---
# Read from environment variables, falling back to defaults
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379)) # Ensure port is an integer
INPUT_QUEUE = 'jsonld_raw' # Queue to read raw JSON-LD blobs from
DB_ENV_VAR = 'DATABASE_URL' # Environment variable for database connection string
WORKER_DELAY = 0.1        # Small delay if queue is empty

# Types we are interested in parsing
APPROVED_EVENT_TYPES = {"Event", "DanceEvent", "SocialDance"} # Add other relevant schema.org types if needed
# -------------------

def initialize_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True) # Use decode_responses=True
        r.ping()
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        return None

def initialize_db():
    # Try .env.local first, then .env as a fallback
    if not load_dotenv('.env.local'):
        load_dotenv('.env') # Loads .env if .env.local is not found or fails to load

    database_url = os.getenv(DB_ENV_VAR)
    if not database_url:
        print(f"Error: {DB_ENV_VAR} not found in environment variables or .env files (.env.local, .env)")
        return None
    try:
        conn = psycopg2.connect(database_url)
        print("Connected to Database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to Database: {e}")
        return None

def parse_datetime(date_string):
    if not date_string:
        return None
    try:
        return dateutil_parser.parse(date_string)
    except (ValueError, TypeError) as e:
        # print(f"Could not parse date: {date_string} - Error: {e}")
        return None

def worker_parse(redis_conn, db_conn):
    print("Starting parse worker...")
    processed_count = 0
    inserted_count = 0
    skipped_count = 0
    fail_count = 0
    cur = None

    while True:
        db_conn_active = False
        try:
            # Check DB connection and get cursor
            if db_conn is None or db_conn.closed != 0:
                print("DB connection lost. Attempting to reconnect...")
                db_conn = initialize_db()
                if not db_conn:
                    print("Failed to reconnect to DB. Waiting...")
                    time.sleep(10)
                    continue # Skip this cycle
            db_conn_active = True
            cur = db_conn.cursor()

            # Check Redis connection
            if not redis_conn or not redis_conn.ping(): # Simple check, might need more robust handling
                 print("Redis connection lost. Attempting to reconnect...")
                 redis_conn = initialize_redis()
                 if not redis_conn:
                     print("Failed to reconnect to Redis. Waiting...")
                     time.sleep(10)
                     continue # Skip this cycle

            package_json = redis_conn.lpop(INPUT_QUEUE)

            if not package_json:
                time.sleep(WORKER_DELAY)
                if cur and db_conn_active: 
                    cur.close()
                    cur = None
                continue

            processed_count += 1
            current_url_for_logging = "unknown_url_in_package"
            try:
                record = json.loads(package_json)
                # Extract fields from the enriched package
                original_url = record.get("original_url") 
                ld_blob = record.get("blob")
                source_metro_id = record.get("source_metro_id") # <<<< Key new field
                source_dance_style = record.get("source_dance_style") # <<<< Optional context
                
                current_url_for_logging = original_url or current_url_for_logging

                if not isinstance(ld_blob, dict) or not original_url:
                    print(f"Skipping malformed package (missing blob or original_url): {package_json[:150]}...")
                    skipped_count += 1
                    continue
                
                # metro_id is critical. If not present from upstream, we might skip or handle as an error.
                if source_metro_id is None:
                    print(f"Skipping package due to missing 'source_metro_id' for URL {original_url}: {package_json[:150]}...")
                    skipped_count += 1
                    continue

                # Check the type
                event_type = ld_blob.get("@type")
                is_approved_type = False
                if isinstance(event_type, str) and event_type in APPROVED_EVENT_TYPES:
                    is_approved_type = True
                elif isinstance(event_type, list):
                    if set(event_type) & APPROVED_EVENT_TYPES: 
                        is_approved_type = True
                
                if not is_approved_type:
                    print(f"Skipping non-event type: @type='{event_type}' from URL: {original_url}")
                    skipped_count += 1
                    continue

                # --- Basic Quality Checks ---
                event_name = ld_blob.get("name")
                start_date_str = ld_blob.get("startDate")
                # location_data = ld_blob.get("location") # For future, if we check location here
                start_ts = parse_datetime(start_date_str)

                if not event_name or not start_ts:
                    # Added more detail to this log message
                    print(f"Skipping approved event type ('{event_type}') due to missing/invalid fields: name='{event_name}', startDate='{start_date_str}' from URL: {original_url}")
                    skipped_count += 1
                    continue
                # --- End Quality Checks ---

                # Extract source_event_id from blob if possible (e.g., a URL or specific ID field within the blob)
                # For now, let's assume it might be the URL of the event if distinct, or leave it Null if not easily found.
                # A common pattern is to use the event's own URL if it has one in the JSON-LD, or an internal ID.
                # If ld_blob has a "url" or "@id" that seems like a permalink, use that.
                potential_source_event_id = ld_blob.get("url") or ld_blob.get("@id")
                if not isinstance(potential_source_event_id, str): # Ensure it's a string
                    potential_source_event_id = None

                # Modified INSERT query: Removed the non-existent 'status' column
                insert_query = sql.SQL("""
                    INSERT INTO event_raw (source, source_event_id, metro_id, raw_json, parsed_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (source, source_event_id) DO NOTHING 
                    RETURNING id;
                """) 

                cur.execute(insert_query, (
                    original_url, 
                    potential_source_event_id, 
                    source_metro_id, 
                    Json(ld_blob)
                ))
                
                result = cur.fetchone()
                if result:
                    inserted_id = result[0]
                    inserted_count += 1
                    print(f"Inserted/Found event with event_raw.id: {inserted_id} from URL: {original_url} (Metro: {source_metro_id})")
                else:
                    skipped_count +=1 
                
                db_conn.commit()
                
                if processed_count % 100 == 0:
                     print(f"Processed {processed_count}, Inserted {inserted_count}, Skipped {skipped_count}, Failed {fail_count}")

            except json.JSONDecodeError:
                print(f"Failed to decode JSON from jsonld_raw: {package_json[:150]}...")
                fail_count += 1
            except psycopg2.Error as e_db_insert:
                print(f"DB insert error for {current_url_for_logging}: {e_db_insert}")
                fail_count += 1
                if db_conn_active:
                    db_conn.rollback()
            except Exception as e_parse:
                print(f"Unexpected error parsing/inserting record from {current_url_for_logging}: {e_parse}")
                fail_count += 1
                if db_conn_active:
                    db_conn.rollback()
            finally:
                if cur and db_conn_active:
                    cur.close()
                    cur = None

        except redis.exceptions.ConnectionError as e_redis_loop:
            print(f"Redis connection error in main loop: {e_redis_loop}. Attempting to reconnect...")
            redis_conn = None # Signal to reconnect
            time.sleep(5)
        except psycopg2.Error as e_db_loop:
            print(f"Database connection error in main loop: {e_db_loop}. Attempting to reconnect...")
            if db_conn_active and db_conn: # Ensure conn exists before trying to close
                try:
                    db_conn.close()
                except Exception: pass # Ignore errors during close
            db_conn = None # Signal to reconnect
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nShutdown signal received.")
            break
        except Exception as e_loop_critical:
            print(f"Critical error in worker loop: {e_loop_critical}")
            time.sleep(5)

    # Final cleanup
    print(f"Worker finished. Processed: {processed_count}, Inserted: {inserted_count}, Skipped: {skipped_count}, Failed: {fail_count}")
    if db_conn and db_conn.closed == 0:
        db_conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    r_conn = initialize_redis()
    d_conn = initialize_db()
    
    if r_conn and d_conn:
        worker_parse(r_conn, d_conn)
    else:
        print("Could not start worker due to Redis or DB connection failure.")
        if d_conn and d_conn.closed == 0:
             d_conn.close() 