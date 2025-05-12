# worker_fetch.py
from playwright.sync_api import sync_playwright, Error as PlaywrightError
import extruct
import json
import redis
import time
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# Read from environment variables, falling back to defaults
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis') 
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379)) # Ensure port is an integer
INPUT_QUEUE = 'url_queue'  # Queue to read URLs from
OUTPUT_QUEUE = 'jsonld_raw' # Queue to write extracted JSON-LD blobs to
BROWSER_TIMEOUT = 15000   # Page load timeout in milliseconds
WORKER_DELAY = 0.1        # Small delay if queue is empty to prevent busy-waiting
# -------------------

def initialize_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True) # Decode responses to get strings
        r.ping() # Check connection
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        return None

def worker_fetch(redis_conn):
    print("Starting fetch worker...")
    processed_count = 0
    fail_count = 0
    
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-gpu'])
            print("Browser launched.")
        except PlaywrightError as e:
            print(f"Failed to launch browser: {e}")
            return

        while True:
            try:
                # Pop the packaged data from INPUT_QUEUE
                # url_package_json = redis_conn.blpop(INPUT_QUEUE, timeout=5) # Example blocking pop
                # url_package_json = url_package_json[1] if url_package_json else None
                url_package_json = redis_conn.lpop(INPUT_QUEUE)
                
                if not url_package_json:
                    time.sleep(WORKER_DELAY)
                    continue

                # Parse the package
                try:
                    url_data = json.loads(url_package_json)
                    url_to_fetch = url_data.get("url")
                    source_metro_id = url_data.get("metro_id") # Get metro_id from package
                    source_dance_style = url_data.get("dance_style_context") # Get dance_style_context

                    if not url_to_fetch:
                        print(f"Malformed package from queue (missing URL): {url_package_json[:100]}...")
                        fail_count +=1 # Consider a different counter for malformed packages
                        continue

                except json.JSONDecodeError:
                    print(f"Failed to decode JSON package from queue: {url_package_json[:100]}...")
                    fail_count += 1 # Consider a different counter
                    continue
                
                # Ensure source_metro_id is present, though it could be None if the upstream package was malformed
                # The downstream worker_parse.py will handle if metro_id is missing for its DB insert.

                print(f"Processing URL: {url_to_fetch} (MetroID: {source_metro_id}, Style: {source_dance_style})")
                page = None 
                try:
                    page = browser.new_page()
                    page.goto(url_to_fetch, wait_until="domcontentloaded", timeout=BROWSER_TIMEOUT)
                    html_content = page.content()
                    
                    # Extract JSON-LD
                    # Use uniform=True to attempt normalization across syntaxes if needed
                    # syntaxes=["json-ld"] limits extraction to only JSON-LD
                    extracted_data = extruct.extract(
                        html_content,
                        base_url=url_to_fetch,      # Helps resolve relative URLs if any inside JSON-LD
                        syntaxes=["json-ld"],
                        uniform=True
                    )["json-ld"]

                    if extracted_data:
                        print(f"  Found {len(extracted_data)} JSON-LD blob(s) for {url_to_fetch}.")
                        for blob in extracted_data:
                            # Package the blob with its source URL and the original context
                            output_package = {
                                "original_url": url_to_fetch,
                                "source_metro_id": source_metro_id, # Pass through metro_id
                                "source_dance_style": source_dance_style, # Pass through dance_style
                                "blob": blob
                            }
                            redis_conn.rpush(OUTPUT_QUEUE, json.dumps(output_package))
                        processed_count += 1
                    else:
                        print(f"  No JSON-LD found for {url_to_fetch}.")

                except PlaywrightError as e_page:
                    print(f"  Playwright error processing {url_to_fetch}: {e_page}")
                    fail_count += 1
                except Exception as e_general:
                    print(f"  Unexpected error processing {url_to_fetch}: {e_general}")
                    fail_count += 1
                finally:
                    if page:
                        page.close()
            
            except redis.exceptions.ConnectionError as e_redis:
                print(f"Redis connection error: {e_redis}. Attempting to reconnect...")
                time.sleep(5)
                redis_conn = initialize_redis() # Try to re-initialize
                if not redis_conn:
                    print("Failed to reconnect to Redis. Exiting worker.")
                    break # Exit the loop if reconnect fails
            except KeyboardInterrupt:
                print("\nShutdown signal received.")
                break
            except Exception as e_loop:
                print(f"Critical error in worker loop: {e_loop}")
                time.sleep(5) # Avoid rapid failing loop
        
        # Cleanup
        print("Closing browser...")
        browser.close()
        print(f"Worker finished. Processed: {processed_count}, Failed: {fail_count}")

if __name__ == '__main__':
    redis_client = initialize_redis()
    if redis_client:
        worker_fetch(redis_client)
    else:
        print("Could not start worker due to Redis connection failure.") 