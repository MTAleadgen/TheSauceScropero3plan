import os
import requests
import json
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# DataForSEO credentials
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD")

# DataForSEO API endpoint for Google Organic Search
endpoint = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"

# Create auth header
auth_header = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()

# STRICTLY control the headers - use only what's absolutely necessary
headers = {
    'Authorization': f'Basic {auth_header}',
    'Content-Type': 'application/json'
}

# CORRECT DATA FORMAT: Include location_name as required by the API
# The location_name parameter is crucial for specifying the geographic location
data = [{
    "keyword": "salsa dance",
    "location_name": "New York,United States",  # Required format: city,country
    "language_name": "English",  # Using language_name instead of language_code
    "depth": 10,
    "se_domain": "google.com"
}]

print(f"Sending request with payload: {json.dumps(data, indent=2)}")

# Enable HTTP request debugging
requests_log = requests.packages.urllib3.add_stderr_logger()
requests_log.setLevel("DEBUG")

try:
    # Make the API request with specifically controlled parameters
    # Set all request parameters explicitly to avoid any default behaviors
    session = requests.Session()
    
    # Create prepared request to see exactly what's being sent
    req = requests.Request('POST', endpoint, headers=headers, json=data)
    prepped = req.prepare()
    
    print("\nPrepared Request Headers:")
    for key, value in prepped.headers.items():
        if key.lower() != 'authorization':  # Don't print auth credentials
            print(f"{key}: {value}")
    
    print("\nPrepared Request Body:")
    print(prepped.body.decode() if isinstance(prepped.body, bytes) else prepped.body)
    
    # Make the actual request
    response = session.send(prepped)
    response.raise_for_status()
    result = response.json()
    
    print(f"\nAPI Response status: {result.get('status_code')} - {result.get('status_message')}")
    
    # Print task-level details
    for task in result.get('tasks', []):
        print(f"Task ID: {task.get('id')}")
        print(f"Task status: {task.get('status_code')} - {task.get('status_message')}")
        
        # Count results if available
        if task.get('result') and isinstance(task.get('result'), list):
            items_count = 0
            for result_block in task.get('result', []):
                if result_block.get('items'):
                    items_count += len(result_block.get('items', []))
            print(f"Results found: {items_count} items")
        else:
            print("No results found")
        
        print(f"Task data: {json.dumps(task.get('data', {}), indent=2)}")
        
    # If there was an error, print the full response for debugging
    if result.get('status_code') != 20000 or any(task.get('status_code') != 20000 for task in result.get('tasks', [])):
        print("\nFull API Response:")
        print(json.dumps(result, indent=2))
        
except Exception as e:
    print(f"Error: {e}")
    if 'response' in locals():
        print(f"Response text: {response.text}") 