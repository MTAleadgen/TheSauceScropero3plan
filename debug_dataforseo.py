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
headers = {
    'Authorization': f'Basic {auth_header}',
    'Content-Type': 'application/json'
}

# Try a completely generic query with no location at all
data = [{
    "keyword": "salsa dance",  # Generic query with no location 
    "language_code": "en", 
    "depth": 10,
    "se_domain": "google.com"
}]

print(f"Sending request with payload: {json.dumps(data, indent=2)}")

try:
    # Make the API request
    response = requests.post(endpoint, json=data, headers=headers)
    response.raise_for_status()
    result = response.json()
    
    print(f"API Response status: {result.get('status_code')} - {result.get('status_message')}")
    
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