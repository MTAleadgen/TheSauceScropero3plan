#!/usr/bin/env python3
"""
get_dataforseo_locations.py

Fetches available location codes from DataForSEO API to use in our queries.
"""

import os
import sys
import json
import base64
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# DataForSEO API credentials
DATAFORSEO_API_LOGIN = os.environ.get('DATAFORSEO_LOGIN')
DATAFORSEO_API_PASSWORD = os.environ.get('DATAFORSEO_PASSWORD')

if not DATAFORSEO_API_LOGIN or not DATAFORSEO_API_PASSWORD:
    print("Error: DataForSEO API credentials not set in environment variables")
    print("Make sure your .env file contains DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD")
    sys.exit(1)

def get_dataforseo_client():
    """Setup DataForSEO API client."""
    auth_string = f"{DATAFORSEO_API_LOGIN}:{DATAFORSEO_API_PASSWORD}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded_auth}',
        'Content-Type': 'application/json',
    }
    
    return headers

def get_locations():
    """Fetch location data from DataForSEO API."""
    headers = get_dataforseo_client()
    endpoint = "https://api.dataforseo.com/v3/serp/google/locations"
    
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error getting locations: {e}")
        return None

def search_locations(locations_data, search_term):
    """Search for locations matching the search term."""
    if not locations_data or 'tasks' not in locations_data:
        return []
    
    results = []
    
    for task in locations_data['tasks']:
        if 'result' not in task:
            continue
            
        for location in task['result']:
            location_name = location.get('location_name', '').lower()
            country_name = location.get('country_name', '').lower()
            
            if search_term.lower() in location_name or search_term.lower() in country_name:
                results.append({
                    'location_code': location['location_code'],
                    'location_name': location['location_name'],
                    'country_name': location.get('country_name', 'Unknown'),
                    'location_type': location.get('location_type', 'Unknown')
                })
    
    return results

def main():
    print("\nFetching DataForSEO location codes...\n")
    
    locations_data = get_locations()
    if not locations_data:
        print("Failed to fetch location data")
        return
    
    # Save complete location data for reference
    with open('dataforseo_locations.json', 'w') as f:
        json.dump(locations_data, f, indent=2)
    
    print("Saved complete location data to dataforseo_locations.json")
    
    # Allow searching for specific locations
    while True:
        search = input("\nEnter city or country name to search (or 'quit' to exit): ")
        if search.lower() in ['quit', 'exit', 'q']:
            break
            
        results = search_locations(locations_data, search)
        
        if results:
            print(f"\nFound {len(results)} matching locations:")
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['location_name']}, {result['country_name']}")
                print(f"   Code: {result['location_code']}, Type: {result['location_type']}")
        else:
            print("\nNo matching locations found")
    
    print("\nDone!")

if __name__ == "__main__":
    main() 