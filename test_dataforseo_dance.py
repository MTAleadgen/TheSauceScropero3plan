#!/usr/bin/env python3
"""
test_dataforseo_dance.py

Test script to verify the DataForSEO events endpoint is working correctly
for dance-related queries. This will run just the data collection portion
with 1 city and the 2 specific queries we want:
1. "dance in [city]"
2. "dancing in [city]"

Usage:
  python test_dataforseo_dance.py [city_name]
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

def make_dataforseo_events_query(city, query_type):
    """Make a query to DataForSEO Events API."""
    headers = get_dataforseo_client()
    # Use the events endpoint 
    endpoint = "https://api.dataforseo.com/v3/serp/google/events/live/advanced"
    
    # Build the query based on type
    if query_type == 'dance':
        keyword = f"dance in {city}"
    else:  # dancing
        keyword = f"dancing in {city}"
    
    print(f"Making query: '{keyword}'")
    
    # Configure the request - use proper location parameters
    data = {
        "keyword": keyword,
        "location_code": 2840,  # US location code (default)
        "language_code": "en",
        "device": "desktop",
        "os": "windows",
        "depth": 1,  # Retrieve 100 events (100 events per page)
        "date_range": "next_week",  # Focus on upcoming events
        "priority": 1  # Higher priority for faster processing
    }
    
    # If city is New York, use appropriate location code
    if city.lower() == "new york":
        data["location_code"] = 1023191  # New York location code
    
    try:
        response = requests.post(endpoint, headers=headers, json=[data])
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error making DataForSEO query: {e}")
        return None

def count_and_display_events(response):
    """Count and display events from the API response."""
    if not response or 'tasks' not in response or not response['tasks']:
        print("No valid response or tasks found")
        return 0
    
    total_events = 0
    
    for task in response['tasks']:
        if task.get('status_code') != 20000:
            print(f"Task error: {task.get('status_message', 'Unknown error')}")
            continue
            
        result = task.get('result', [])
        if not result:
            print("No results found in task")
            continue
            
        for item in result:
            items = item.get('items', [])
            if not items:
                print("No items found in result")
                continue
                
            print(f"Found {len(items)} events:")
            for i, event in enumerate(items[:5], 1):  # Show first 5 events
                print(f"  {i}. {event.get('title', 'No title')}")
                if 'date' in event:
                    print(f"     Date: {event.get('date')}")
                if 'venue' in event:
                    print(f"     Venue: {event.get('venue')}")
            
            if len(items) > 5:
                print(f"  ... and {len(items) - 5} more events")
            
            total_events += len(items)
    
    return total_events

def main():
    # Get city name from command line or use default
    if len(sys.argv) > 1:
        city = sys.argv[1]
    else:
        city = "New York"  # Default city
    
    print(f"\nTesting DataForSEO events endpoint for city: {city}")
    print("Running the 2 queries we need for our pipeline:\n")
    
    # Query for "dance in [city]"
    print("\n=== Query 1: 'dance in {city}' ===")
    response1 = make_dataforseo_events_query(city, 'dance')
    count1 = count_and_display_events(response1)
    
    # Query for "dancing in [city]"
    print("\n=== Query 2: 'dancing in {city}' ===")
    response2 = make_dataforseo_events_query(city, 'dancing')
    count2 = count_and_display_events(response2)
    
    # Summary
    print("\n=== Summary ===")
    print(f"Query 1 ('dance in {city}'): {count1} events")
    print(f"Query 2 ('dancing in {city}'): {count2} events")
    print(f"Total: {count1 + count2} events")
    
    # Save raw responses for inspection
    with open('dance_query_response.json', 'w') as f:
        json.dump(response1, f, indent=2)
    with open('dancing_query_response.json', 'w') as f:
        json.dump(response2, f, indent=2)
    print("\nSaved raw responses to dance_query_response.json and dancing_query_response.json")

if __name__ == "__main__":
    main() 