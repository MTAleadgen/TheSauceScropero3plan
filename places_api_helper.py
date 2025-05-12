#!/usr/bin/env python3
"""
places_api_helper.py

Helper module for resolving venue addresses using Google Places API
with built-in caching and usage tracking to stay within free tier limits.
"""

import os
import sys
import json
import requests
import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor, Json

# Load environment variables
load_dotenv()

# Google Places API configuration
GOOGLE_PLACES_API_KEY = os.environ.get('GOOGLE_PLACES_API')
if not GOOGLE_PLACES_API_KEY:
    print("Warning: GOOGLE_PLACES_API environment variable not set")

# Free tier usage limits (as of 2025)
# Reference: https://developers.google.com/maps/documentation/places/web-service/usage-and-billing
# Pricing structure updated on March 1, 2025: Free monthly calls replaced $200 monthly credit
ESSENTIALS_FREE_CALLS = 10000  # 10K free calls per SKU per month
PRO_FREE_CALLS = 5000          # 5K free calls per SKU per month 
ENTERPRISE_FREE_CALLS = 1000   # 1K free calls per SKU per month

# We're using Text Search which is an Essentials SKU when using IDs only
# For our use case where we just need basic info, we'll stay in Essentials tier
FREE_CALLS_LIMIT = ESSENTIALS_FREE_CALLS

# Cost per 1000 requests (CPM)
TEXT_SEARCH_COST = 0.032  # $0.032 per call for Places API Text Search (Essentials)
SAFETY_MARGIN_PERCENT = 20  # Keep 20% buffer to avoid accidental overages

# Calculate safe limits - use 80% of the free calls for safety margin
MONTHLY_SEARCH_LIMIT = int(FREE_CALLS_LIMIT * (100 - SAFETY_MARGIN_PERCENT) / 100)
DAILY_SEARCH_LIMIT = int(MONTHLY_SEARCH_LIMIT / 30)  # Distribute evenly over a month

# API endpoints
PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

def initialize_db():
    """Initialize database connection."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL environment variable not set.")
        return None
    
    try:
        conn = psycopg2.connect(db_url, cursor_factory=DictCursor)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def setup_venue_cache_table(conn):
    """
    Set up the venue cache table if it doesn't exist.
    This table stores previously looked up venues to avoid duplicate API calls.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS venue_cache (
                    id SERIAL PRIMARY KEY,
                    venue_name TEXT NOT NULL,
                    venue_address TEXT,
                    city TEXT,
                    formatted_address TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    place_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(venue_name, city)
                )
            """)
            
            # Also create usage tracking table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS api_usage_tracking (
                    id SERIAL PRIMARY KEY,
                    api_name TEXT NOT NULL,
                    calls_count INTEGER DEFAULT 0,
                    usage_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(api_name, usage_date)
                )
            """)
            conn.commit()
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error setting up cache tables: {e}")
        return False
    
    return True

def check_api_usage_limits(conn, api_name='google_places'):
    """
    Check if we've exceeded our daily or monthly API usage limits.
    Returns True if safe to make more calls, False if we're at limit.
    """
    try:
        with conn.cursor() as cur:
            # Check today's usage
            today = datetime.date.today()
            cur.execute("""
                SELECT calls_count FROM api_usage_tracking
                WHERE api_name = %s AND usage_date = %s
            """, (api_name, today))
            
            today_result = cur.fetchone()
            today_count = today_result[0] if today_result else 0
            
            # Check this month's usage
            month_start = today.replace(day=1)
            cur.execute("""
                SELECT SUM(calls_count) FROM api_usage_tracking
                WHERE api_name = %s AND usage_date >= %s
            """, (api_name, month_start))
            
            month_result = cur.fetchone()
            month_count = month_result[0] if month_result and month_result[0] else 0
            
            # Check against limits
            if today_count >= DAILY_SEARCH_LIMIT:
                print(f"Daily API limit reached ({today_count}/{DAILY_SEARCH_LIMIT})")
                return False
                
            if month_count >= MONTHLY_SEARCH_LIMIT:
                print(f"Monthly API limit reached ({month_count}/{MONTHLY_SEARCH_LIMIT})")
                return False
            
            return True
            
    except psycopg2.Error as e:
        print(f"Error checking API usage: {e}")
        # If we can't check limits, assume we're at limit as a safety measure
        return False

def track_api_call(conn, api_name='google_places'):
    """
    Record an API call in our usage tracking table.
    """
    try:
        with conn.cursor() as cur:
            today = datetime.date.today()
            
            # Try to update existing record for today
            cur.execute("""
                INSERT INTO api_usage_tracking (api_name, usage_date, calls_count)
                VALUES (%s, %s, 1)
                ON CONFLICT (api_name, usage_date) 
                DO UPDATE SET calls_count = api_usage_tracking.calls_count + 1
            """, (api_name, today))
            
            conn.commit()
            return True
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error tracking API usage: {e}")
        return False

def check_venue_cache(conn, venue_name, city=None):
    """
    Check if we already have this venue in our cache.
    Returns venue data if found, None otherwise.
    """
    try:
        with conn.cursor() as cur:
            if city:
                # If we have city info, use it for more accurate lookup
                cur.execute("""
                    SELECT venue_name, venue_address, formatted_address, 
                           latitude, longitude, place_id 
                    FROM venue_cache
                    WHERE LOWER(venue_name) = LOWER(%s) AND LOWER(city) = LOWER(%s)
                """, (venue_name, city))
            else:
                # Otherwise just check by venue name
                cur.execute("""
                    SELECT venue_name, venue_address, formatted_address, 
                           latitude, longitude, place_id 
                    FROM venue_cache
                    WHERE LOWER(venue_name) = LOWER(%s)
                """, (venue_name,))
            
            result = cur.fetchone()
            if result:
                return {
                    'venue_name': result[0],
                    'venue_address': result[1], 
                    'formatted_address': result[2],
                    'latitude': result[3],
                    'longitude': result[4],
                    'place_id': result[5]
                }
            return None
            
    except psycopg2.Error as e:
        print(f"Error checking venue cache: {e}")
        return None

def save_venue_to_cache(conn, venue_data):
    """
    Save venue data to the cache for future lookups.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO venue_cache 
                (venue_name, venue_address, city, formatted_address, latitude, longitude, place_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_name, city) DO UPDATE SET
                venue_address = EXCLUDED.venue_address,
                formatted_address = EXCLUDED.formatted_address,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                place_id = EXCLUDED.place_id
            """, (
                venue_data['venue_name'],
                venue_data.get('venue_address'),
                venue_data.get('city'),
                venue_data.get('formatted_address'),
                venue_data.get('latitude'),
                venue_data.get('longitude'),
                venue_data.get('place_id')
            ))
            
            conn.commit()
            return True
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error saving to venue cache: {e}")
        return False

def resolve_venue_with_google_places(venue_name, original_address=None, city=None):
    """
    Resolve a venue's address using Google Places API.
    
    Args:
        venue_name: Name of the venue to look up
        original_address: Original address if available (for better search)
        city: City where the venue is located (for better search)
        
    Returns:
        Dict with venue data or None if lookup failed
    """
    if not GOOGLE_PLACES_API_KEY:
        print("Error: Google Places API key not set")
        return None
        
    # Construct search query with as much info as we have
    query = venue_name
    if city:
        query += f" {city}"
    elif original_address and len(original_address) > 5:
        # If address is substantial, add it to improve search
        # But don't add very short addresses as they might confuse the search
        query += f" {original_address}"
    
    # Call the Places API
    params = {
        'query': query,
        'key': GOOGLE_PLACES_API_KEY
    }
    
    try:
        response = requests.get(PLACES_SEARCH_URL, params=params)
        data = response.json()
        
        # Check if API call was successful
        if data['status'] != 'OK':
            print(f"Places API error: {data['status']}")
            if 'error_message' in data:
                print(f"Error message: {data['error_message']}")
            return None
            
        # Get the first (best) result
        if data['results']:
            place = data['results'][0]
            
            # Extract venue data
            venue_data = {
                'venue_name': venue_name,
                'venue_address': original_address,
                'city': city,
                'formatted_address': place['formatted_address'],
                'latitude': place['geometry']['location']['lat'],
                'longitude': place['geometry']['location']['lng'],
                'place_id': place['place_id']
            }
            
            return venue_data
        else:
            print(f"No results found for venue: {venue_name}")
            return None
            
    except Exception as e:
        print(f"Error calling Places API: {e}")
        return None

def resolve_venue_address(venue_name, original_address=None, city=None):
    """
    Main function to resolve a venue's address.
    First checks cache, then falls back to API with usage tracking.
    
    Args:
        venue_name: Name of the venue to look up
        original_address: Original address if available
        city: City name if available
        
    Returns:
        Dict with venue data or None if lookup failed
    """
    # Connect to database
    conn = initialize_db()
    if not conn:
        return None
    
    try:
        # Ensure required tables exist
        setup_venue_cache_table(conn)
        
        # Try to get from cache first
        cached_data = check_venue_cache(conn, venue_name, city)
        if cached_data:
            print(f"Found venue in cache: {venue_name}")
            return cached_data
            
        # Check if we're within API usage limits
        if not check_api_usage_limits(conn):
            print(f"API usage limit reached. Skipping lookup for: {venue_name}")
            return None
            
        # Call the API
        print(f"Looking up venue with Google Places API: {venue_name}")
        venue_data = resolve_venue_with_google_places(venue_name, original_address, city)
        
        # Track the API call
        track_api_call(conn)
        
        # If we got results, save to cache
        if venue_data:
            save_venue_to_cache(conn, venue_data)
            return venue_data
            
        return None
        
    finally:
        if conn:
            conn.close()

def get_api_usage_stats():
    """
    Get current API usage statistics.
    Returns a dict with usage data.
    """
    conn = initialize_db()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            # Get today's usage
            today = datetime.date.today()
            cur.execute("""
                SELECT calls_count FROM api_usage_tracking
                WHERE api_name = 'google_places' AND usage_date = %s
            """, (today,))
            
            today_result = cur.fetchone()
            today_count = today_result[0] if today_result else 0
            
            # Get this month's usage
            month_start = today.replace(day=1)
            cur.execute("""
                SELECT SUM(calls_count) FROM api_usage_tracking
                WHERE api_name = 'google_places' AND usage_date >= %s
            """, (month_start,))
            
            month_result = cur.fetchone()
            month_count = month_result[0] if month_result and month_result[0] else 0
            
            # Get total cached venues
            cur.execute("SELECT COUNT(*) FROM venue_cache")
            cache_count = cur.fetchone()[0]
            
            return {
                'daily_usage': today_count,
                'daily_limit': DAILY_SEARCH_LIMIT,
                'monthly_usage': month_count,
                'monthly_limit': MONTHLY_SEARCH_LIMIT,
                'daily_percent': round((today_count / DAILY_SEARCH_LIMIT) * 100, 1) if today_count else 0,
                'monthly_percent': round((month_count / MONTHLY_SEARCH_LIMIT) * 100, 1) if month_count else 0,
                'cached_venues': cache_count
            }
            
    except psycopg2.Error as e:
        print(f"Error getting API usage stats: {e}")
        return None
    finally:
        if conn:
            conn.close()

def main():
    """Command-line interface for testing venue resolution."""
    load_dotenv()
    
    if len(sys.argv) < 2:
        print("Usage examples:")
        print("  python places_api_helper.py stats")
        print("  python places_api_helper.py lookup \"Venue Name\" [\"City\"]")
        print("  python places_api_helper.py fix-records [limit]")
        return
    
    command = sys.argv[1].lower()
    
    if command == "stats":
        # Show API usage statistics
        stats = get_api_usage_stats()
        if stats:
            print("\n=== Google Places API Usage ===")
            print(f"Daily usage: {stats['daily_usage']}/{stats['daily_limit']} ({stats['daily_percent']}%)")
            print(f"Monthly usage: {stats['monthly_usage']}/{stats['monthly_limit']} ({stats['monthly_percent']}%)")
            print(f"Cached venues: {stats['cached_venues']}")
        
    elif command == "lookup":
        # Test venue lookup
        if len(sys.argv) < 3:
            print("Please provide a venue name to look up")
            return
            
        venue_name = sys.argv[2]
        city = sys.argv[3] if len(sys.argv) > 3 else None
        
        print(f"Looking up venue: {venue_name}" + (f" in {city}" if city else ""))
        
        result = resolve_venue_address(venue_name, city=city)
        if result:
            print("\nResults:")
            print(f"Formatted address: {result['formatted_address']}")
            print(f"Coordinates: {result['latitude']}, {result['longitude']}")
            print(f"Place ID: {result['place_id']}")
        else:
            print("Venue lookup failed")
            
    elif command == "fix-records":
        # Process error records from event_raw
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
        
        # Connect to the database
        conn = initialize_db()
        if not conn:
            return
            
        try:
            with conn.cursor() as cur:
                # Get error records
                limit_clause = f"LIMIT {limit}" if limit else ""
                cur.execute(f"""
                    SELECT id, raw_json
                    FROM event_raw 
                    WHERE normalization_status = 'error'
                    ORDER BY id
                    {limit_clause}
                """)
                
                error_records = cur.fetchall()
                print(f"Found {len(error_records)} error records to fix")
                
                fixed_count = 0
                for record in error_records:
                    record_id = record[0]
                    raw_json = record[1]
                    
                    print(f"\nProcessing record {record_id}...")
                    
                    if not isinstance(raw_json, dict) or 'location' not in raw_json:
                        print("  Missing location data, skipping")
                        continue
                        
                    location = raw_json['location']
                    if not isinstance(location, dict):
                        print("  Invalid location format, skipping")
                        continue
                        
                    venue_name = location.get('name')
                    address = location.get('address')
                    
                    if not venue_name:
                        print("  Missing venue name, skipping")
                        continue
                        
                    # Try to extract city from address or event title
                    city = None
                    if address and isinstance(address, str) and ',' in address:
                        parts = address.split(',')
                        if len(parts) >= 2:
                            # Take the last meaningful part as city
                            for i in range(len(parts)-1, 0, -1):
                                potential_city = parts[i].strip()
                                if len(potential_city) > 2 and not potential_city.isdigit():
                                    city = potential_city
                                    break
                    
                    # If city not found in address, try looking in the event title
                    if not city and 'name' in raw_json:
                        title = raw_json['name']
                        for common_city in ['New York', 'San Francisco', 'Los Angeles', 'Chicago', 'London', 
                                           'Paris', 'Barcelona', 'Madrid', 'Berlin', 'Rio', 'Tokyo']:
                            if common_city in title:
                                city = common_city
                                break
                    
                    print(f"  Looking up venue: {venue_name}" + (f" in {city}" if city else ""))
                    venue_data = resolve_venue_address(venue_name, address, city)
                    
                    if venue_data:
                        # Update the record with resolved location
                        updated_location = {
                            "@type": "Place",
                            "name": venue_name
                        }
                        
                        if venue_data['formatted_address']:
                            updated_location["address"] = venue_data['formatted_address']
                            
                        if venue_data['latitude'] and venue_data['longitude']:
                            updated_location["geo"] = {
                                "@type": "GeoCoordinates",
                                "latitude": venue_data['latitude'],
                                "longitude": venue_data['longitude']
                            }
                            
                        # Update the raw_json
                        raw_json['location'] = updated_location
                        
                        # Reset normalization status to try again
                        cur.execute("""
                            UPDATE event_raw
                            SET raw_json = %s::jsonb,
                                normalized_at = NULL,
                                normalization_status = NULL
                            WHERE id = %s
                        """, (Json(raw_json), record_id))
                        
                        conn.commit()
                        fixed_count += 1
                        print(f"  Successfully updated location for record {record_id}")
                    else:
                        print(f"  Failed to resolve venue for record {record_id}")
                
                print(f"\nFixed {fixed_count} out of {len(error_records)} error records")
                
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Database error: {e}")
        finally:
            conn.close()
            
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main() 