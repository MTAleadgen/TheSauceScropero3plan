#!/usr/bin/env python3
"""
Maps between DataForSEO location codes and our internal metro IDs.
Helps identify the correct location_code to use for a given city.
"""
import os
import sys
import argparse
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get database connection string
DATABASE_URL = os.getenv("DATABASE_URL")

def find_metros_by_name(city_name):
    """
    Find metro records by city name.
    
    Args:
        city_name: The name of the city to search for
        
    Returns:
        A list of matching metro records
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        
        # Search for matches
        with conn.cursor() as cur:
            # First try exact match
            cur.execute("""
                SELECT metro_id, name, asciiname, country_iso2, population
                FROM metro
                WHERE LOWER(name) = LOWER(%s)
                OR LOWER(asciiname) = LOWER(%s)
                ORDER BY population DESC
            """, (city_name, city_name))
            
            rows = cur.fetchall()
            
            # If no exact match, try fuzzy match
            if not rows:
                cur.execute("""
                    SELECT metro_id, name, asciiname, country_iso2, population
                    FROM metro
                    WHERE LOWER(name) LIKE LOWER(%s)
                    OR LOWER(asciiname) LIKE LOWER(%s)
                    OR %s = ANY(STRING_TO_ARRAY(LOWER(alternatenames), ','))
                    ORDER BY population DESC
                    LIMIT 10
                """, (f"%{city_name}%", f"%{city_name}%", city_name.lower()))
                
                rows = cur.fetchall()
            
            return rows
        
    except Exception as e:
        print(f"Error: {e}")
        return []

def find_dataforseo_codes():
    """
    Find all DataForSEO location codes from event_raw records
    
    Returns:
        A dictionary mapping location codes to city info
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        
        codes = {}
        
        # Find location codes in raw events
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT raw_json->'tasks'->0->'data'->>'location_code' as location_code,
                       raw_json->'tasks'->0->'data'->>'keyword' as keyword,
                       metro_id
                FROM event_raw
                WHERE source = 'dataforseo_serp'
                AND raw_json->'tasks'->0->'data'->>'location_code' IS NOT NULL
            """)
            
            for row in cur.fetchall():
                location_code, keyword, metro_id = row
                
                # Extract city name from keyword (typically "dance_style in City")
                keyword_parts = keyword.split(' in ')
                city_name = keyword_parts[1] if len(keyword_parts) > 1 else keyword
                
                # Get metro info
                cur2 = conn.cursor()
                cur2.execute("""
                    SELECT name, country_iso2
                    FROM metro
                    WHERE metro_id = %s
                """, (metro_id,))
                metro_row = cur2.fetchone()
                metro_name = metro_row[0] if metro_row else "Unknown"
                country = metro_row[1] if metro_row else "--"
                
                # Store in dictionary
                codes[location_code] = {
                    'location_code': location_code,
                    'metro_id': metro_id,
                    'metro_name': metro_name,
                    'country': country,
                    'search_term': keyword
                }
        
        return codes
        
    except Exception as e:
        print(f"Error: {e}")
        return {}

def suggest_location_code(city_name):
    """
    Suggest a DataForSEO location code for a given city name
    
    Args:
        city_name: The name of the city
    """
    # First find matching metros
    metros = find_metros_by_name(city_name)
    
    if not metros:
        print(f"No metro records found for '{city_name}'")
        return
    
    print(f"Found {len(metros)} matching metro records:")
    for i, metro in enumerate(metros):
        metro_id, name, asciiname, country, population = metro
        print(f"{i+1}. Metro ID: {metro_id}, Name: {name}, Country: {country}, Population: {population}")
    
    # Find known DataForSEO location codes
    codes = find_dataforseo_codes()
    
    if not codes:
        print("\nNo DataForSEO location codes found in event_raw records.")
        return
    
    # Match metros with known codes
    matches = []
    for metro in metros:
        metro_id = metro[0]
        for code, info in codes.items():
            if info['metro_id'] == metro_id:
                matches.append(info)
    
    if matches:
        print("\nFound DataForSEO location codes for these metros:")
        for info in matches:
            print(f"Location Code: {info['location_code']}, Metro: {info['metro_name']} ({info['country']})")
            print(f"Example search: {info['search_term']}")
            print(f"Usage: --location-code {info['location_code']} --metro-id {info['metro_id']}")
            print()
    else:
        print("\nNo existing DataForSEO location codes found for these metros.")
        print("You will need to look up the appropriate code in the DataForSEO documentation.")

def main():
    """Parse command line arguments and execute the appropriate function"""
    parser = argparse.ArgumentParser(description="Map between DataForSEO location codes and metro IDs.")
    parser.add_argument("--city", type=str, help="Find metro IDs and location codes for a city name")
    parser.add_argument("--list-codes", action="store_true", help="List all known DataForSEO location codes")
    
    args = parser.parse_args()
    
    if args.city:
        suggest_location_code(args.city)
    elif args.list_codes:
        codes = find_dataforseo_codes()
        print(f"Found {len(codes)} DataForSEO location codes:")
        for code, info in codes.items():
            print(f"Code: {code}, Metro: {info['metro_name']} ({info['country']}), ID: {info['metro_id']}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 