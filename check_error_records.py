#!/usr/bin/env python3
"""
check_error_records.py

Analyzes records that failed normalization to understand why
and how an LLM might help process them.
"""

import os
import sys
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

def initialize_db():
    """Initialize database connection."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL environment variable not set.")
        return None
    
    try:
        conn = psycopg2.connect(db_url, cursor_factory=DictCursor)
        print("Connected to database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def check_error_records(db_conn):
    """Analyze records that failed normalization."""
    try:
        with db_conn.cursor() as cur:
            # Get count of error records
            cur.execute("""
                SELECT COUNT(*) FROM event_raw
                WHERE normalization_status = 'error'
            """)
            error_count = cur.fetchone()[0]
            print(f"Found {error_count} records with normalization_status = 'error'")
            
            if error_count == 0:
                return
            
            # Get error records
            cur.execute("""
                SELECT id, source, metro_id, raw_json
                FROM event_raw
                WHERE normalization_status = 'error'
                ORDER BY id
            """)
            error_records = cur.fetchall()
            
            print("\nAnalyzing error records to understand potential issues:")
            
            # Common issues to check for
            missing_fields = 0
            missing_title = 0
            missing_date = 0
            missing_location = 0
            malformed_data = 0
            unknown_issues = 0
            
            for record in error_records:
                record_id = record['id']
                raw_json = record['raw_json']
                print(f"\n--- Record ID: {record_id} ---")
                
                # Basic analysis of issues
                issues = []
                
                # Check if raw_json is valid
                if not isinstance(raw_json, dict):
                    issues.append("Invalid raw_json format")
                    malformed_data += 1
                    continue
                
                # Check for essential fields
                if 'name' not in raw_json:
                    issues.append("Missing title (name)")
                    missing_title += 1
                    missing_fields += 1
                
                if 'startDate' not in raw_json:
                    issues.append("Missing start date")
                    missing_date += 1
                    missing_fields += 1
                
                # Check for location data
                has_location = False
                if 'location' in raw_json:
                    location = raw_json.get('location')
                    if isinstance(location, dict):
                        has_venue_name = 'name' in location
                        has_address = False
                        
                        if 'address' in location:
                            address = location['address']
                            if isinstance(address, dict) or isinstance(address, str):
                                has_address = True
                        
                        has_location = has_venue_name or has_address
                
                if not has_location:
                    issues.append("Incomplete or missing location data")
                    missing_location += 1
                    missing_fields += 1
                
                # Print summary of issues
                if issues:
                    print(f"Issues identified:")
                    for issue in issues:
                        print(f"  - {issue}")
                else:
                    print("No obvious issues identified - might be a geocoding or fingerprint failure")
                    unknown_issues += 1
                
                # Show a snippet of the raw data
                print("\nData snippet:")
                if isinstance(raw_json, dict):
                    print(f"  Title: {raw_json.get('name')}")
                    print(f"  Start Date: {raw_json.get('startDate')}")
                    if 'location' in raw_json:
                        location = raw_json['location']
                        if isinstance(location, dict):
                            print(f"  Venue: {location.get('name')}")
                            if 'address' in location:
                                addr = location['address']
                                if isinstance(addr, str):
                                    print(f"  Address: {addr}")
                                elif isinstance(addr, dict):
                                    print(f"  Address: {addr}")
                else:
                    print(f"  Raw data: {str(raw_json)[:100]}...")
            
            # Print summary
            print("\n=== Summary of Issues ===")
            print(f"Total error records: {error_count}")
            print(f"Records with missing fields: {missing_fields}")
            print(f"- Missing title: {missing_title}")
            print(f"- Missing date: {missing_date}")
            print(f"- Missing location: {missing_location}")
            print(f"Records with malformed data: {malformed_data}")
            print(f"Records with unknown issues: {unknown_issues}")
            
            print("\n=== How an LLM Could Help ===")
            print("1. Missing data: LLM could extract implied information from descriptions or other fields")
            print("2. Location enrichment: LLM could parse address text and extract structured components")
            print("3. Date parsing: LLM could handle complex or unusual date formats")
            print("4. Content enrichment: LLM could generate missing descriptions or categorize events")
            print("5. Error recovery: LLM could suggest fixes for malformed data")
    
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    """Main function."""
    load_dotenv()
    
    db_conn = initialize_db()
    if not db_conn:
        sys.exit(1)
    
    try:
        check_error_records(db_conn)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 