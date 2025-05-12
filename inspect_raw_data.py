#!/usr/bin/env python3
"""
Inspect the raw JSON data from DataForSEO to understand its structure.
This tool shows the full JSON structure with pretty-printing for better analysis.
"""
import os
import sys
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get database connection string
DATABASE_URL = os.getenv("DATABASE_URL")

def inspect_raw_data(record_id):
    """
    Retrieve and display the raw JSON data for a specified event_raw record
    with pretty-printing for readability.
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        
        # Fetch the record
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, source, source_event_id, metro_id, raw_json FROM event_raw WHERE id = %s", 
                (record_id,)
            )
            row = cur.fetchone()
            
            if row:
                record_id, source, source_event_id, metro_id, raw_json = row
                print(f"Record ID: {record_id}")
                print(f"Source: {source}")
                print(f"Source Event ID: {source_event_id}")
                print(f"Metro ID: {metro_id}")
                
                # Parse and pretty print the JSON
                if isinstance(raw_json, str):
                    data = json.loads(raw_json)
                else:
                    data = raw_json
                
                print("\nRAW JSON STRUCTURE:")
                print("====================")
                
                # Pretty print with indentation
                json_pretty = json.dumps(data, indent=2, sort_keys=False)
                print(json_pretty)
                
                # Summarize the top-level keys
                print("\nTOP LEVEL KEYS:")
                print("================")
                for key in data.keys():
                    print(f"- {key}")
                
                # If there are tasks, print their structure
                tasks = data.get("tasks", [])
                if tasks:
                    print("\nTASK STRUCTURE:")
                    print("===============")
                    task = tasks[0]  # Usually just one task
                    for key in task.keys():
                        print(f"- {key}")
                    
                    # Print result items if they exist
                    result = task.get("result", [])
                    if result:
                        print("\nRESULT ITEMS:")
                        print("=============")
                        for i, item in enumerate(result):
                            item_type = item.get("type", "unknown")
                            print(f"{i}. Type: {item_type}")
                            
                            # If it's an events section, summarize the events
                            if item_type == "events":
                                events_items = item.get("items", [])
                                print(f"   Contains {len(events_items)} events")
                            
                            # If it's organic results, summarize them
                            elif item_type == "organic":
                                organic_items = item.get("items", [])
                                print(f"   Contains {len(organic_items)} organic results")
                            
                            # For other types, just print the item keys
                            elif item_type in ["local_pack", "knowledge_graph"]:
                                print(f"   Keys: {', '.join(item.keys())}")
            else:
                print(f"No record found with ID {record_id}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Main function to parse command line arguments and execute the tool"""
    if len(sys.argv) != 2:
        print("Usage: python inspect_raw_data.py RECORD_ID")
        return
    
    try:
        record_id = int(sys.argv[1])
        inspect_raw_data(record_id)
    except ValueError:
        print("ERROR: Record ID must be an integer")

if __name__ == "__main__":
    main() 