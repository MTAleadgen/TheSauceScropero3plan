#!/usr/bin/env python3
"""
View an event_raw record in detail to understand why events aren't being detected
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

def view_record(record_id):
    """View a specific event_raw record"""
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        
        # Fetch the record
        with conn.cursor() as cur:
            cur.execute("SELECT id, source, source_event_id, metro_id, raw_json FROM event_raw WHERE id = %s", (record_id,))
            row = cur.fetchone()
            
            if row:
                record_id, source, source_event_id, metro_id, raw_json = row
                print(f"Record ID: {record_id}")
                print(f"Source: {source}")
                print(f"Source Event ID: {source_event_id}")
                print(f"Metro ID: {metro_id}")
                
                # Parse the JSON
                if isinstance(raw_json, str):
                    data = json.loads(raw_json)
                else:
                    data = raw_json
                
                # Get metadata
                metadata = data.get("_script_discovery_metadata", {})
                print(f"\nMetadata:")
                for key, value in metadata.items():
                    print(f"  {key}: {value}")
                
                # Extract tasks
                tasks = data.get("tasks", [])
                if tasks:
                    task = tasks[0]
                    print(f"\nTask ID: {task.get('id')}")
                    print(f"Task Status: {task.get('status_code')} - {task.get('status_message')}")
                    
                    # Print task data
                    task_data = task.get('data', {})
                    print(f"\nTask Data:")
                    for key, value in task_data.items():
                        print(f"  {key}: {value}")
                    
                    # Check result
                    result = task.get("result", [])
                    if result:
                        print(f"\nResult contains {len(result)} items:")
                        for i, item in enumerate(result):
                            item_type = item.get("type", "unknown")
                            print(f"  Item {i}: type={item_type}")
                            
                            # If this is an events section, print info about the events
                            if item_type == "events":
                                events_items = item.get("items", [])
                                if events_items:
                                    print(f"    Contains {len(events_items)} events:")
                                    for j, event in enumerate(events_items[:3]):  # Show first 3 events
                                        print(f"      Event {j}:")
                                        print(f"        Title: {event.get('title', '(no title)')}")
                                        print(f"        Snippet: {event.get('snippet', '(no snippet)')}")
                                    if len(events_items) > 3:
                                        print(f"      ... and {len(events_items) - 3} more events")
                                else:
                                    print("    No events found in events section!")
                            
                            # If this is a local_pack, print venue info
                            elif item_type == "local_pack":
                                print(f"    Venue: {item.get('title', '(no title)')}")
                                print(f"    Description: {item.get('description', '(no description)')[:100]}...")
                    else:
                        print("\nNo result data found in task!")
                else:
                    print("\nNo tasks found in record!")
            else:
                print(f"No record found with ID {record_id}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python view_record.py RECORD_ID")
        return
    
    try:
        record_id = int(sys.argv[1])
        view_record(record_id)
    except ValueError:
        print("ERROR: Record ID must be an integer")

if __name__ == "__main__":
    main() 