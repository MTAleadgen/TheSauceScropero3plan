import os
import psycopg2
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database connection
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Get the latest records
print("Latest 5 records:")
cur.execute('''
    SELECT id, source, source_event_id, discovered_at 
    FROM event_raw 
    ORDER BY id DESC 
    LIMIT 5
''')
rows = cur.fetchall()
for row in rows:
    print(row)
print("\n")

# Check for valid organic search results
print("Checking for valid organic search results...")
cur.execute('''
    SELECT id, source, source_event_id, discovered_at, 
           (raw_json::jsonb -> 'tasks' -> 0 ->> 'status_code')::int as task_status_code,
           raw_json::jsonb -> 'tasks' -> 0 ->> 'status_message' as task_status_message
    FROM event_raw 
    WHERE source = 'dataforseo_organic_raw'
    ORDER BY id DESC 
    LIMIT 10
''')
rows = cur.fetchall()
for row in rows:
    print(f"ID: {row[0]}, Source: {row[1]}, Event ID: {row[2]}, Status Code: {row[4]}, Message: {row[5]}")

# Check for individual tasks by ID
print("\nChecking specific record contents:")
cur.execute('''
    SELECT id, raw_json::jsonb 
    FROM event_raw 
    WHERE source = 'dataforseo_organic_raw'
    ORDER BY id DESC 
    LIMIT 1
''')
row = cur.fetchone()
if row:
    record_id = row[0]
    raw_json = row[1]
    print(f"Record ID: {record_id}")
    if "tasks" in raw_json and len(raw_json["tasks"]) > 0:
        task = raw_json["tasks"][0]
        print(f"Task status code: {task.get('status_code')}")
        print(f"Task status message: {task.get('status_message')}")
        print(f"Has result?: {'Yes' if task.get('result') else 'No'}")
        print(f"Task data: {json.dumps(task.get('data', {}), indent=2)}")
    else:
        print("No tasks found in the record")

# Close connection
cur.close()
conn.close() 