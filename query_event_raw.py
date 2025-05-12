import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('.env')

def query_event_details(event_ids):
    conn = None
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        placeholders = '%s, ' * len(event_ids)
        placeholders = placeholders.rstrip(', ')
        # Querying for id, source, discovered_at, normalized_at, normalization_status
        query = f"SELECT id, source, discovered_at, normalized_at, normalization_status FROM event_raw WHERE id IN ({placeholders});"
        cur.execute(query, tuple(event_ids))
        records = cur.fetchall()
        
        if records:
            print(f"Details for specified events:")
            for record in records:
                print(f"  ID={record[0]}, Source='{record[1]}', DiscoveredAt={record[2]}, NormalizedAt={record[3]}, Status='{record[4]}'")
        else:
            print(f"No records found for IDs: {event_ids}")
        
        # Count events with normalized_at IS NULL
        cur.execute("SELECT COUNT(*) FROM event_raw WHERE normalized_at IS NULL;")
        count = cur.fetchone()[0]
        print(f"\nTotal events with normalized_at IS NULL: {count}")

        cur.close()
    except Exception as e:
        print(f"Database query error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    query_event_details([1, 2, 121, 122]) # Query a few relevant event IDs 