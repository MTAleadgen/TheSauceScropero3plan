import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('.env') # Ensure it loads from .env

def reset_event_status(event_id):
    conn = None
    updated_rows = 0
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        cur.execute("""
            UPDATE event_raw 
            SET normalized_at = NULL, normalization_status = NULL
            WHERE id = %s;
        """, (event_id,))
        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
        if updated_rows > 0:
            print(f"Successfully reset status for event_raw_id {event_id}.")
        else:
            print(f"No event found with id = {event_id} to update, or status was already NULL.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database error while resetting status for event_id {event_id}: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    reset_event_status(121) 