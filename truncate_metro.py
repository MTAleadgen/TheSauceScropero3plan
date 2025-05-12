import os
import psycopg2
from dotenv import load_dotenv

def truncate_metro_table():
    # Try loading .env.local first, then .env
    if not load_dotenv('.env.local'):
        load_dotenv('.env')
        
    database_url = os.getenv('DATABASE_URL')

    if not database_url:
        print("Error: DATABASE_URL not found in environment variables or .env.local file.")
        return

    conn = None
    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        truncate_command = "TRUNCATE TABLE metro CASCADE;"
        print(f"Executing: {truncate_command}")
        cur.execute(truncate_command)
        conn.commit()
        print("Successfully truncated the 'metro' table.")
        
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    truncate_metro_table() 