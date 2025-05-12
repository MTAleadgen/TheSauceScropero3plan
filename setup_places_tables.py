#!/usr/bin/env python3
"""
setup_places_tables.py

Script to set up the necessary database tables for Google Places API integration.
"""

import os
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

def setup_tables(conn):
    """Set up the required tables for Google Places API integration."""
    try:
        with conn.cursor() as cur:
            # Create venue cache table
            print("Creating venue_cache table...")
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
            
            # Create API usage tracking table
            print("Creating api_usage_tracking table...")
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
            print("Tables created successfully.")
            
            # Check if tables were created
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'venue_cache'")
            venue_table_exists = cur.fetchone()[0] > 0
            
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'api_usage_tracking'")
            api_table_exists = cur.fetchone()[0] > 0
            
            if venue_table_exists and api_table_exists:
                print("Verification: Both tables exist in the database.")
            else:
                print(f"Verification: venue_cache exists: {venue_table_exists}, api_usage_tracking exists: {api_table_exists}")
                
            return True
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error setting up tables: {e}")
        return False

def main():
    """Main function."""
    load_dotenv()
    
    conn = initialize_db()
    if not conn:
        return
    
    try:
        setup_tables(conn)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 