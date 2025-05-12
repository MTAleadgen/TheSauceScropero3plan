import os
import json
import glob
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
import logging
from dotenv import load_dotenv
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_env():
    """Load environment variables from .env file"""
    if not load_dotenv('.env.local'):
        load_dotenv('.env')
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not found in environment variables")
        return None
    return database_url

def connect_to_db(database_url):
    """Connect to the Supabase database"""
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        logger.info("Successfully connected to the database")
        return conn, cursor
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return None, None

def get_json_files_from_data_raw():
    """Get all JSON files from data_raw directory"""
    data_raw_dir = "./data_raw"
    json_files = []
    
    # Get all JSON files in main directory
    json_files.extend(glob.glob(f"{data_raw_dir}/*.json"))
    
    # Get all JSON files in subdirectories
    for subdir in ["dance_queries_enhanced", "dance_queries", "aggressive"]:
        subdir_path = os.path.join(data_raw_dir, subdir)
        if os.path.exists(subdir_path):
            json_files.extend(glob.glob(f"{subdir_path}/*.json"))
    
    return json_files

def create_data_raw_table_if_not_exists(cursor):
    """Create the data_raw table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data_raw (
        id SERIAL PRIMARY KEY,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_type TEXT NOT NULL,
        dance_style TEXT,
        city TEXT,
        data JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(file_path)
    );
    """
    try:
        cursor.execute(create_table_query)
        logger.info("Ensured data_raw table exists")
        return True
    except Exception as e:
        logger.error(f"Failed to create data_raw table: {e}")
        return False

def extract_metadata_from_filepath(file_path):
    """Extract metadata from the file path"""
    file_name = os.path.basename(file_path)
    dir_name = os.path.dirname(file_path)
    
    # Determine file type based on directory
    if "dance_queries_enhanced" in dir_name:
        file_type = "dance_queries_enhanced"
    elif "dance_queries" in dir_name:
        file_type = "dance_queries"
    elif "aggressive" in dir_name:
        file_type = "aggressive"
    else:
        file_type = "general"
    
    # Try to extract city and dance style from filename
    dance_style = None
    city = None
    
    name_parts = file_name.split('_')
    
    # Check for dance style
    for style in ["salsa", "bachata", "kizomba", "zouk", "cumbia", 
                  "rumba", "tango", "hustle", "chacha", "coast", 
                  "lambada", "samba", "ballroom", "forro"]:
        if style in file_name.lower():
            dance_style = style
            if style == "coast":
                dance_style = "coast swing"
            break
    
    # Check for city names (common ones in the dataset)
    common_cities = ["new_york", "london", "paris", "berlin", "madrid", 
                     "chicago", "los_angeles", "miami", "sao_paulo", 
                     "rio_de_janeiro", "mexico_city", "istanbul"]
    
    for city_name in common_cities:
        if city_name in file_name.lower():
            city = city_name.replace("_", " ")
            break
    
    return {
        "file_name": file_name,
        "file_path": file_path,
        "file_type": file_type,
        "dance_style": dance_style,
        "city": city
    }

def upload_file_to_supabase(cursor, file_path):
    """Upload a single JSON file to Supabase"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract metadata
        metadata = extract_metadata_from_filepath(file_path)
        
        # Check if this file already exists in the database
        cursor.execute(
            "SELECT id FROM data_raw WHERE file_path = %s",
            (metadata["file_path"],)
        )
        existing = cursor.fetchone()
        
        if existing:
            logger.info(f"File already exists in database: {metadata['file_name']}")
            return False
        
        # Insert new record
        cursor.execute(
            """
            INSERT INTO data_raw 
            (file_name, file_path, file_type, dance_style, city, data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                metadata["file_name"],
                metadata["file_path"],
                metadata["file_type"],
                metadata["dance_style"],
                metadata["city"],
                Json(data)
            )
        )
        
        logger.info(f"Uploaded file to database: {metadata['file_name']}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file {file_path}: {e}")
        return False

def main():
    """Main function to upload data to Supabase"""
    # Load environment variables
    database_url = load_env()
    if not database_url:
        return
    
    # Connect to database
    conn, cursor = connect_to_db(database_url)
    if not conn or not cursor:
        return
    
    try:
        # Create data_raw table if it doesn't exist
        if not create_data_raw_table_if_not_exists(cursor):
            return
        
        # Get all JSON files from data_raw directory
        json_files = get_json_files_from_data_raw()
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        # Upload each file to Supabase
        success_count = 0
        for file_path in json_files:
            if upload_file_to_supabase(cursor, file_path):
                success_count += 1
                conn.commit()
            
            # Small delay to avoid overwhelming the DB
            time.sleep(0.1)
        
        logger.info(f"Successfully uploaded {success_count} of {len(json_files)} files to Supabase")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main() 