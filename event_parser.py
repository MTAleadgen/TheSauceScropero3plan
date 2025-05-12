#!/usr/bin/env python3
"""
Parse event_raw data from DataForSEO searches and populate event_clean table.
This script extracts structured event data from the raw search results.
"""
import os
import sys
import json
import logging
import psycopg2
from psycopg2.extras import Json, execute_values
import re
import hashlib
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse as parse_date
import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Database connection established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def get_unparsed_event_raws(db_conn, limit=None):
    """Get event_raw records that haven't been parsed yet."""
    try:
        with db_conn.cursor() as cur:
            query = """
                SELECT id, source, source_event_id, metro_id, raw_json
                FROM event_raw
                WHERE source = 'dataforseo_serp'
                AND parsed_at IS NULL
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cur.execute(query)
            rows = cur.fetchall()
            logger.info(f"Found {len(rows)} unparsed event_raw records")
            return rows
    except Exception as e:
        logger.error(f"Error fetching unparsed records: {e}")
        return []

def extract_venue_from_location(location_info):
    """
    Extract venue name from location string.
    Examples:
    - "Stepping Out Studios New York, NY" -> "Stepping Out Studios"
    - "Palomas Bk New York, NY" -> "Palomas Bk"
    """
    # Try to match venue and city/state pattern
    venue_match = re.match(r"([^,]+)([A-Za-z\s]+, [A-Z]{2})", location_info)
    if venue_match:
        # If we have a match, take the first part
        venue_candidate = venue_match.group(1).strip()
        
        # Remove common city references that might be part of the string
        city_refs = ["New York", "New Yor", "NYC", "NY City", "New York City"]
        for city_ref in city_refs:
            if venue_candidate.endswith(city_ref):
                venue_candidate = venue_candidate[:-len(city_ref)].strip()
        
        # If there are multiple words, take all but the last one (which might be the city)
        if " " in venue_candidate and len(venue_candidate.split()) > 1:
            words = venue_candidate.split()
            # Check if the last word might be part of a common city name
            if words[-1].lower() in ["new", "los", "san", "las"]:
                # Keep the last word if it's part of a common city name
                return venue_candidate
            else:
                return " ".join(words)
        else:
            return venue_candidate
    
    # If we can't match the pattern, return the first part of the string
    parts = location_info.split(" ", 2)
    if len(parts) > 1:
        return " ".join(parts[:2])
    return parts[0] if parts else ""

def extract_events_from_json(raw_record):
    """
    Extract structured event data from DataForSEO JSON.
    
    Args:
        raw_record: Tuple of (id, source, source_event_id, metro_id, raw_json)
    
    Returns:
        list: List of extracted event dictionaries
    """
    event_raw_id, source, source_event_id, metro_id, raw_json = raw_record
    
    extracted_events = []
    current_year = datetime.now().year
    
    try:
        # Parse the raw JSON
        if isinstance(raw_json, str):
            data = json.loads(raw_json)
        else:
            data = raw_json
        
        # Get metadata (city, dance_style)
        metadata = data.get("_script_discovery_metadata", {})
        city_name = metadata.get("script_city_name", "")
        dance_style = metadata.get("script_dance_style", "")
        
        # Extract tasks array
        tasks = data.get("tasks", [])
        if not tasks:
            logger.warning(f"No tasks found in event_raw record {event_raw_id}")
            return []
        
        # Get the result from the first task
        result = tasks[0].get("result", [])
        if not result:
            logger.warning(f"No result found in event_raw record {event_raw_id}")
            return []
        
        # Log the types of items in the result for debugging
        item_types = [item.get("type", "unknown") for item in result]
        logger.info(f"Result contains item types: {item_types}")
        
        # Flag to track if we found events section
        events_section_found = False
        
        # Look for events section
        for item in result:
            if item.get("type") == "events":
                events_section_found = True
                events_items = item.get("items", [])
                logger.info(f"Found events section with {len(events_items)} items")
                
                for event in events_items:
                    # Extract event data
                    title = event.get("title", "")
                    snippet = event.get("snippet", "")
                    
                    if not title or not snippet:
                        continue
                    
                    # Parse date, time, venue, and location from snippet
                    # Example: "May 11, 7:00 PM Stepping Out Studios New York, NY"
                    date_time_match = re.match(r"([A-Za-z]+ \d+), (\d+:\d+ [AP]M)", snippet)
                    
                    if date_time_match:
                        date_str = date_time_match.group(1)
                        time_str = date_time_match.group(2)
                        
                        # Add current year (since DataForSEO doesn't provide it)
                        date_str = f"{date_str}, {current_year}"
                        
                        # Parse datetime
                        try:
                            dt_str = f"{date_str} {time_str}"
                            event_dt = parse_date(dt_str)
                            # Convert to UTC
                            if event_dt.tzinfo is None:
                                event_dt = pytz.timezone('US/Eastern').localize(event_dt)
                            start_ts = event_dt.astimezone(pytz.UTC)
                            
                            # Default end time is 3 hours after start using timedelta
                            end_ts = start_ts + timedelta(hours=3)
                        except Exception as e:
                            logger.warning(f"Failed to parse date/time '{dt_str}': {e}")
                            continue
                        
                        # Extract venue and location
                        # Remove date and time from snippet to get location info
                        location_info = snippet.replace(f"{date_time_match.group(0)}", "").strip()
                        
                        # Extract venue name using improved function
                        venue_name = extract_venue_from_location(location_info)
                        
                        # Extract price if present
                        price_match = re.search(r"\$(\d+)", snippet)
                        price_val = float(price_match.group(1)) if price_match else None
                        price_ccy = "USD" if price_match else None  # Currency code is 3 letters
                        
                        # Generate fingerprint (unique identifier for deduplication)
                        fingerprint_str = f"{title}|{start_ts.isoformat()}|{venue_name}|{metro_id}"
                        fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
                        
                        # Generate a description
                        description = f"{dance_style} event in {city_name}: {title} at {venue_name}"
                        
                        # Generate URL (we don't have actual URLs from the events listing)
                        url = None
                        
                        # Create event dict
                        event_dict = {
                            "event_raw_id": event_raw_id,
                            "source": source,
                            "source_event_id": f"{source_event_id}_{events_items.index(event)}",
                            "title": title,
                            "description": description,
                            "url": url,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "venue_name": venue_name,
                            "venue_address": location_info,
                            "venue_geom": None,  # We'd need geocoding here
                            "image_url": None,
                            "tags": [dance_style],
                            "metro_id": metro_id,
                            "price_val": price_val,
                            "price_ccy": price_ccy,
                            "fingerprint": fingerprint,
                            "quality_score": 0.6  # Default score, could be improved
                        }
                        
                        extracted_events.append(event_dict)
            
            # Check for organic results that might contain event information
            elif item.get("type") == "organic":
                organic_items = item.get("items", [])
                logger.info(f"Found organic section with {len(organic_items)} items")
                
                for organic in organic_items:
                    title = organic.get("title", "")
                    description = organic.get("description", "")
                    url = organic.get("url", "")
                    
                    # If this looks like an event listing site and has dance style keywords
                    if ((dance_style in title.lower() or dance_style in description.lower()) and
                        ("event" in title.lower() or "calendar" in title.lower())):
                        
                        logger.info(f"Found potential event listing: {title}")
                        
                        # Create a generic "resource" event to represent this listing source
                        # Use current date at 8PM for the event time
                        today = datetime.now()
                        start_ts = datetime(today.year, today.month, today.day, 20, 0, 0, tzinfo=pytz.UTC)
                        end_ts = datetime(today.year, today.month, today.day, 23, 0, 0, tzinfo=pytz.UTC)
                        
                        # Generate fingerprint
                        fingerprint_str = f"resource|{title}|{url}|{metro_id}"
                        fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
                        
                        # Create event dict for resource listing
                        event_dict = {
                            "event_raw_id": event_raw_id,
                            "source": source,
                            "source_event_id": f"{source_event_id}_resource_{hash(title) % 10000}",
                            "title": f"{dance_style.capitalize()} events: {title}",
                            "description": f"{dance_style} event resource: {description}",
                            "url": url,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "venue_name": "Online Resource",
                            "venue_address": url,
                            "venue_geom": None,
                            "image_url": None,
                            "tags": [dance_style, "resource"],
                            "metro_id": metro_id,
                            "price_val": None,
                            "price_ccy": None,  # Ensure this is 3 letters max
                            "fingerprint": fingerprint,
                            "quality_score": 0.4  # Lower score for resources
                        }
                        
                        extracted_events.append(event_dict)
                
            # Also check local_pack results (dance venues)
            elif item.get("type") == "local_pack":
                venue = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "rating": item.get("rating", {}).get("value"),
                    "phone": item.get("phone")
                }
                
                # Extract address from description
                address_match = re.match(r"([^\\n]+)", venue["description"]) if venue["description"] else None
                venue_address = address_match.group(1).strip() if address_match else ""
                
                # Extract opening hours info
                hours_match = re.search(r"(Opens|Closed)([^\\n]+)", venue["description"]) if venue["description"] else None
                hours_info = hours_match.group(0).strip() if hours_match else ""
                
                # Create a special "venue" event - these aren't actual events but places that host events
                if venue["title"] and ("salsa" in venue["title"].lower() or "dance" in venue["title"].lower()):
                    # Generate a start time for today at 8PM
                    today = datetime.now()
                    start_ts = datetime(today.year, today.month, today.day, 20, 0, 0, tzinfo=pytz.UTC)
                    end_ts = datetime(today.year, today.month, today.day, 23, 0, 0, tzinfo=pytz.UTC)
                    
                    # Create description
                    if hours_info:
                        description = f"{dance_style} venue: {venue['title']}. {hours_info}"
                    else:
                        description = f"{dance_style} venue: {venue['title']}"
                    
                    # Add review snippet if available
                    review_match = re.search(r"\"([^\"]+)\"", venue["description"]) if venue["description"] else None
                    if review_match:
                        description += f" Review: {review_match.group(1)}"
                    
                    # Generate fingerprint
                    fingerprint_str = f"venue|{venue['title']}|{venue_address}|{metro_id}"
                    fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
                    
                    # Create event dict for venue listing
                    event_dict = {
                        "event_raw_id": event_raw_id,
                        "source": source,
                        "source_event_id": f"{source_event_id}_venue_{hash(venue['title']) % 10000}",
                        "title": f"{venue['title']} - {dance_style} venue",
                        "description": description,
                        "url": venue["url"],
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "venue_name": venue["title"],
                        "venue_address": venue_address,
                        "venue_geom": None,  # We'd need geocoding here
                        "image_url": None, 
                        "tags": [dance_style, "venue"],
                        "metro_id": metro_id,
                        "price_val": None,
                        "price_ccy": None,  # Ensure this is 3 letters max
                        "fingerprint": fingerprint,
                        "quality_score": 0.5  # Slightly lower score for venues vs actual events
                    }
                    
                    extracted_events.append(event_dict)
        
        if not events_section_found:
            logger.warning(f"No events section found in record {event_raw_id}. This may indicate an API limitation or search result structure change.")
        
        return extracted_events
        
    except Exception as e:
        logger.error(f"Error extracting events from record {event_raw_id}: {e}")
        return []

def store_events_in_event_clean(db_conn, events):
    """Store extracted events in the event_clean table."""
    if not events:
        return 0
    
    try:
        with db_conn.cursor() as cur:
            # Get the column info from the table
            cur.execute("""
                SELECT column_name, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'event_clean' AND data_type = 'character'
            """)
            char_column_lengths = {row[0]: row[1] for row in cur.fetchall()}
            logger.info(f"Character column length constraints: {char_column_lengths}")
            
            # Prepare query with correct column names (snake_case)
            insert_query = """
                INSERT INTO event_clean (
                    event_raw_id, source, source_event_id, title, description, url,
                    start_ts, end_ts, venue_name, venue_address, venue_geom,
                    image_url, tags, metro_id, price_val, price_ccy, fingerprint, quality_score
                )
                VALUES %s
                ON CONFLICT (metro_id, fingerprint) DO NOTHING
                RETURNING id;
            """
            
            # Prepare values - truncate any character fields to their max lengths
            values = []
            for e in events:
                # Make a copy to avoid modifying the original
                event = dict(e)
                
                # Truncate price_ccy to 3 characters if present
                if event["price_ccy"] and len(event["price_ccy"]) > 3:
                    event["price_ccy"] = event["price_ccy"][:3]
                
                # Truncate fingerprint to 16 characters if needed
                if event["fingerprint"] and len(event["fingerprint"]) > 16:
                    event["fingerprint"] = event["fingerprint"][:16]
                
                values.append((
                    event["event_raw_id"], event["source"], event["source_event_id"], event["title"], 
                    event["description"], event["url"], event["start_ts"], event["end_ts"], 
                    event["venue_name"], event["venue_address"], event["venue_geom"], event["image_url"],
                    Json(event["tags"]), event["metro_id"], event["price_val"], event["price_ccy"],
                    event["fingerprint"], event["quality_score"]
                ))
            
            # Execute the query
            result = execute_values(cur, insert_query, values, fetch=True)
            
            # Update event_raw records as parsed
            event_raw_ids = list(set([e["event_raw_id"] for e in events]))
            now = datetime.now(timezone.utc)
            
            update_query = """
                UPDATE event_raw
                SET parsed_at = %s, normalization_status = 'processed'
                WHERE id = ANY(%s)
            """
            
            cur.execute(update_query, (now, event_raw_ids))
            db_conn.commit()
            
            logger.info(f"Successfully inserted {len(result)} new events into event_clean")
            return len(result)
            
    except Exception as e:
        logger.error(f"Error storing events in event_clean: {e}")
        if db_conn:
            db_conn.rollback()
        return 0

def check_event_clean_table(db_conn):
    """Check the contents of the event_clean table."""
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM event_clean")
            total_count = cur.fetchone()[0]
            logger.info(f"Total records in event_clean: {total_count}")
            
            if total_count > 0:
                logger.info("Sample event_clean records:")
                cur.execute("""
                    SELECT id, source, title, start_ts, venue_name, metro_id
                    FROM event_clean
                    ORDER BY id DESC
                    LIMIT 5
                """)
                
                # Print column names
                col_names = [desc[0] for desc in cur.description]
                logger.info(" | ".join(col_names))
                
                # Print results
                for row in cur.fetchall():
                    logger.info(" | ".join(str(v) for v in row))
            
            return True
            
    except Exception as e:
        logger.error(f"Error checking event_clean table: {e}")
        return False

def main():
    """Main function to parse event_raw and populate event_clean."""
    logger.info("Starting event parser...")
    
    # Connect to database
    db_conn = get_db_connection()
    if not db_conn:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Get unparsed event_raw records
        raw_records = get_unparsed_event_raws(db_conn)
        
        if not raw_records:
            logger.info("No unparsed event_raw records found.")
            return
        
        total_extracted = 0
        total_stored = 0
        
        # Process each record
        for raw_record in raw_records:
            event_raw_id = raw_record[0]
            logger.info(f"Processing event_raw record {event_raw_id}")
            
            # Extract events
            extracted_events = extract_events_from_json(raw_record)
            total_extracted += len(extracted_events)
            
            # Store events
            if extracted_events:
                stored_count = store_events_in_event_clean(db_conn, extracted_events)
                total_stored += stored_count
                logger.info(f"Stored {stored_count} events from record {event_raw_id}")
            else:
                # Mark as parsed even if no events were found
                with db_conn.cursor() as cur:
                    now = datetime.now(timezone.utc)
                    cur.execute(
                        "UPDATE event_raw SET parsed_at = %s, normalization_status = 'no_events' WHERE id = %s",
                        (now, event_raw_id)
                    )
                    db_conn.commit()
                logger.info(f"No events found in record {event_raw_id}")
        
        # Check event_clean table
        logger.info("\nChecking event_clean table contents...")
        check_event_clean_table(db_conn)
        
        logger.info(f"\nParser completed. Extracted {total_extracted} events, stored {total_stored} new events.")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main() 