import pandas as pd
import pytz
from datetime import datetime
from shapely.geometry import Point
import unicodedata
import re

# Function to create a slug
def create_slug(name, country_code):
    if pd.isna(name) or pd.isna(country_code):
        return None
    # Normalize to NFD (Normalization Form D) to decompose combined characters
    s = unicodedata.normalize('NFD', str(name).lower())
    # Remove diacritics (marks)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'[^a-z0-9\s-]', '', s)  # Remove special characters except space and hyphen
    s = re.sub(r'[\s_]+|-', '-', s).strip('-') # Replace whitespace/underscores with hyphens, clean multiple/leading/trailing hyphens
    return f"{s}-{str(country_code).lower()}"

# Function to calculate timezone offset in minutes
def get_tz_offset_minutes(tz_id):
    if pd.isna(tz_id):
        return None
    try:
        timezone = pytz.timezone(tz_id)
        # Get current offset. Note: This offset can change due to DST.
        # For a fixed offset, you might need a specific date or a library that handles historical DST.
        # For simplicity, we use the current UTC offset.
        offset_seconds = timezone.utcoffset(datetime.utcnow()).total_seconds()
        return int(offset_seconds / 60)
    except pytz.exceptions.UnknownTimeZoneError:
        return None
    except Exception:
        return None # Catch any other potential errors during offset calculation

# Function to determine metro tier
def get_metro_tier(population):
    if pd.isna(population):
        return None
    pop = int(population)
    if pop >= 10000000:
        return 1
    elif pop >= 3000000:
        return 2
    elif pop >= 1000000:
        return 3
    else:
        return 4

# Function to create a bounding box (25km buffer, returns WKT string)
# Approximate conversion: 1 degree of latitude is approx 111 km.
# For longitude, it varies. For simplicity, we use a fixed degree buffer.
# A more precise method would use a geospatial library to buffer in meters and then get the bounds.
KM_PER_DEGREE_LAT = 111.0
KM_PER_DEGREE_LON_EQUATOR = 111.32 # At the equator
BUFFER_KM = 25.0

def get_bbox_wkt(latitude, longitude):
    if pd.isna(latitude) or pd.isna(longitude):
        return None
    try:
        lat = float(latitude)
        lon = float(longitude)
        # Approximate buffer in degrees
        lat_buffer = BUFFER_KM / KM_PER_DEGREE_LAT
        # Approximation for lon_buffer, gets smaller away from equator
        lon_buffer = BUFFER_KM / (KM_PER_DEGREE_LON_EQUATOR * abs(pd.np.cos(pd.np.radians(lat))))
        
        min_lon, min_lat = lon - lon_buffer, lat - lat_buffer
        max_lon, max_lat = lon + lon_buffer, lat + lat_buffer
        
        # Create a WKT polygon string for the bounding box
        return f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
    except Exception:
        return None

# Load the CSV
input_csv = 'geonames_top1000.csv'
output_csv = 'geonames_top1000_enriched.csv'

try:
    df = pd.read_csv(input_csv)

    # Compute helper columns
    print("Computing tz_offset...")
    df['tz_offset_min'] = df['timezone'].apply(get_tz_offset_minutes)
    
    print("Computing metro_tier...")
    df['metro_tier'] = df['population'].apply(get_metro_tier)
    
    print("Computing slug...")
    df['slug'] = df.apply(lambda row: create_slug(row['asciiname'], row['country_code']), axis=1)
    
    print("Computing bbox (WKT)...")
    df['bbox_wkt'] = df.apply(lambda row: get_bbox_wkt(row['latitude'], row['longitude']), axis=1)

    # Save the enriched DataFrame
    df.to_csv(output_csv, index=False, encoding='utf-8')

    print(f"\nSuccessfully enriched data and saved to {output_csv}")
    print("First 5 rows of the enriched CSV:")
    print(df.head().to_string())

except FileNotFoundError:
    print(f"Error: The file {input_csv} was not found.")
except Exception as e:
    print(f"An error occurred during enrichment: {e}") 