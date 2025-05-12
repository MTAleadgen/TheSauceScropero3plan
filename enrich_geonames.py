import pandas as pd
import pytz
from datetime import datetime
from shapely.geometry import Point
import unicodedata
import re
import argparse

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
        print(f"Warning: Unknown timezone {tz_id}")
        return None
    except Exception as e:
        print(f"Warning: Could not get offset for {tz_id}: {e}")
        return None # Catch any other potential errors during offset calculation

# Function to determine metro tier
def get_metro_tier(population):
    if pd.isna(population):
        return None
    try:
        pop = int(population)
    except ValueError:
        return None # Population might not be a valid integer
        
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
        import math
        lon_buffer_denominator = (KM_PER_DEGREE_LON_EQUATOR * abs(math.cos(math.radians(lat))))
        if lon_buffer_denominator == 0: # Avoid division by zero at poles
            lon_buffer = BUFFER_KM / KM_PER_DEGREE_LON_EQUATOR # fallback or handle as error
        else:
            lon_buffer = BUFFER_KM / lon_buffer_denominator
        
        min_lon, min_lat = lon - lon_buffer, lat - lat_buffer
        max_lon, max_lat = lon + lon_buffer, lat + lat_buffer
        
        # Create a WKT polygon string for the bounding box
        return f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
    except ValueError: # Handle cases where lat/lon might not be convertible to float
        return None
    except Exception as e:
        print(f"Warning: Could not calculate bbox for lat={latitude}, lon={longitude}: {e}")
        return None

def main(input_file, output_file):
    geonames_fields = [
        'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 
        'longitude', 'feature_class', 'feature_code', 'country_code', 
        'cc2', 'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code', 
        'population', 'elevation', 'dem', 'timezone', 'modification_date'
    ]

    try:
        # Read tab-separated file, no header, assign column names
        df = pd.read_csv(input_file, delimiter='\t', header=None, names=geonames_fields, low_memory=False)

        # Convert relevant columns to appropriate types if necessary, after loading
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        print(f"Processing {len(df)} rows from {input_file}...")

        print("Computing tz_offset...")
        df['tz_offset_min'] = df['timezone'].apply(get_tz_offset_minutes)
        
        print("Computing metro_tier...")
        df['metro_tier'] = df['population'].apply(get_metro_tier)
        
        print("Computing slug...")
        df['slug'] = df.apply(lambda row: create_slug(row['asciiname'], row['country_code']), axis=1)
        
        print("Computing bbox (WKT)...")
        df['bbox_wkt'] = df.apply(lambda row: get_bbox_wkt(row['latitude'], row['longitude']), axis=1)

        # Save the enriched DataFrame to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')

        print(f"\nSuccessfully enriched data and saved to {output_file}")
        # print("First 5 rows of the enriched CSV:")
        # print(df.head().to_string()) # This might be too verbose for many rows

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except Exception as e:
        print(f"An error occurred during enrichment: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enrich Geonames data with additional fields.')
    parser.add_argument('input_file', type=str, help='Path to the input Geonames TXT file (tab-delimited, no header).')
    parser.add_argument('output_file', type=str, help='Path to save the enriched CSV file.')
    
    args = parser.parse_args()
    
    main(args.input_file, args.output_file) 