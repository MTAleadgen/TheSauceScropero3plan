import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import numpy as np # For handling potential NaN values more gracefully

def load_data():
    load_dotenv('.env.local')
    database_url = os.getenv('DATABASE_URL')
    csv_file_path = 'geonames_top1000_enriched.csv'

    if not database_url:
        print("Error: DATABASE_URL not found in .env.local")
        return

    try:
        df = pd.read_csv(csv_file_path)
        # Replace pandas NaT/NaN with None for psycopg2 compatibility
        df = df.replace({pd.NaT: None, np.nan: None})
        print(f"Loaded {len(df)} rows from {csv_file_path}")
    except FileNotFoundError:
        print(f"Error: CSV file {csv_file_path} not found.")
        return
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    conn = None
    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        print("Successfully connected to the database.")

        # Prepare columns for insertion, ensuring they match the table definition
        # and the DataFrame columns. Ordering matters for execute_values.
        table_columns = [
            'geonameid', 'name', 'asciiname', 'alternatenames', 'country_iso2',
            'population', 'timezone', 'tz_offset_min', 'metro_tier',
            'latitude', 'longitude', 'slug', 'bbox_wkt',
            'geom', 'bbox' # These will be constructed with SQL functions
        ]
        
        # Filter DataFrame to only include columns that are in table_columns (except geom and bbox initially)
        # and handle potential missing columns gracefully.
        cols_for_df_select = [
            'geonameid', 'name', 'asciiname', 'alternatenames', 'country_code', # country_code from CSV
            'population', 'timezone', 'tz_offset_min', 'metro_tier',
            'latitude', 'longitude', 'slug', 'bbox_wkt'
        ]
        
        # Ensure all necessary columns exist in the DataFrame, renaming if needed (e.g. country_code to country_iso2)
        df_insert = df.rename(columns={'country_code': 'country_iso2'}) # CSV uses country_code
        
        # Select and reorder df columns to match cols_for_df_select for value preparation
        df_prepared_values = []
        for col in cols_for_df_select:
            if col not in df_insert.columns:
                print(f"Warning: Column '{col}' not found in CSV, will insert NULL if possible or fail if NOT NULL.")
                df_insert[col] = None # Add missing column with None
        df_prepared_values = df_insert[cols_for_df_select].values.tolist()

        # Construct the tuples for execute_values, including SQL for geom and bbox
        # This approach is complex with execute_values directly. 
        # It's often easier to iterate and insert one by one or use a more advanced ORM feature if available.
        # For now, let's try to build the values tuples correctly for execute_values, but it requires careful alignment.

        # The INSERT statement with PostGIS functions for geom and bbox
        # Note: using %s for column names is not standard for psycopg2 sql.SQL.format for values part.
        # We will use sql.SQL().format for the overall query structure and pass tuples as data.
        
        # Simplified approach: Iterate and insert with individual execute calls for clarity with PostGIS functions
        print(f"Starting data insertion for {len(df)} rows...")
        insert_query_template = sql.SQL("""
        INSERT INTO metro ( 
            geonameid, name, asciiname, alternatenames, country_iso2,
            population, timezone, tz_offset_min, metro_tier, 
            latitude, longitude, slug, bbox_wkt, 
            geom, bbox
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                ST_SetSRID(ST_MakePoint(%s, %s), 4326), 
                ST_GeomFromText(%s, 4326))
        ON CONFLICT (geonameid) DO NOTHING; -- Or DO UPDATE if you want to update existing records
        """)

        success_count = 0
        fail_count = 0
        for index, row in df_insert.iterrows():
            try:
                # Ensure correct order and types for values
                # geom and bbox values are derived from other columns
                values_tuple = (
                    row.get('geonameid'), row.get('name'), row.get('asciiname'), row.get('alternatenames'), row.get('country_iso2'),
                    row.get('population'), row.get('timezone'), row.get('tz_offset_min'), row.get('metro_tier'),
                    row.get('latitude'), row.get('longitude'), row.get('slug'), row.get('bbox_wkt'),
                    # Values for ST_MakePoint (longitude first, then latitude for standard geometry)
                    row.get('longitude'), row.get('latitude'),
                    # Value for ST_GeomFromText
                    row.get('bbox_wkt')
                )
                cur.execute(insert_query_template, values_tuple)
                success_count += 1
            except Exception as e_insert:
                fail_count += 1
                print(f"Failed to insert row {index} (geonameid: {row.get('geonameid')}): {e_insert}")
                # print(f"Problematic data: {values_tuple}") # For debugging
            
            if (index + 1) % 100 == 0:
                print(f"Processed {index + 1} rows...")
                conn.commit() # Commit periodically

        conn.commit() # Final commit
        cur.close()
        print(f"Data loading complete. {success_count} rows inserted/updated. {fail_count} rows failed.")

    except psycopg2.Error as e_db:
        print(f"Database error: {e_db}")
        if conn:
            conn.rollback() # Rollback on error
    except Exception as e_general:
        print(f"An unexpected error occurred: {e_general}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    load_data() 