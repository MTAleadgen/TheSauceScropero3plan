import os
import pandas as pd
import psycopg2
from psycopg2 import sql
# from psycopg2.extras import execute_values # Not strictly needed for row-by-row insertion
from dotenv import load_dotenv
import numpy as np

def load_data():
    conn = None
    cur = None

    if not load_dotenv('.env.local'):
        load_dotenv('.env')
        
    database_url = os.getenv('DATABASE_URL')
    csv_file_path = 'geonames_na_sa_eu_top1785.csv'

    if not database_url:
        print("Error: DATABASE_URL not found in environment variables.")
        return

    df = None # Initialize df
    try:
        df = pd.read_csv(csv_file_path)
        df = df.replace({pd.NaT: None, np.nan: None})
        print(f"Loaded {len(df)} rows from {csv_file_path}")
    except FileNotFoundError:
        print(f"Error: CSV file {csv_file_path} not found.")
        return
    except Exception as e:
        print(f"Error reading or initially processing CSV: {e}")
        return

    if df is None: # Should not happen if returns are working, but as a safeguard
        print("Error: DataFrame not loaded.")
        return

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        print("Successfully connected to the database.")

        print(f"DEBUG: Type of 'country_code' in expected_csv_cols: {type('country_code')}")
        if 'country_code' in df.columns:
            idx = df.columns.tolist().index('country_code')
            print(f"DEBUG: Type of df.columns element 'country_code': {type(df.columns[idx])}")
            print(f"DEBUG: repr(df.columns[idx]): {repr(df.columns[idx])}")
        else:
            print("DEBUG: 'country_code' not even in df.columns before set conversion!")
        print(f"DEBUG: repr(df.columns.tolist()): {[repr(c) for c in df.columns.tolist()]}")
        print(f"DEBUG: 'country_code' in set(str(c) for c in df.columns.tolist()): {'country_code' in set(str(c) for c in df.columns.tolist())}")

        expected_csv_cols = {'geonameid', 'name', 'asciiname', 'country_code', 'population', 'timezone', 'latitude', 'longitude', 'slug', 'bbox_wkt'}
        missing_cols = expected_csv_cols - set(str(c) for c in df.columns)
        if missing_cols:
            for col in missing_cols:
                print(f"Warning: Column '{col}' not found in CSV (after str conversion check), will insert NULL if possible or fail if NOT NULL.")
        
        df_for_insert = df.copy()
        if 'country_code' in df_for_insert.columns:
            df_for_insert = df_for_insert.rename(columns={'country_code': 'country_iso2'})
        elif 'country_iso2' not in df_for_insert.columns:
            df_for_insert['country_iso2'] = None

        db_cols_payload = [
            'geonameid', 'name', 'asciiname', 'alternatenames', 'country_iso2',
            'population', 'timezone', 'tz_offset_min', 'metro_tier',
            'latitude', 'longitude', 'slug', 'bbox_wkt'
        ]

        final_df_to_insert = pd.DataFrame()
        for col_name in db_cols_payload:
            if col_name in df_for_insert.columns:
                final_df_to_insert[col_name] = df_for_insert[col_name]
            else:
                final_df_to_insert[col_name] = None
                if col_name in ['geonameid', 'name', 'latitude', 'longitude', 'slug', 'bbox_wkt']:
                     print(f"Warning: Critical DB column '{col_name}' was not found in source CSV and will be NULL.")
        
        print(f"Starting data insertion for {len(final_df_to_insert)} rows...")
        # Corrected SQL string quoting using triple single quotes
        insert_query_template = sql.SQL('''
        INSERT INTO metro ( 
            geonameid, name, asciiname, alternatenames, country_iso2,
            population, timezone, tz_offset_min, metro_tier, 
            latitude, longitude, slug, bbox_wkt, 
            geom, bbox
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                ST_SetSRID(ST_MakePoint(%s, %s), 4326), 
                ST_GeomFromText(%s, 4326))
        ON CONFLICT (geonameid) DO NOTHING;
        ''')

        success_count = 0
        skipped_due_to_conflict_count = 0
        fail_count = 0
        for index, row in final_df_to_insert.iterrows():
            try:
                values_tuple = (
                    row.get('geonameid'), row.get('name'), row.get('asciiname'), 
                    row.get('alternatenames'), row.get('country_iso2'),
                    row.get('population'), row.get('timezone'), 
                    row.get('tz_offset_min'), row.get('metro_tier'),
                    row.get('latitude'), row.get('longitude'), 
                    row.get('slug'), row.get('bbox_wkt'),
                    row.get('longitude'), row.get('latitude'),
                    row.get('bbox_wkt')
                )
                cur.execute(insert_query_template, values_tuple)
                if cur.rowcount > 0: # rowcount is 1 if INSERT happened, 0 if DO NOTHING
                    success_count += 1
                else:
                    skipped_due_to_conflict_count +=1
            except Exception as e_insert:
                fail_count += 1
                print(f"Failed to insert row {index} (geonameid: {row.get('geonameid')}): {e_insert}")
            
            if (index + 1) % 100 == 0:
                print(f"Processed {index + 1} rows...")
                conn.commit() 

        conn.commit()
        print(f"Data loading complete. {success_count} rows actually inserted. {skipped_due_to_conflict_count} rows skipped due to existing geonameid. {fail_count} rows failed to process.")

    except psycopg2.Error as e_db:
        print(f"Database error: {e_db}")
        if conn:
            conn.rollback()
    except Exception as e_general:
        print(f"An unexpected error occurred during DB operations: {e_general}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    load_data() 