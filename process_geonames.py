import pandas as pd

# Define column names for cities15000.txt based on GeoNames readme
colnames = [
    'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
    'feature_class', 'feature_code', 'country_code', 'cc2', 'admin1_code',
    'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation',
    'dem', 'timezone', 'modification_date'
]

# File path for the input file
geonames_file = 'cities15000.txt' # Make sure this file is in the same directory or provide full path
output_csv_file = 'geonames_top1000.csv'

try:
    # Read the tab-separated file
    df = pd.read_csv(geonames_file, sep='\t', header=None, names=colnames, low_memory=False)

    # Ensure population is numeric before sorting
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df.dropna(subset=['population'], inplace=True) # Remove rows where population couldn't be converted

    # Sort by population in descending order and take the top 1000
    df_top1000 = df.sort_values(by='population', ascending=False).head(1000)

    # Select the desired columns for the master list
    selected_columns = [
        'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
        'country_code', 'population', 'timezone'
    ]
    output_df = df_top1000[selected_columns]

    # Save to CSV
    output_df.to_csv(output_csv_file, index=False, encoding='utf-8')

    print(f"Successfully processed {geonames_file}.")
    print(f"Top 1000 cities by population saved to {output_csv_file}")
    print(f"Number of rows in output: {len(output_df)}")
    if not output_df.empty:
        print("\nFirst 5 rows of the output CSV:")
        print(output_df.head().to_string())

except FileNotFoundError:
    print(f"Error: The file {geonames_file} was not found. Please ensure it is in the root directory.")
except Exception as e:
    print(f"An error occurred: {e}") 