# Dance Event Data Collection System

This system collects, processes, and stores dance event data from multiple sources, with a focus on DataForSEO search results. The pipeline extracts structured event information and makes it available for downstream applications.

## System Components

### 1. Data Collection

- **dataforseo_fixed.py**: Queries the DataForSEO API to search for dance events in specified cities
  - Uses location_code instead of location_name for reliable city searches
  - Properly configures API parameters for optimal results
  - Stores raw search results in the `event_raw` table

### 2. Data Processing

- **event_parser.py**: Extracts structured event data from raw search results
  - Parses different data formats (events, venues, organic search results)
  - Handles edge cases and missing data
  - Creates well-formatted event entries with consistent fields
  - Stores processed events in the `event_clean` table

### 3. Utilities

- **db_command.py**: Executes database commands without password prompts
  - Uses environment variables for secure database access
  - Simplifies database operations for other scripts

- **view_record.py**: Examines specific event_raw records for debugging
  - Shows the contents of raw search results
  - Helps diagnose API response issues

- **inspect_raw_data.py**: Shows the full JSON structure of raw records
  - Provides detailed view of DataForSEO response format
  - Makes API response hierarchy clear for developers

- **check_events.py**: Displays and filters events in the event_clean table
  - Supports filtering by metro_id and dance style
  - Shows data in table or CSV format
  - Provides statistics about collected events

- **debug_parser.py**: Tests the parser on specific records with detailed output
  - Shows each step of the extraction process
  - Helpful for diagnosing parser issues

- **location_mapper.py**: Maps between DataForSEO location codes and metro IDs
  - Helps find the correct DataForSEO location_code for a city
  - Finds metro IDs matching city names
  - Shows existing mappings between location codes and metros

## Database Schema

### event_raw
- Stores raw search results from DataForSEO
- Key fields: `id`, `source`, `source_event_id`, `metro_id`, `raw_json`, `parsed_at`

### event_clean
- Stores structured event data extracted from raw results
- Key fields: `id`, `event_raw_id`, `metro_id`, `source`, `title`, `description`, `start_ts`, `end_ts`, `venue_name`, `venue_address`, `tags`, `fingerprint`

## Usage Instructions

### Environment Setup

Create a `.env` file with your database connection string:
```
DATABASE_URL=postgres://user:password@host:port/dbname
```

### Running the Data Collection Pipeline

1. Collect data from DataForSEO:
```
python dataforseo_fixed.py --city "New York" --dance-style "salsa"
```

2. Process raw data into structured events:
```
python event_parser.py
```

3. Check extracted events:
```
python check_events.py --dance-style "salsa"
```

### Debugging

If events aren't being extracted properly:
1. View the raw record:
```
python view_record.py <record_id>
```

2. Inspect the JSON structure:
```
python inspect_raw_data.py <record_id>
```

3. Debug the parser process:
```
python debug_parser.py <record_id>
```

### Finding DataForSEO Location Codes

To map between city names, DataForSEO location codes, and metro IDs:

1. Look up DataForSEO location codes for a city:
```
python location_mapper.py --city "Chicago"
```

2. List all known location codes:
```
python location_mapper.py --list-codes
```

Use the location code and metro ID when collecting data:
```
python dataforseo_fixed.py --location-code 2840 --metro-id 5128581 --dance-style "salsa"
```

## Notes for Developers

1. **Column Naming Conventions**: The database uses snake_case for column names (e.g., `venue_name` not `venueName`).

2. **Column Constraints**: Be aware of column type constraints:
   - `fingerprint` is limited to 16 characters
   - `price_ccy` is limited to 3 characters
   - `tags` is a JSONB array

3. **Error Handling**: All scripts include robust error handling and logging to simplify debugging.

4. **Event Types**: The system currently extracts three types of entries:
   - Actual dance events (from the "events" section)
   - Dance resources (from organic search results)
   - Dance venues (from local_pack results)

5. **Quality Scores**: Events have different quality scores based on their source:
   - Actual events: 0.6
   - Venues: 0.5
   - Resources: 0.4 