# SERP to LLM Pipeline Test

This document explains how to run a test of the complete SERP-to-LLM pipeline, which collects dance class information from search engine results and processes it using an LLM.

## Overview

The test pipeline consists of two main stages:
1. **SERP Collection**: Using DataForSEO to gather search results about dance classes in various cities
2. **LLM Processing**: Parsing the raw SERP data using the Qwen-3 LLM to extract structured event information

## Prerequisites

1. **DataForSEO Account**: You need a DataForSEO account with API credentials
2. **GPU Access**: The LLM processing requires a GPU (either local or on Lambda Labs)
3. **Python Dependencies**: Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Setting Up Environment Variables

Create a `.env` file with your DataForSEO credentials:

```
DATAFORSEO_LOGIN=your_login
DATAFORSEO_PASSWORD=your_password
```

## Running the Test

### Option 1: Complete Pipeline (SERP to LLM)

To run the complete pipeline with default settings (10 cities, Qwen-3 model):

```bash
python test_serp_to_llm.py
```

The script will:
1. Collect SERP data for 10 cities using DataForSEO
2. Store the raw data in `./data_raw/`
3. Process the data with the LLM
4. Store the parsed event data in `./data_parsed/`
5. Provide a summary of the results

### Option 2: Run Individual Steps

#### SERP Collection Only

```bash
python run_serp_test.py
```

This will collect SERP data for 10 cities and store it in `./data_raw/`.

#### LLM Processing Only

To run just the LLM processing on previously collected data:

```bash
python test_serp_to_llm.py --skip-serp
```

### Custom Options

The test script supports several options:

```
--skip-serp        Skip the SERP collection step
--skip-llm         Skip the LLM parsing step
--llm-model MODEL  Specify the LLM model (default: Qwen/Qwen3-7B)
--max-files N      Maximum number of files to process (default: 10)
```

Example with custom options:
```bash
python test_serp_to_llm.py --llm-model Qwen/Qwen3-0.5B --max-files 5
```

## Using Docker

You can also run the SERP collection using Docker:

```bash
docker compose up discovery
```

This will run the discovery service with DataForSEO to collect SERP data for 10 cities.

## Output Data Structure

### Raw SERP Data

The raw SERP data is stored in `./data_raw/` with filenames in the format:
```
{city_name}_{timestamp}.json
```

### Parsed Event Data

The parsed event data is stored in `./data_parsed/` with filenames in the format:
```
parsed_{city_name}_{timestamp}.json
```

Each file contains a JSON array of event objects with the following structure:

```json
[
  {
    "event_name": "Salsa Classes at Dance With Me",
    "dance_style": "salsa",
    "date_time": "Monday, May 11, 2025 at 7:00 PM",
    "location": "Dance With Me Studios, New York",
    "organizer": "Dance With Me Studios",
    "price": "$20 per class or $150 for a 10-class package",
    "experience_level": "beginner",
    "url": "https://dancewithme.com/salsa-classes-new-york/",
    "description": "Learn the basics of salsa dancing in a fun, supportive environment."
  },
  ...
]
```

## Next Steps

After confirming that the SERP-to-LLM pipeline works:

1. **Integrate into Airflow DAG**: Create an Airflow DAG that runs this pipeline regularly
2. **Scale to More Cities**: Increase the number of cities processed
3. **Implement Validation**: Add data validation to ensure the extracted events are accurate
4. **Set Up Monitoring**: Implement monitoring to track the pipeline's performance 