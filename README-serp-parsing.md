# SERP Parsing with Qwen-3 LLM

This component extracts structured dance class event information from SERP (Search Engine Results Page) data using the Qwen-3 Large Language Model.

## Overview

The system uses the Qwen-3-7B LLM to process search engine results about dance classes and extract structured information about events. This information can be used to populate a database of dance classes and events.

## Components

1. **Sample SERP Data**: `sample_serp_data.json` - Example search results for testing
2. **Basic Parsing Script**: `llm_serp_parsing.py` - Simple script for testing without GPU
3. **GPU-Powered Parsing**: `llm_serp_parser_qwen.py` - Full implementation using Qwen-3 on Lambda GPU
4. **Interactive Demo**: `serp_parsing_demo.ipynb` - Jupyter notebook for interactive testing

## Setup

1. Launch a Lambda Labs GPU instance:
   ```
   python lambda_automation.py launch
   ```

2. Upload setup and parsing files:
   ```
   python upload_setup.py <ip_address>
   scp -i $env:USERPROFILE\.ssh\TheSauce llm_serp_parser_qwen.py sample_serp_data.json serp_parsing_demo.ipynb ubuntu@<ip_address>:~/
   ```

3. SSH into the instance:
   ```
   ssh -i $env:USERPROFILE\.ssh\TheSauce ubuntu@<ip_address>
   ```

4. Run the setup script:
   ```
   ./setup.sh
   ```

## Usage

### Command-Line Parsing

To parse SERP data from the command line:

```bash
# Activate the environment
source ~/activate_llm_env.sh

# Run the parser on sample data
python llm_serp_parser_qwen.py --input sample_serp_data.json --output extracted_events.json
```

### Interactive Notebook

For interactive exploration:

1. Start Jupyter:
   ```bash
   source ~/activate_llm_env.sh
   pip install jupyter
   jupyter notebook --ip=0.0.0.0 --no-browser
   ```

2. Access the notebook at the URL provided in the terminal output
3. Open `serp_parsing_demo.ipynb`

## Output Format

The parser extracts the following information for each dance class event:

```json
{
  "event_name": "Salsa Classes at Dance With Me",
  "dance_style": "salsa",
  "date_time": "Monday, May 11, 2025 at 7:00 PM",
  "location": "Dance With Me Studios, New York",
  "organizer": "Dance With Me Studios",
  "price": "$20 per class or $150 for a 10-class package",
  "experience_level": "beginner",
  "url": "https://dancewithme.com/salsa-classes-new-york/",
  "description": "Learn the basics of salsa dancing in a fun, supportive environment. No partner or experience needed!"
}
```

## Integration with Pipeline

This component is designed to be integrated into the Airflow pipeline as follows:

1. DataForSEO API fetches SERP data for dance classes in each city
2. The SERP data is saved temporarily
3. This parsing component extracts structured event data from the SERP results
4. The structured data is saved to the database

## Troubleshooting

- If you encounter GPU memory issues, try reducing the model size or batch size
- For parsing errors, check the raw LLM output in the `*_raw.txt` file
- If the model doesn't extract the correct information, try adjusting the prompt template 