FROM python:3.10-slim

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY discovery.py discovery.py
# The CSV file will be mounted as a volume via docker-compose.yml
# COPY ../geonames_top1000_enriched.csv geonames_top1000_enriched.csv 

# Command to run the discovery script
# This container is expected to run as a one-off task (e.g., via docker run or triggered by Airflow)
CMD ["python", "discovery.py"] 