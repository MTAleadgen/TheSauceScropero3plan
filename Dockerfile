# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables to prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies that might be needed for playwright or psycopg2
# Use apt-get for Debian-based images like python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    cmake \
    libpq-dev \
    # Add any system libs needed by psycopg2 or other packages if necessary
    # For Playwright dependencies:
    libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libxkbcommon0 libpango-1.0-0 libcairo2 \
    && rm -rf /var/lib/apt/lists/*

# Install pip requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
# This runs the playwright install command during the image build
RUN playwright install --with-deps chromium

# Copy the rest of the application code into the working directory
COPY . .

# Default command can be set here, but docker-compose will override it for each worker
# CMD ["python", "your_default_script.py"] 