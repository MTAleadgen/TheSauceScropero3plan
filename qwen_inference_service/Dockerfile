FROM python:3.10-slim

WORKDIR /app

# Install system dependencies that might be needed for CUDA, torch, etc.
# (Often PyTorch NGC containers or specific base images handle this, 
# but for python:3.10-slim, some common libs might be needed if not using a pre-built torch with CUDA)
# For now, assuming standard torch wheels with CUDA runtime will work.

COPY requirements.txt requirements.txt

# Consider using a virtual environment inside Docker if preferred
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure the port is exposed (though uvicorn command will also specify it)
EXPOSE 8008

# Command to run the FastAPI application
# The --host 0.0.0.0 makes it accessible from outside the container
# Add --workers 2 as per your suggestion for concurrency
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008", "--workers", "2"] 