# Lambda A10 GPU Instance Automation

This folder contains scripts to automate the process of creating, managing, and setting up Lambda Labs GPU instances for LLM processing.

## Prerequisites

1. Python 3.6+ with pip
2. SSH client
3. Lambda Labs account with API key

## Initial Setup

1. Create a `.env` file in the root directory with the following variables:
   ```
   LAMBDA_API_KEY=your_lambda_api_key
   GPU_INSTANCE_TYPE=gpu_1x_a10
   GPU_REGION=us-west-1
   SSH_KEY_NAME=TheSauce
   ```

2. Install required Python packages:
   ```
   pip install requests python-dotenv
   ```

## Scripts Overview

### 1. Lambda Automation Script

`lambda_automation.py` - A comprehensive tool for managing Lambda instances.

**Commands:**

- **Get IP of active instance:**
  ```
  python lambda_automation.py ip
  ```

- **Launch a new instance:**
  ```
  python lambda_automation.py launch
  ```

- **Terminate an instance:**
  ```
  python lambda_automation.py terminate <instance_id>
  ```

- **Print SSH command:**
  ```
  python lambda_automation.py ssh
  ```

### 2. Setup Script

`lambda_setup.sh` - Installs all necessary dependencies on a fresh Lambda instance:
- System packages
- Python environment
- PyTorch with CUDA
- LLM libraries
- SERP processing dependencies

### 3. Upload Setup Script

`upload_setup.py` - Helper to upload the setup script to a Lambda instance:

```
python upload_setup.py <ip_address>
```

## Typical Workflow

1. **Launch a new instance:**
   ```
   python lambda_automation.py launch
   ```

2. **Upload the setup script:**
   ```
   python upload_setup.py <ip_address>
   ```

3. **SSH into the instance:**
   ```
   ssh -i ~/.ssh/TheSauce ubuntu@<ip_address>
   ```

4. **Run the setup script:**
   ```
   ./setup.sh
   ```

5. **When finished, terminate the instance:**
   ```
   python lambda_automation.py terminate <instance_id>
   ```

## Customization

- Edit `lambda_setup.sh` to add or remove packages
- Modify GPU type in `.env` file if needed
- Update repository URLs or model downloads in the setup script

## Troubleshooting

- If you encounter connection errors, the Lambda API might be experiencing issues. Try again after a few minutes.
- If SSH authentication fails, check that your SSH key is properly set up with Lambda Labs.
- For GPU-related errors, check the instance type and CUDA compatibility. 