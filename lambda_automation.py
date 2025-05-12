import os
import requests
import time
import logging
import argparse
import sys
import random
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

LAMBDA_API_KEY = os.getenv("LAMBDA_API_KEY")
GPU_INSTANCE_TYPE = os.getenv("GPU_INSTANCE_TYPE", "gpu_1x_a10")
GPU_REGION = os.getenv("GPU_REGION", "us-west-1")
SSH_KEY_NAME = os.getenv("SSH_KEY_NAME", "TheSauce")
INSTANCE_NAME_PREFIX = os.getenv("INSTANCE_NAME_PREFIX", "llm-server")

LAMBDA_API_BASE_URL = "https://cloud.lambdalabs.com/api/v1"
LAUNCH_ENDPOINT = f"{LAMBDA_API_BASE_URL}/instance-operations/launch"
INSTANCES_ENDPOINT = f"{LAMBDA_API_BASE_URL}/instances"
TERMINATE_ENDPOINT = f"{LAMBDA_API_BASE_URL}/instance-operations/terminate"
SSH_KEYS_ENDPOINT = f"{LAMBDA_API_BASE_URL}/ssh-keys"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def retry_api_call(func, *args, max_retries=5, initial_backoff=1, **kwargs):
    """
    Retry an API call with exponential backoff
    
    Args:
        func: The function to retry
        *args: Arguments to pass to the function
        max_retries: Maximum number of retries
        initial_backoff: Initial backoff time in seconds
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
    """
    retries = 0
    backoff = initial_backoff
    
    while retries <= max_retries:
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            retries += 1
            if retries > max_retries:
                logger.error(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                raise
            
            # Add some randomness to backoff (jitter)
            sleep_time = backoff + random.uniform(0, 1)
            logger.warning(f"API call failed: {str(e)}. Retrying in {sleep_time:.2f}s (attempt {retries}/{max_retries})")
            time.sleep(sleep_time)
            
            # Exponential backoff
            backoff = min(backoff * 2, 60)  # Cap at 60 seconds

def api_get(url, headers, timeout=20):
    """
    Make a GET request to the API with retry logic
    
    Args:
        url: The URL to request
        headers: Headers to include
        timeout: Request timeout in seconds
        
    Returns:
        Response object
    """
    return retry_api_call(requests.get, url, headers=headers, timeout=timeout)

def api_post(url, headers, json, timeout=20):
    """
    Make a POST request to the API with retry logic
    
    Args:
        url: The URL to request
        headers: Headers to include
        json: JSON payload
        timeout: Request timeout in seconds
        
    Returns:
        Response object
    """
    return retry_api_call(requests.post, url, headers=headers, json=json, timeout=timeout)

def launch_instance():
    """Launch a Lambda Labs instance for LLM work"""
    if not LAMBDA_API_KEY:
        logger.error("LAMBDA_API_KEY not found in environment variables.")
        return None
    
    if not SSH_KEY_NAME:
        logger.error("SSH_KEY_NAME not found in environment variables.")
        return None
    
    headers = {
        "Authorization": f"Bearer {LAMBDA_API_KEY}",
        "Content-Type": "application/json"
    }
    
    instance_name = f"{INSTANCE_NAME_PREFIX}-{GPU_INSTANCE_TYPE}-{int(time.time())}"
    
    payload = {
        "region_name": GPU_REGION,
        "instance_type_name": GPU_INSTANCE_TYPE,
        "ssh_key_names": [SSH_KEY_NAME],
        "name": instance_name
    }
    
    logger.info(f"Launching instance: Type='{GPU_INSTANCE_TYPE}', Region='{GPU_REGION}', SSHKey='{SSH_KEY_NAME}', Name='{instance_name}'")
    
    try:
        response = api_post(
            LAUNCH_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=20
        )
        
        if response.status_code == 200:
            data = response.json()
            instance_ids = data.get("data", {}).get("instance_ids", [])
            if instance_ids:
                instance_id = instance_ids[0]
                logger.info(f"Successfully launched instance: {instance_id}")
                return instance_id
            else:
                logger.error("No instance ID returned in the successful response.")
                return None
        else:
            logger.error(f"Failed to launch instance. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error launching instance: {str(e)}")
        return None

def get_instance_status(instance_id=None):
    """Get status of a Lambda Labs instance or the most recent one"""
    if not LAMBDA_API_KEY:
        logger.error("LAMBDA_API_KEY not found in environment variables.")
        return None
    
    headers = {
        "Authorization": f"Bearer {LAMBDA_API_KEY}"
    }
    
    try:
        response = api_get(
            INSTANCES_ENDPOINT,
            headers=headers,
            timeout=20
        )
        
        if response.status_code == 200:
            data = response.json()
            instances = data.get("data", {})
            
            # Check if instances is a dictionary
            if isinstance(instances, dict):
                # Format is {instance_id: instance_data, ...}
                if not instances:
                    logger.warning("No instances found in account.")
                    return None
                
                # If instance_id is provided, get that specific instance
                if instance_id and instance_id in instances:
                    instance = instances[instance_id]
                    instance["id"] = instance_id
                    return instance
                
                # Otherwise get the most recent active instance
                active_instances = []
                for id, instance in instances.items():
                    # Add ID to the instance object
                    instance["id"] = id
                    active_instances.append(instance)
                
                if not active_instances:
                    logger.warning("No instances found.")
                    return None
                
                # Sort by creation time if available
                if "created_at" in active_instances[0]:
                    active_instances.sort(key=lambda x: x.get("created_at", 0), reverse=True)
                    
                # If we were looking for a specific ID but didn't find it
                if instance_id:
                    logger.warning(f"Instance {instance_id} not found.")
                    return None
                
                # Return the most recent instance
                return active_instances[0]
            else:
                # Handle the case where instances might be a list
                if not instances:
                    logger.warning("No instances found in account.")
                    return None
                
                # If we have a specific ID, find that instance
                if instance_id:
                    for instance in instances:
                        if instance.get("id") == instance_id:
                            return instance
                    logger.warning(f"Instance {instance_id} not found.")
                    return None
                
                # Otherwise get the most recent
                if "created_at" in instances[0]:
                    instances.sort(key=lambda x: x.get("created_at", 0), reverse=True)
                return instances[0]
        else:
            logger.error(f"Failed to get instance status. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting instance status: {str(e)}")
        return None

def wait_for_instance_ip(instance_id, max_polls=20, poll_interval=15):
    """Wait for an instance to get an IP address"""
    logger.info(f"Waiting for instance {instance_id} to get an IP address (max {max_polls} polls)...")
    
    for i in range(max_polls):
        logger.info(f"Poll {i+1}/{max_polls}: Checking instance status...")
        
        instance = get_instance_status(instance_id)
        
        if not instance:
            logger.warning("Could not retrieve instance.")
        else:
            status = instance.get("status")
            ip = instance.get("ip")
            
            logger.info(f"Instance status: {status}, IP: {ip}")
            
            if status == "active" and ip:
                logger.info(f"Instance is ready! IP address: {ip}")
                return ip
        
        if i < max_polls - 1:
            logger.info(f"Waiting {poll_interval} seconds before next check...")
            time.sleep(poll_interval)
    
    logger.warning("Max polls reached. Instance may still be initializing.")
    return None

def terminate_instance(instance_id):
    """Terminate a Lambda Labs instance"""
    if not LAMBDA_API_KEY:
        logger.error("LAMBDA_API_KEY not found in environment variables.")
        return False
    
    headers = {
        "Authorization": f"Bearer {LAMBDA_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "instance_ids": [instance_id]
    }
    
    logger.info(f"Attempting to terminate instance: {instance_id}")
    
    try:
        response = api_post(
            TERMINATE_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=20
        )
        
        if response.status_code == 200:
            logger.info(f"Successfully initiated termination of instance: {instance_id}")
            return True
        else:
            logger.error(f"Failed to terminate instance. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error terminating instance: {str(e)}")
        return False

def find_active_instance_ip():
    """Find the IP of the most recently active instance"""
    instance = get_instance_status()
    
    if instance and instance.get("status") == "active" and instance.get("ip"):
        ip = instance.get("ip")
        instance_id = instance.get("id")
        logger.info(f"Found active instance: ID={instance_id}, IP={ip}")
        return ip
    
    return None

def launch_and_wait():
    """Launch an instance and wait for it to get an IP address"""
    instance_id = launch_instance()
    
    if not instance_id:
        logger.error("Failed to launch instance.")
        return None
    
    logger.info(f"Instance ID: {instance_id}")
    
    ip = wait_for_instance_ip(instance_id)
    
    if ip:
        logger.info("Instance is ready for use!")
        logger.info(f"IP Address: {ip}")
        logger.info(f"SSH Command: ssh -i $env:USERPROFILE\\.ssh\\{SSH_KEY_NAME} ubuntu@{ip}")
        return ip
    else:
        logger.warning("Could not determine when instance is ready. Check Lambda dashboard.")
        return None

def print_ssh_command(ip):
    """Print the SSH command for the given IP address"""
    if ip:
        print(f"\nInstance IP: {ip}")
        print(f"SSH Command: ssh -i $env:USERPROFILE\\.ssh\\{SSH_KEY_NAME} ubuntu@{ip}")
        # Also output just the IP for easy script usage
        print(f"\n{ip}")
        return True
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lambda Labs GPU Instance Automation")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Launch command
    launch_parser = subparsers.add_parser("launch", help="Launch a new instance and wait for it to boot")
    launch_parser.add_argument("--max-polls", "-m", type=int, default=20, help="Maximum number of polling attempts")
    launch_parser.add_argument("--interval", "-i", type=int, default=15, help="Polling interval in seconds")
    
    # Terminate command
    terminate_parser = subparsers.add_parser("terminate", help="Terminate an instance")
    terminate_parser.add_argument("instance_id", help="ID of the instance to terminate")
    
    # IP command
    ip_parser = subparsers.add_parser("ip", help="Get the IP address of the most recent active instance")
    
    # SSH command
    ssh_parser = subparsers.add_parser("ssh", help="Print SSH command for the most recent active instance")
    
    # Debug flag
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "launch":
        ip = launch_and_wait()
        if not print_ssh_command(ip):
            sys.exit(1)
    
    elif args.command == "terminate":
        success = terminate_instance(args.instance_id)
        if not success:
            sys.exit(1)
    
    elif args.command == "ip":
        ip = find_active_instance_ip()
        if not print_ssh_command(ip):
            sys.exit(1)
    
    elif args.command == "ssh":
        ip = find_active_instance_ip()
        if ip:
            print(f"ssh -i $env:USERPROFILE\\.ssh\\{SSH_KEY_NAME} ubuntu@{ip}")
        else:
            sys.exit(1) 