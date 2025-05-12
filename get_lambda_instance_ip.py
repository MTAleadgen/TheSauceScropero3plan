import os
import requests
import time
import logging
import json
import sys
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

LAMBDA_API_KEY = os.getenv("LAMBDA_API_KEY")
LAMBDA_API_BASE_URL = "https://cloud.lambdalabs.com/api/v1"
INSTANCES_ENDPOINT = f"{LAMBDA_API_BASE_URL}/instances"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_latest_instance_ip():
    """Get the most recently launched Lambda instance IP"""
    if not LAMBDA_API_KEY:
        logger.error("LAMBDA_API_KEY not found in environment variables.")
        return None
    
    headers = {
        "Authorization": f"Bearer {LAMBDA_API_KEY}"
    }
    
    try:
        response = requests.get(
            INSTANCES_ENDPOINT,
            headers=headers,
            timeout=20
        )
        
        if response.status_code == 200:
            data = response.json()
            instances = data.get("data", [])
            
            # Check if instances is a list or a dict
            if isinstance(instances, dict):
                # Format is {instance_id: instance_data, ...}
                if not instances:
                    logger.warning("No instances found in account.")
                    return None
                
                # Find most recent active instance
                active_instances = []
                for instance_id, instance in instances.items():
                    if instance.get("status") == "active" and instance.get("ip"):
                        # Add creation timestamp for sorting
                        instance["id"] = instance_id
                        active_instances.append(instance)
                
                if not active_instances:
                    logger.warning("No active instances with IP addresses found.")
                    return None
                
                # Sort by most recent (if created_at exists)
                if "created_at" in active_instances[0]:
                    active_instances.sort(key=lambda x: x.get("created_at", 0), reverse=True)
                
                # Take the first (most recent) instance
                latest_instance = active_instances[0]
                ip = latest_instance.get("ip")
                instance_id = latest_instance.get("id")
                
                logger.info(f"Found active instance: ID={instance_id}, IP={ip}")
                
                # Print SSH command for easy copying
                ssh_command = f"ssh -i $env:USERPROFILE\\.ssh\\TheSauce ubuntu@{ip}"
                logger.info(f"SSH command: {ssh_command}")
                
                return ip
            else:
                # Legacy or different format - try to handle arrays
                if not instances:
                    logger.warning("No instances found in account.")
                    return None
                
                active_instances = [inst for inst in instances if inst.get("status") == "active" and inst.get("ip")]
                
                if not active_instances:
                    logger.warning("No active instances with IP addresses found.")
                    return None
                
                # Sort by most recent (if created_at exists)
                if "created_at" in active_instances[0]:
                    active_instances.sort(key=lambda x: x.get("created_at", 0), reverse=True)
                
                latest_instance = active_instances[0]
                ip = latest_instance.get("ip")
                instance_id = latest_instance.get("id")
                
                logger.info(f"Found active instance: ID={instance_id}, IP={ip}")
                
                # Print SSH command for easy copying
                ssh_command = f"ssh -i $env:USERPROFILE\\.ssh\\TheSauce ubuntu@{ip}"
                logger.info(f"SSH command: {ssh_command}")
                
                return ip
        else:
            logger.error(f"Failed to get instances. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting instances: {str(e)}")
        return None

def poll_for_ip(max_polls=20, poll_interval=15):
    """Poll for an IP address until it's available or max polls is reached"""
    logger.info(f"Polling for instance IP address (max {max_polls} attempts)...")
    
    for i in range(max_polls):
        logger.info(f"Poll {i+1}/{max_polls}: Checking for IP address...")
        
        ip = get_latest_instance_ip()
        
        if ip:
            logger.info(f"Success! Found IP address: {ip}")
            return ip
        
        if i < max_polls - 1:
            logger.info(f"Waiting {poll_interval} seconds before next check...")
            time.sleep(poll_interval)
    
    logger.warning("Max polls reached. Could not find an instance with an IP address.")
    return None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Get Lambda instance IP address")
    parser.add_argument("--poll", "-p", action="store_true", help="Poll until an IP is found")
    parser.add_argument("--interval", "-i", type=int, default=15, help="Polling interval in seconds")
    parser.add_argument("--max-polls", "-m", type=int, default=20, help="Maximum number of polling attempts")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.poll:
        ip = poll_for_ip(max_polls=args.max_polls, poll_interval=args.interval)
    else:
        ip = get_latest_instance_ip()
    
    if ip:
        print(f"\nInstance IP: {ip}")
        print(f"SSH Command: ssh -i $env:USERPROFILE\\.ssh\\TheSauce ubuntu@{ip}")
        # Output just the IP for easy script usage
        print(f"\n{ip}")
    else:
        print("\nFailed to find an instance IP address.")
        sys.exit(1) 