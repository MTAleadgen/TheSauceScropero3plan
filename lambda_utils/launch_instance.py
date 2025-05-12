import requests
import os
import json
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("LAMBDA_API_KEY")
API_URL = "https://cloud.lambdalabs.com/api/v1/instance-operations/launch"

def launch_instance(region_name: str, instance_type_name: str, ssh_key_names: list[str], name: str | None = None):
    """Launches a Lambda Labs instance."""
    if not API_KEY:
        print("Error: LAMBDA_API_KEY not found in environment variables or .env file.")
        return

    payload = {
        "region_name": region_name,
        "instance_type_name": instance_type_name,
        "ssh_key_names": ssh_key_names,
        # "file_system_names": [], # Optional: Add if needed
        # "quantity": 1, # Default is 1
    }
    if name:
        payload["name"] = name

    headers = {"Content-Type": "application/json"}

    print(f"Sending launch request to Lambda Labs:")
    print(f"  Region: {region_name}")
    print(f"  Type: {instance_type_name}")
    print(f"  SSH Keys: {ssh_key_names}")
    if name:
        print(f"  Name: {name}")
    
    try:
        response = requests.post(
            API_URL,
            headers=headers,
            auth=(API_KEY, ''), # HTTP Basic Auth: API key as username, empty password
            json=payload
        )
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        response_data = response.json()
        instance_ids = [instance['id'] for instance in response_data.get('data', {}).get('launched_instances', [])]
        
        if instance_ids:
            print("\nSuccessfully launched instance(s)!")
            print("Instance ID(s):")
            for instance_id in instance_ids:
                print(f"  {instance_id}")
            print("\nNote: It may take a few minutes for the instance(s) to become fully active.")
        else:
            print("\nLaunch request sent, but no instance IDs returned in the response.")
            print(f"Raw response: {response_data}")

    except requests.exceptions.RequestException as e:
        print(f"\nError launching instance: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"API Error Details: {error_data}")
            except json.JSONDecodeError:
                print(f"Could not parse error response: {e.response.text}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch a Lambda Labs GPU instance.")
    parser.add_argument("-t", "--type", required=True, help="Instance type name (e.g., gpu_1x_a10)")
    parser.add_argument("-r", "--region", required=True, help="Region name (e.g., us-east-1)")
    parser.add_argument("-k", "--ssh-key", required=True, help="Name of the SSH key registered in Lambda Labs")
    parser.add_argument("-n", "--name", help="Optional name for the instance")

    args = parser.parse_args()

    # Currently takes one key, but API expects a list
    ssh_keys = [args.ssh_key]

    launch_instance(args.region, args.type, ssh_keys, args.name) 