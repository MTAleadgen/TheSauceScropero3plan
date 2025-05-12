import requests
import os
import json
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("LAMBDA_API_KEY")
API_URL = "https://cloud.lambdalabs.com/api/v1/instance-operations/terminate"

def terminate_instances(instance_ids: list[str]):
    """Terminates one or more Lambda Labs instances."""
    if not API_KEY:
        print("Error: LAMBDA_API_KEY not found in environment variables or .env file.")
        return
    
    if not instance_ids:
        print("Error: No instance IDs provided.")
        return

    payload = {"instance_ids": instance_ids}
    headers = {"Content-Type": "application/json"}

    print(f"Sending termination request to Lambda Labs for instance(s):")
    for instance_id in instance_ids:
        print(f"  {instance_id}")

    try:
        response = requests.post(
            API_URL,
            headers=headers,
            auth=(API_KEY, ''),
            json=payload
        )
        response.raise_for_status() 

        response_data = response.json()
        terminated_ids = [instance['id'] for instance in response_data.get('data', {}).get('terminated_instances', [])]

        if terminated_ids:
            print("\nSuccessfully terminated instance(s)!")
            print("Terminated ID(s):")
            for term_id in terminated_ids:
                print(f"  {term_id}")
        else:
            # Check if the IDs provided were invalid or already terminated
            print("\nTermination request sent, but API did not confirm termination for the provided IDs.")
            print(f"This might happen if IDs were incorrect or instances were already terminated.")
            print(f"Raw response: {response_data}")

    except requests.exceptions.RequestException as e:
        print(f"\nError terminating instance(s): {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"API Error Details: {error_data}")
            except json.JSONDecodeError:
                print(f"Could not parse error response: {e.response.text}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Terminate Lambda Labs GPU instance(s).")
    parser.add_argument("instance_ids", nargs='+', help="One or more instance IDs to terminate")

    args = parser.parse_args()

    terminate_instances(args.instance_ids) 