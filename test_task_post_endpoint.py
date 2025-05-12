import os
import requests
import json
import base64
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# DataForSEO credentials
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD")

# DataForSEO API endpoints - using task_post instead of live/advanced
task_post_endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_post"
task_get_endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced"

# Create auth header
auth_header = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()
headers = {
    'Authorization': f'Basic {auth_header}',
    'Content-Type': 'application/json'
}

# CORRECT request format for task_post endpoint
# Include location_name parameter as required by the API
data = [{
    "keyword": "salsa dance",
    "location_name": "New York,United States",  # Required format: city,country
    "language_name": "English",  # Use language_name instead of language_code
    "depth": 10,
    "se_domain": "google.com",
    "tag": "test_correct_task_post"  # Tag to help identify this task
}]

print(f"Step 1: Submitting task with payload: {json.dumps(data, indent=2)}")

try:
    # Submit the task
    response = requests.post(task_post_endpoint, json=data, headers=headers)
    response.raise_for_status()
    result = response.json()
    
    print(f"Task submission response: {result.get('status_code')} - {result.get('status_message')}")
    
    # Check if task was submitted successfully
    if result.get("status_code") == 20000 and result.get("tasks_count", 0) > 0:
        task_id = None
        
        # Get the task ID
        for task in result.get("tasks", []):
            if task.get("id"):
                task_id = task.get("id")
                print(f"Task ID: {task_id}")
                print(f"Task status: {task.get('status_code')} - {task.get('status_message')}")
                break
        
        if not task_id:
            print("No task ID found in response. Task submission may have failed.")
            exit(1)
            
        # Wait for the task to complete (polling)
        print("\nStep 2: Waiting for task to complete...")
        max_attempts = 10
        attempts = 0
        task_completed = False
        
        while attempts < max_attempts and not task_completed:
            attempts += 1
            print(f"Checking task status (attempt {attempts}/{max_attempts})...")
            time.sleep(5)  # Wait 5 seconds between checks
            
            try:
                # Check if task is ready
                task_check_url = f"{task_get_endpoint}/{task_id}"
                check_response = requests.get(task_check_url, headers=headers)
                check_response.raise_for_status()
                check_result = check_response.json()
                
                if check_result.get("status_code") == 20000:
                    if check_result.get("tasks") and len(check_result.get("tasks")) > 0:
                        task_status = check_result.get("tasks")[0].get("status_code")
                        if task_status == 20000:
                            print("Task completed successfully!")
                            task_completed = True
                            
                            # Print results
                            print("\nStep 3: Task Results:")
                            task_detail = check_result.get("tasks")[0]
                            result_count = task_detail.get("result_count", 0)
                            print(f"Found {result_count} result items")
                            
                            # Print the first few results
                            if task_detail.get("result") and len(task_detail.get("result")) > 0:
                                result_items = []
                                for result_block in task_detail.get("result", []):
                                    if result_block.get("items"):
                                        for item in result_block.get("items"):
                                            result_items.append(item)
                                
                                print(f"Total items found: {len(result_items)}")
                                print("First 3 results:")
                                for idx, item in enumerate(result_items[:3]):
                                    print(f"Item {idx+1}: {item.get('title')}")
                                    print(f"URL: {item.get('url')}")
                                    print()
                            else:
                                print("No result items found")
                            
                        elif task_status == 20100:
                            print("Task is still in queue...")
                        else:
                            print(f"Task error: {task_status} - {check_result.get('tasks')[0].get('status_message')}")
                            task_completed = True  # Stop checking on error
                    else:
                        print("No task details found in response")
                else:
                    print(f"Error checking task: {check_result.get('status_code')} - {check_result.get('status_message')}")
            
            except Exception as e:
                print(f"Error checking task status: {e}")
        
        if not task_completed:
            print("Task did not complete within the maximum number of attempts.")
    else:
        print(f"Task submission failed: {result.get('status_code')} - {result.get('status_message')}")
        print("Full response:")
        print(json.dumps(result, indent=2))
        
except Exception as e:
    print(f"Error: {e}")
    if 'response' in locals():
        print(f"Response text: {response.text}") 