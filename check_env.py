#!/usr/bin/env python3
"""
check_env.py

Simple script to check if the DataForSEO API credentials
are being properly loaded from the .env file.
"""

import os
import sys
from dotenv import load_dotenv

# Print current working directory
print(f"Current working directory: {os.getcwd()}")

# Check if .env file exists
env_file = os.path.join(os.getcwd(), '.env')
if os.path.exists(env_file):
    print(f".env file found at: {env_file}")
    
    # Display file contents (with sensitive info masked)
    try:
        with open(env_file, 'r') as f:
            lines = f.readlines()
            print("\n.env file contents:")
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Mask actual values for security
                    if '=' in line:
                        key, value = line.split('=', 1)
                        if key.strip() in ['DATAFORSEO_API_LOGIN', 'DATAFORSEO_API_PASSWORD']:
                            masked_value = value[:3] + '***' if len(value) > 3 else '***'
                            print(f"  {key}={masked_value}")
                        else:
                            print(f"  {key}=***")
                    else:
                        print(f"  {line} (no value)")
    except Exception as e:
        print(f"Error reading .env file: {e}")
else:
    print(f"ERROR: .env file not found at: {env_file}")

# Try to load the environment variables
print("\nAttempting to load environment variables...")
load_dotenv()

# Check if credentials are loaded
DATAFORSEO_API_LOGIN = os.environ.get('DATAFORSEO_API_LOGIN')
DATAFORSEO_API_PASSWORD = os.environ.get('DATAFORSEO_API_PASSWORD')

if DATAFORSEO_API_LOGIN:
    print("✓ DATAFORSEO_API_LOGIN found in environment variables")
else:
    print("✗ DATAFORSEO_API_LOGIN not found in environment variables")

if DATAFORSEO_API_PASSWORD:
    print("✓ DATAFORSEO_API_PASSWORD found in environment variables")
else:
    print("✗ DATAFORSEO_API_PASSWORD not found in environment variables")

print("\nDebugging tips if credentials are not found:")
print("1. Make sure your .env file is in the correct directory")
print("2. Check that variable names are exactly DATAFORSEO_API_LOGIN and DATAFORSEO_API_PASSWORD")
print("3. Verify there are no spaces around the equal sign (KEY=value, not KEY = value)")
print("4. Ensure values are not enclosed in quotes unless needed for special characters")
print("5. Try creating the .env file with the correct format:")
print("\nDATA_FOR_SEO_API_LOGIN=your_login_here")
print("DATA_FOR_SEO_API_PASSWORD=your_password_here")

print("\nTo create/update your .env file, run:")
print('echo "DATAFORSEO_API_LOGIN=your_login" > .env')
print('echo "DATAFORSEO_API_PASSWORD=your_password" >> .env') 