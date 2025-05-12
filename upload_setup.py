#!/usr/bin/env python3
import os
import argparse
import subprocess
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Upload setup script to Lambda instance")
    parser.add_argument("ip", help="IP address of the Lambda instance")
    parser.add_argument("--setup_script", default="lambda_setup.sh", 
                       help="Path to the setup script (default: lambda_setup.sh)")
    parser.add_argument("--key", default=None, 
                       help="SSH key path (default: ~/.ssh/TheSauce)")
    args = parser.parse_args()
    
    # Determine the SSH key path
    if args.key:
        key_path = args.key
    else:
        # Default key location with platform-specific path
        if sys.platform == "win32":
            key_path = os.path.join(os.environ["USERPROFILE"], ".ssh", "TheSauce")
        else:
            key_path = os.path.expanduser("~/.ssh/TheSauce")
    
    # Check if the setup script exists
    setup_script = Path(args.setup_script)
    if not setup_script.exists():
        print(f"Error: Setup script not found at {setup_script}")
        sys.exit(1)
    
    # Upload the script using scp
    print(f"Uploading {setup_script} to ubuntu@{args.ip}:~/setup.sh")
    
    scp_command = [
        "scp",
        "-i", key_path,
        str(setup_script),
        f"ubuntu@{args.ip}:~/setup.sh"
    ]
    
    try:
        result = subprocess.run(scp_command, check=True)
        print("Upload successful!")
        
        # Make the script executable
        ssh_command = [
            "ssh",
            "-i", key_path,
            f"ubuntu@{args.ip}",
            "chmod +x ~/setup.sh"
        ]
        
        subprocess.run(ssh_command, check=True)
        print("Made script executable.")
        
        # Print next steps
        print("\nNext steps:")
        print(f"1. SSH into the instance: ssh -i {key_path} ubuntu@{args.ip}")
        print("2. Run the setup script: ./setup.sh")
        
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 