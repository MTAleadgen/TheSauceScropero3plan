#!/usr/bin/env python3
"""
Execute database commands without password prompts
"""
import os
import sys
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_db_command(command):
    """Run a database command using environment variables to avoid password prompts"""
    # Get DB settings from environment
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("ERROR: DATABASE_URL environment variable not set.")
        return False
    
    # Parse connection string to get components
    # Format: postgres://user:password@host:port/dbname
    try:
        # Remove postgres:// or postgresql:// prefix
        if "://" in db_url:
            db_url = db_url.split("://")[1]
        
        auth, rest = db_url.split("@")
        user_pass = auth.split(":")
        user = user_pass[0]
        password = user_pass[1] if len(user_pass) > 1 else ""
        
        host_port_db = rest.split("/")
        host_port = host_port_db[0].split(":")
        host = host_port[0]
        port = host_port[1] if len(host_port) > 1 else "5432"
        dbname = host_port_db[1]
        
        # Create environment with PGPASSWORD
        env = os.environ.copy()
        env["PGPASSWORD"] = password
        
        # Build psql command
        psql_cmd = f"psql -h {host} -p {port} -U {user} -d {dbname} -c \"{command}\""
        
        # Run the command
        print(f"Running command: {command}")
        result = subprocess.run(psql_cmd, shell=True, env=env, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
        
        # Print output
        if result.returncode == 0:
            print(result.stdout)
            return True
        else:
            print(f"ERROR: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error parsing connection string or running command: {e}")
        return False

def main():
    """Main function to run database commands"""
    if len(sys.argv) < 2:
        print("Usage: python db_command.py \"SQL COMMAND\"")
        return
    
    # Get command from command line arguments
    command = " ".join(sys.argv[1:])
    run_db_command(command)

if __name__ == "__main__":
    main() 