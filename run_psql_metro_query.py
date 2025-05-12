import os
import subprocess
from dotenv import load_dotenv

load_dotenv('.env')

def run_psql_command(sql_command):
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("Error: DATABASE_URL not found in environment.")
        return False

    command = [
        'psql',
        database_url,
        '-c',
        sql_command
    ]

    try:
        print(f"Executing: PGPASSWORD=******** psql -U <user> -h <host> -p <port> -d <db> -c \"{sql_command}\"") 
        result = subprocess.run(command, capture_output=True, text=True, check=False, shell=False)
        
        if result.returncode != 0:
            print(f"psql command failed with exit code {result.returncode}")
            print("Stdout:")
            print(result.stdout)
            print("Stderr:")
            print(result.stderr)
            return False

        print("Command Output:")
        # For \d commands, we want the full, aligned output
        print(result.stdout) 
        return True

    except FileNotFoundError:
        print("Error: 'psql' command not found. Make sure PostgreSQL client tools are installed and in your PATH.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False

if __name__ == '__main__':
    # Set a 5-minute timeout for the truncate operation, then run truncate
    # Note: some psql versions/setups might require two -c flags or careful quoting for multiple commands.
    # A single string is generally fine for psql -c.
    long_timeout_truncate_command = "SET statement_timeout = '300s'; TRUNCATE metro CASCADE;"
    
    print(f"Attempting to truncate metro table with increased timeout...")
    success = run_psql_command(long_timeout_truncate_command)
    if not success:
        print("Failed to truncate metro table even with increased timeout.")
    else:
        print("Successfully sent TRUNCATE command for metro table with increased timeout.") 