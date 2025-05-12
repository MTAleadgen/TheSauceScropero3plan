#!/usr/bin/env python3
"""
Script to rename outdated and deprecated files in the codebase.
This appends .deprecated to filenames rather than deleting them.
"""

import os
import sys
import shutil
from pathlib import Path

# List of files to rename with their full relative paths
FILES_TO_RENAME = [
    # Test scripts
    "test_dataforseo.py",
    "test_dance_queries.py",
    "test_dance_queries_enhanced.py",
    "test_aggressive_query.py",
    "test_optimized_query.py",
    "test_more_results.py",
    "test_multiple_cities.py",
    "test_serp_variant.py",
    "test_simple_query.py",
    
    # Duplicate/Outdated Scripts
    "services/discovery/discovery.py",
    "services/discovery/discovery_test.py",
    "dataforseo_fixed.py",
    "fix_dataforseo_api.py",
    "fix_dataforseo_locations.py",
    
    # Debugging/Helper Scripts
    "debug_parser.py",
    "dump_events_structure.py",
    "find_city_codes.py",
    "check_events.py",
    
    # Test JSON files
    "test_task_post_response.json",
    "test_task_get_response.json",
    "test_live_response.json"
]

def rename_file(file_path):
    """Rename a file by appending .deprecated to its name"""
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        print(f"File not found: {file_path}")
        sys.stdout.flush()
        return False
    
    # Check if it's already renamed
    if str(path).endswith('.deprecated'):
        print(f"File already renamed: {file_path}")
        sys.stdout.flush()
        return False
    
    # New path with .deprecated appended
    new_path = Path(f"{path}.deprecated")
    
    # If the new path already exists, don't overwrite
    if new_path.exists():
        print(f"Destination already exists, skipping: {new_path}")
        sys.stdout.flush()
        return False
    
    try:
        # Rename the file
        path.rename(new_path)
        print(f"Renamed: {file_path} -> {new_path}")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"Error renaming {file_path}: {e}")
        sys.stdout.flush()
        return False

def main():
    """Main function to rename files"""
    print("Starting to rename deprecated files...")
    sys.stdout.flush()
    
    renamed_count = 0
    skipped_count = 0
    
    for file_path in FILES_TO_RENAME:
        print(f"Processing: {file_path}")
        sys.stdout.flush()
        if rename_file(file_path):
            renamed_count += 1
        else:
            skipped_count += 1
    
    print(f"\nRename operation complete.")
    print(f"Files renamed: {renamed_count}")
    print(f"Files skipped: {skipped_count}")
    sys.stdout.flush()
    
    if renamed_count > 0:
        print("\nThese files have been marked as deprecated but are still available for reference.")
        print("If you need to use any of them again, simply remove the .deprecated extension.")
        sys.stdout.flush()

if __name__ == "__main__":
    main() 