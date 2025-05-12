#!/usr/bin/env python3
"""
verify_pipeline_queries.py

Verifies that the dance_events_pipeline.py is correctly configured
to run only 2 queries per city:
1. 'dance in [city]'
2. 'dancing in [city]'

This doesn't make actual API calls - just checks the code configuration.
"""

import os
import sys
import importlib.util

def check_query_types_in_file(file_path):
    """Check that the pipeline only uses the 2 specific query types we want."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Check if query_types is defined correctly
        if "query_types = ['dance', 'dancing']" in content:
            print("✓ PASS: Query types correctly limited to just 'dance' and 'dancing'")
        else:
            print("✗ FAIL: Query types not correctly defined as ['dance', 'dancing']")
            
            # Find actual definition
            import re
            query_types_match = re.search(r"query_types\s*=\s*\[(.*?)\]", content, re.DOTALL)
            if query_types_match:
                print(f"  Current definition: query_types = [{query_types_match.group(1)}]")
        
        # Check if make_dataforseo_events_query has correct keyword formatting
        if "keyword = f\"dance in {city" in content and "keyword = f\"dancing in {city" in content:
            print("✓ PASS: Keyword formatting is correct for both 'dance in' and 'dancing in'")
        else:
            print("✗ FAIL: Keyword formatting not correctly defined")
            
        # Check endpoint
        if "endpoint = \"https://api.dataforseo.com/v3/serp/google/events/live/advanced\"" in content:
            print("✓ PASS: Using correct events endpoint")
        else:
            print("✗ FAIL: Not using the events endpoint")
            
        # Check depth setting
        if "depth\": 1," in content:
            print("✓ PASS: Depth correctly set to 1")
        else:
            print("✗ FAIL: Depth not set to 1")
            depth_match = re.search(r"depth\": (\d+),", content)
            if depth_match:
                print(f"  Current depth: {depth_match.group(1)}")
        
        # Check date_range
        if "date_range\": \"next_week\"" in content:
            print("✓ PASS: Date range set to 'next_week'")
        else:
            print("✗ FAIL: Date range not set to 'next_week'")
            
        return True
    except Exception as e:
        print(f"Error checking file: {e}")
        return False

def main():
    pipeline_file = "dance_events_pipeline.py"
    
    if not os.path.exists(pipeline_file):
        print(f"Error: {pipeline_file} not found in the current directory")
        return False
        
    print(f"\nVerifying configuration of {pipeline_file}\n")
    
    # Check the query types configuration
    check_query_types_in_file(pipeline_file)
    
    print("\nVerification complete.")

if __name__ == "__main__":
    main() 