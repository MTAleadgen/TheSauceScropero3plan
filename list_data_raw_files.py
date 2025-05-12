import os
import glob
from pathlib import Path

def format_size(size_bytes):
    """Format file size in a human-readable way"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.2f} KB"
    else:
        return f"{size_bytes/(1024*1024):.2f} MB"

def list_all_files(directory):
    """List all files in a directory and subdirectories with their sizes"""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist!")
        return
        
    print(f"Files in {directory}:")
    print("-" * 80)
    print(f"{'File Path':<60} {'Size':<10} {'Modified':<20}")
    print("-" * 80)
    
    # Walk through all files in the directory and subdirectories
    for root, dirs, files in os.walk(directory):
        # Print directory
        rel_path = os.path.relpath(root, start=os.path.dirname(directory))
        print(f"\nDirectory: {rel_path}")
        
        # Print files in the directory
        if not files:
            print("  <No files in this directory>")
        for file in sorted(files):
            file_path = os.path.join(root, file)
            rel_file_path = os.path.relpath(file_path, start=os.path.dirname(directory))
            
            try:
                file_size = os.path.getsize(file_path)
                mod_time = os.path.getmtime(file_path)
                mod_time_str = os.path.getmtime(file_path)
                
                # Format file size
                size_str = format_size(file_size)
                
                # Format modification time
                from datetime import datetime
                mod_time_str = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"  {rel_file_path:<58} {size_str:<10} {mod_time_str:<20}")
            except Exception as e:
                print(f"  {rel_file_path:<58} <Error reading file info: {e}>")

if __name__ == "__main__":
    # Get the absolute path of the current directory
    current_dir = os.path.abspath(os.path.dirname(__file__))
    print(f"Current directory: {current_dir}")
    
    # List all files in data_raw
    data_raw_dir = os.path.join(current_dir, "data_raw")
    list_all_files(data_raw_dir)
    
    # Also check if there's a data_raw directory in another location
    alt_data_raw = os.path.abspath("./data_raw")
    if alt_data_raw != data_raw_dir:
        print("\nChecking alternative data_raw path:")
        list_all_files(alt_data_raw) 