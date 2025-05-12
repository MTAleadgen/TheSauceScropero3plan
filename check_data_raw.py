import os
import json
import glob
from collections import defaultdict

def summarize_data_raw():
    """Summarize the contents of the data_raw directory"""
    print("Checking data_raw directory contents...")
    
    # Check if data_raw exists
    if not os.path.exists("data_raw"):
        print("data_raw directory not found!")
        return
    
    # Count files in main data_raw directory
    main_files = [f for f in os.listdir("data_raw") if os.path.isfile(os.path.join("data_raw", f))]
    print(f"Main data_raw directory contains {len(main_files)} files")
    
    # Check dance_queries_enhanced directory
    enhanced_dir = os.path.join("data_raw", "dance_queries_enhanced")
    if os.path.exists(enhanced_dir):
        enhanced_files = [f for f in os.listdir(enhanced_dir) if os.path.isfile(os.path.join(enhanced_dir, f))]
        print(f"dance_queries_enhanced directory contains {len(enhanced_files)} files")
        
        # Look for results.json files
        results_files = [f for f in enhanced_files if f.endswith("_results.json")]
        print(f"Found {len(results_files)} results.json files")
        
        # Parse one of the results files to show summary
        if results_files:
            results_path = os.path.join(enhanced_dir, results_files[0])
            try:
                with open(results_path, 'r') as f:
                    data = json.load(f)
                    city = data.get('city', 'Unknown')
                    total_urls = data.get('total_unique_urls', 0)
                    styles = data.get('url_counts_by_style', {})
                    
                    print(f"\nSample data for {city}:")
                    print(f"Total unique URLs: {total_urls}")
                    print("URL counts by dance style:")
                    for style, count in styles.items():
                        print(f"  - {style}: {count}")
            except Exception as e:
                print(f"Error reading results file: {e}")
    else:
        print("dance_queries_enhanced directory not found!")
    
    # Check dance_queries directory
    queries_dir = os.path.join("data_raw", "dance_queries")
    if os.path.exists(queries_dir):
        queries_files = [f for f in os.listdir(queries_dir) if os.path.isfile(os.path.join(queries_dir, f))]
        print(f"\ndance_queries directory contains {len(queries_files)} files")
    else:
        print("\ndance_queries directory not found!")
        
    # Check aggressive directory
    aggressive_dir = os.path.join("data_raw", "aggressive")
    if os.path.exists(aggressive_dir):
        aggressive_files = [f for f in os.listdir(aggressive_dir) if os.path.isfile(os.path.join(aggressive_dir, f))]
        print(f"aggressive directory contains {len(aggressive_files)} files")
    else:
        print("aggressive directory not found!")
    
    # Look for any dance style summary files
    summary_files = glob.glob(os.path.join("data_raw", "*summary*.json"))
    if summary_files:
        print(f"\nFound {len(summary_files)} summary files:")
        for summary_file in summary_files:
            print(f"  - {os.path.basename(summary_file)}")
            
            try:
                with open(summary_file, 'r') as f:
                    data = json.load(f)
                    if 'dance_style_total_counts' in data:
                        print("    Dance style total counts:")
                        for style, count in data['dance_style_total_counts'].items():
                            print(f"      - {style}: {count}")
            except Exception as e:
                print(f"    Error reading summary file: {e}")

if __name__ == "__main__":
    summarize_data_raw() 