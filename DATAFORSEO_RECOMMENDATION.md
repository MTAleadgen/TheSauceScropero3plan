# DataForSEO API Issue Solution

## Issue Resolution

We have successfully resolved the "Invalid Field: 'location_name'" error that was occurring with the DataForSEO Google Organic Search API.

## Root Cause

The error was happening because:

1. The `location_name` parameter is **required** by the Google Organic Search API, not prohibited as we initially assumed.

2. Our approach of removing all location parameters and including location in the search query was incorrect.

## Working Solution

We implemented the following solution:

1. Added the required `location_name` parameter formatted as "city,country" (e.g., "New York,United States").

2. Used `language_name` (e.g., "English") instead of `language_code` for better compatibility.

3. Kept the search query focused on the specific term without including location.

## Example of Working API Request

```json
{
  "keyword": "salsa dance",
  "location_name": "New York,United States",
  "language_name": "English",
  "depth": 10,
  "se_domain": "google.com"
}
```

## Implementation Details

We updated the following files:

1. `debug_dataforseo.py` - Updated to use the correct parameters
2. `services/discovery/discovery_enhanced_organic_only.py`:
   - Updated `get_dataforseo_results_for_dance_style` function
   - Updated `batch_organic_style_tasks` function
3. `test_task_post_endpoint.py` - Created a working test script for the task_post endpoint

## Verification

We verified the solution through:

1. Testing the debug script, which now returns successful results
2. Testing the task_post endpoint, which now successfully creates tasks

## Next Steps

1. Continue using the updated code for organic search API calls
2. If any issues persist, verify location names are in the correct format "city,country"
3. Consider caching API results to reduce API usage 