# Solution to DataForSEO API Issue - `Invalid Field: 'location_name'`

## Problem Summary

When using the DataForSEO Google Organic Search API (`/v3/serp/google/organic/live/advanced`), we were consistently getting the error:

```
Invalid Field: 'location_name'
```

This error occurred even when we thought we weren't including the `location_name` field in our API requests.

## Root Cause

After further investigation and guidance from the API documentation, we discovered:

1. The `location_name` parameter is **required** by the Google Organic Search API endpoint, not optional or prohibited as we initially assumed.

2. We needed to properly format the `location_name` parameter as `"city,country"` (e.g., "New York,United States").

3. Our approach of removing all location parameters and putting location in the search query was incorrect. The API expects location information as a separate parameter.

## Solution

The correct solution is to:

1. **Include the `location_name` parameter** in all Google Organic Search API requests.

2. Format the location_name parameter correctly as `"city,country"` (e.g., "New York,United States").

3. Use `language_name` (e.g., "English") instead of `language_code` for better compatibility.

4. Keep the search query focused on the specific term (e.g., "salsa dance") without including location in the query itself.

## Example of Correct API Request

```json
[
  {
    "keyword": "salsa dance",
    "location_name": "New York,United States",
    "language_name": "English",
    "depth": 10,
    "se_domain": "google.com"
  }
]
```

## Implementation Changes

The following changes were made to fix the issue:

1. Added proper `location_name` parameter with city and country information to all API requests
2. Used `language_name` instead of `language_code` 
3. Removed location information from the search query
4. Updated the code to construct location_name in the format "city,country"

## Lessons Learned

1. API documentation must be carefully read and followed - parameter requirements can differ between endpoints and are often mandatory.

2. When encountering persistent errors, verify the expected parameter format in the API documentation rather than removing parameters.

3. Test with a simple, minimal script that follows the exact format shown in documentation examples.

4. Parameter formatting can be critical - even missing commas or incorrect spacing in compound parameters can cause errors. 