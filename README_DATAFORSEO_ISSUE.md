# DataForSEO API Issue - `Invalid Field: 'location_name'`

## Problem Summary

When attempting to use the DataForSEO Google Organic Search API (`/v3/serp/google/organic/live/advanced`), we're consistently getting the error:

```
Invalid Field: 'location_name'
```

This error occurs even when:
- No `location_name` field is provided in the API payload
- Using a minimal payload with only required fields (`keyword`, `language_code`, `depth`, `se_domain`)
- Trying queries with and without location information in the search keyword

## Debugging Steps Taken

1. Removed all location parameters from API payloads
2. Created a minimal debug script (`debug_dataforseo.py`) to send basic requests
3. Tested with different city names and generic queries
4. Ensured no `location_name` field is present in any way

## Suspected Issues

1. **API Account Configuration**: There may be an issue with how the DataForSEO account is configured for organic search.
2. **API Endpoint Understanding**: The organic search endpoint may have specific requirements that aren't clearly documented.
3. **Default Parameters**: The API client library or DataForSEO backend might be adding default parameters.

## Recommended Next Steps

1. **Contact DataForSEO Support**: Share the `debug_dataforseo.py` script and error details with their support team to get guidance on the correct API usage.

2. **Check API Documentation**: Verify if there are any undocumented requirements for the organic search endpoint.

3. **Try Different Endpoint**: If possible, test the `/v3/serp/google/organic/task_post` endpoint to submit batch tasks instead of live requests.

4. **Use Google Events API**: Since our events API is working, you could continue using that in the meantime.

## Sample Valid Payload Structure

According to our tests, this minimal payload still generates the error:

```json
[
  {
    "keyword": "salsa dance",
    "language_code": "en",
    "depth": 10,
    "se_domain": "google.com"
  }
]
```

## API Response from Debug Script

```json
{
  "version": "0.1.20250425",
  "status_code": 20000,
  "status_message": "Ok.",
  "time": "0.0408 sec.",
  "cost": 0,
  "tasks_count": 1,
  "tasks_error": 1,
  "tasks": [
    {
      "id": "05120545-9976-0139-0000-3bd7c6482e76",
      "status_code": 40501,
      "status_message": "Invalid Field: 'location_name'.",
      "time": "0 sec.",
      "cost": 0,
      "result_count": 0,
      "path": [
        "v3",
        "serp",
        "google",
        "organic",
        "live",
        "advanced"
      ],
      "data": {
        "api": "serp",
        "function": "live",
        "se": "google",
        "se_type": "organic",
        "keyword": "salsa dance",
        "language_code": "en",
        "depth": 10,
        "se_domain": "google.com"
      },
      "result": null
    }
  ]
}
``` 