import redis
import json

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Test data with a more precise NYC address for Minskoff Theatre
data = {
    "url": "https://example.com/minskoff-theatre-event",
    "blob": {
        "@type": "Event",
        "name": "Salsa Night at Minskoff Theatre",
        "startDate": "2024-07-20T20:00:00",
        "description": "A fantastic night of salsa dancing at a famous Broadway theatre!",
        "location": {
            "@type": "Place",
            "name": "Minskoff Theatre",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": "1515 Broadway", 
                "addressLocality": "New York",
                "addressRegion": "NY",
                "postalCode": "10036",
                "addressCountry": "US"
            }
        },
        "offers": [
            {
                "@type": "Offer",
                "price": "25.00",
                "priceCurrency": "USD",
                "availability": "https://schema.org/InStock",
                "url": "https://example.com/tickets-minskoff"
            }
        ]
    }
}

# Push to queue
r.lpush('jsonld_raw', json.dumps(data))

print(f"Test data for '{data['blob']['name']}' pushed to jsonld_raw queue") 