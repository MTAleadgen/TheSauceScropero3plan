from shapely.geometry import Point, Polygon
from shapely.wkt import loads

# Coordinates for "One Astor Place, 1515, Broadway" (lon, lat)
# From normalize_worker logs for event 122
point_coords = (-73.9864456, 40.7579139) 

# BBOX WKT for New York City (metro_id = 30)
# From earlier run_psql_metro_query.py output, slightly corrected if there were parsing issues.
# Ensure this is the exact WKT string from your database for metro_id=30
nyc_bbox_wkt = "POLYGON((-74.25559 40.49612, -74.25559 40.91525, -73.69921 40.91525, -73.69921 40.49612, -74.25559 40.49612))"

def check_point_in_polygon(point_lon_lat, polygon_wkt):
    try:
        point = Point(point_lon_lat)
        polygon = loads(polygon_wkt)
        
        is_within = polygon.contains(point)
        print(f"Point {point_lon_lat} is within NYC polygon: {is_within}")
        
        if not is_within:
            print(f"Point: {point.wkt}")
            print(f"Polygon: {polygon.wkt}")
            # Optional: print distance to polygon boundary or centroid if helpful
            # print(f"Distance to polygon boundary: {point.distance(polygon.boundary)}")

    except Exception as e:
        print(f"Error checking point in polygon: {e}")

if __name__ == '__main__':
    check_point_in_polygon(point_coords, nyc_bbox_wkt) 