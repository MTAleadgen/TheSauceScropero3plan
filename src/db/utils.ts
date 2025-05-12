import { customType, type CustomTypeParams } from 'drizzle-orm/pg-core';
import type { GeoJSON } from 'geojson'; // Assuming GeoJSON types are available globally or imported

// Custom type for PostGIS GEOGRAPHY(Point, 4326)
export const pgGeography = customType<{
    data: GeoJSON.Point; // Use GeoJSON Point type for data representation
    driverData: string; // Driver expects a string (WKT or similar)
    config: { srid: number };
}>(
    {
        // Define dataType with explicit config type annotation
        dataType(config?: { srid: number }): string {
            const srid = config?.srid ?? 4326;
            return `geography(Point, ${srid})`;
        },
        fromDriver(value: string): GeoJSON.Point {
            // Placeholder - implement WKT/EWKT/GeoJSON string parsing to GeoJSON object if needed for reads
            console.warn(`pgGeography fromDriver needs implementation to parse: ${value}`);
            return { type: "Point", coordinates: [0, 0] }; 
        },
        toDriver(value: GeoJSON.Point): string {
            // Convert GeoJSON Point to PostGIS ST_MakePoint format string
            // SRID can be assumed based on column definition or fetched from config if available
            // Let's assume the config passed to dataType defines the SRID for the column
            // We cannot reliably access the specific instance config here easily,
            // so we rely on the column definition + ST_SetSRID.
            
            const lon = Number(value.coordinates[0]);
            const lat = Number(value.coordinates[1]);
            if (isNaN(lon) || isNaN(lat)) {
                throw new Error(`Invalid coordinates for Point: ${value.coordinates}`);
            }
            // Return the SQL string for PostGIS function, assuming SRID 4326 if not specified elsewhere
            // The actual SRID applied depends on the column's definition (via dataType)
            // Using 4326 explicitly in ST_SetSRID is robust.
            return `ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)`; 
        }
    } satisfies CustomTypeParams<{ data: GeoJSON.Point; driverData: string; config: { srid: number }; }>
    // Add 'satisfies CustomTypeParams' for better type checking
); 