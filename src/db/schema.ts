import { pgTable, serial, integer, text, char, doublePrecision, timestamp, customType } from 'drizzle-orm/pg-core';
import type { InferSelectModel, InferInsertModel } from 'drizzle-orm';

// Define custom types for PostGIS geography columns
// These tell Drizzle how to map these to SQL. The actual GEOGRAPHY type handling
// (e.g., from WKT) happens at the database level or during data insertion.
const geographyPoint = customType<{ data: string; driverData: string; notNull: false; hasDefault: false }>({ // Added notNull and hasDefault for completeness
  dataType() {
    return 'GEOGRAPHY(POINT, 4326)';
  },
});

const geographyPolygon = customType<{ data: string; driverData: string; notNull: false; hasDefault: false }>({ // Added notNull and hasDefault for completeness
  dataType() {
    return 'GEOGRAPHY(POLYGON, 4326)';
  },
});

export const metro = pgTable('metro', {
  metroId: serial('metro_id').primaryKey(),
  geonameid: integer('geonameid').unique().notNull(),
  name: text('name'),
  asciiname: text('asciiname'),
  alternatenames: text('alternatenames'), // Storing as raw text from CSV
  countryIso2: char('country_iso2', { length: 2 }),
  population: integer('population'),
  timezone: text('timezone'),
  // For tzOffsetMin and metroTier, PostgreSQL's SMALLINT is more space-efficient.
  // Drizzle's integer() maps to INTEGER. If strict SMALLINT is needed,
  // a customType for 'smallint' might be used, or accept INTEGER for now.
  tzOffsetMin: integer('tz_offset_min'),
  metroTier: integer('metro_tier'),
  latitude: doublePrecision('latitude').notNull(),
  longitude: doublePrecision('longitude').notNull(),
  slug: text('slug'),
  bboxWkt: text('bbox_wkt'),               // Raw WKT for the bounding box from CSV
  geom: geographyPoint('geom'),            // To be populated from latitude, longitude
  bbox: geographyPolygon('bbox'),          // To be populated from bboxWkt
  createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).defaultNow().notNull(),
});

// Types for inference
export type SelectMetro = InferSelectModel<typeof metro>;
export type InsertMetro = InferInsertModel<typeof metro>; 