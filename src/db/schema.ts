import { pgTable, serial, integer, text, char, doublePrecision, timestamp, customType, boolean, uuid, jsonb, primaryKey, uniqueIndex, numeric } from 'drizzle-orm/pg-core';
import type { InferSelectModel, InferInsertModel } from 'drizzle-orm';
import { relations, sql } from 'drizzle-orm';
import { pgGeography } from './utils'; // Try explicit .js import for module resolution

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
  // tempCol: text('temp_col'), // Removed temporary column
});

// Event Tables
export const eventRaw = pgTable("event_raw", {
	id: serial("id").primaryKey(),
	source: text("source").notNull(),
	sourceEventId: text("source_event_id"),
	metroId: integer("metro_id").references(() => metro.geonameid),
	rawJson: jsonb("raw_json").notNull(),
	discoveredAt: timestamp("discovered_at", { precision: 3, mode: 'string' }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
	parsedAt: timestamp("parsed_at", { precision: 3, mode: 'string' }),
	normalizedAt: timestamp("normalized_at", { precision: 3, mode: 'string' }), 
	normalizationStatus: text("normalization_status"), 
}, (table) => ({
    sourceEventIdx: uniqueIndex("source_event_idx").on(table.source, table.sourceEventId),
}));

export const eventClean = pgTable("event_clean", {
	id: serial("id").primaryKey(),
	eventRawId: integer("event_raw_id").references(() => eventRaw.id),
	metroId: integer("metro_id").references(() => metro.geonameid),
	source: text("source"),
	sourceEventId: text("source_event_id"),
	title: text("title"),
	description: text("description"),
	url: text("url"),
	startTs: timestamp("start_ts", { withTimezone: true }),
	endTs: timestamp("end_ts", { withTimezone: true }),
	venueName: text("venue_name"),
	venueAddress: text("venue_address"),
	// Use customType for GEOGRAPHY
	venueGeom: pgGeography("venue_geom", { srid: 4326 }).$type<GeoJSON.Point>(),
	imageUrl: text("image_url"),
	tags: jsonb("tags"), // e.g., dance styles, event type
	qualityScore: numeric("quality_score").default('0'),
	fingerprint: char("fingerprint", { length: 16 }),
	normalizedAt: timestamp("normalized_at", { precision: 3, mode: 'string' }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
}, (table) => ({
    eventDupIdx: uniqueIndex("event_dup_idx").on(table.metroId, table.fingerprint),
}));

// Relations
export const metroRelations = relations(metro, ({many}) => ({
  eventsClean: many(eventClean, {
      relationName: "metroToEvents"
  }),
}));

export const eventRawRelations = relations(eventRaw, ({one}) => ({
  eventClean: one(eventClean, {
    fields: [eventRaw.id],
    references: [eventClean.eventRawId],
    relationName: "rawToClean"
  }),
  metro: one(metro, {
      fields: [eventRaw.metroId],
      references: [metro.geonameid],
      relationName: "rawToMetro"
  })
}));

export const eventCleanRelations = relations(eventClean, ({one}) => ({
  rawEvent: one(eventRaw, {
    fields: [eventClean.eventRawId],
    references: [eventRaw.id],
    relationName: "cleanToRaw"
  }),
  metro: one(metro, {
    fields: [eventClean.metroId],
    references: [metro.geonameid],
    relationName: "cleanToMetro"
  }),
}));

// Types for inference
export type SelectMetro = InferSelectModel<typeof metro>;
export type InsertMetro = InferInsertModel<typeof metro>;
export type SelectEventRaw = InferSelectModel<typeof eventRaw>;
export type InsertEventRaw = InferInsertModel<typeof eventRaw>;
export type SelectEventClean = InferSelectModel<typeof eventClean>;
export type InsertEventClean = InferInsertModel<typeof eventClean>; 