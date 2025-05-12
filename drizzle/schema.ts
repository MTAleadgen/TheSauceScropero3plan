import { pgTable, serial, text, date, boolean, timestamp, unique, integer, char, doublePrecision, index, foreignKey, uuid, numeric, jsonb, check, varchar, pgView } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"



export const danceEvents = pgTable("dance_events", {
	id: serial().primaryKey().notNull(),
	title: text().notNull(),
	venue: text(),
	phoneNumber: text("phone_number"),
	address: text(),
	price: text(),
	date: date().notNull(),
	startTime: text("start_time"),
	endTime: text("end_time"),
	danceStyles: text("dance_styles").array(),
	webNavigationLink: text("web_navigation_link"),
	classBefore: boolean("class_before"),
	liveBand: boolean("live_band"),
	source: text(),
	createdAt: timestamp("created_at", { mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
});

export const metro = pgTable("metro", {
	metroId: serial("metro_id").primaryKey().notNull(),
	geonameid: integer().notNull(),
	name: text(),
	asciiname: text(),
	alternatenames: text(),
	countryIso2: char("country_iso2", { length: 2 }),
	population: integer(),
	timezone: text(),
	tzOffsetMin: integer("tz_offset_min"),
	metroTier: integer("metro_tier"),
	latitude: doublePrecision().notNull(),
	longitude: doublePrecision().notNull(),
	slug: text(),
	bboxWkt: text("bbox_wkt"),
	// TODO: failed to parse database type 'geography'
	geom: unknown("geom"),
	// TODO: failed to parse database type 'geography'
	bbox: unknown("bbox"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
}, (table) => [
	unique("metro_geonameid_unique").on(table.geonameid),
]);

export const eventClean = pgTable("event_clean", {
	eventId: uuid("event_id").primaryKey().notNull(),
	title: text(),
	startTs: timestamp("start_ts", { withTimezone: true, mode: 'string' }),
	endTs: timestamp("end_ts", { withTimezone: true, mode: 'string' }),
	venue: text(),
	rawAddr: text("raw_addr"),
	lat: doublePrecision(),
	lon: doublePrecision(),
	// TODO: failed to parse database type 'geography'
	geom: unknown("geom"),
	danceTags: text("dance_tags").array(),
	priceVal: numeric("price_val"),
	priceCcy: char("price_ccy", { length: 3 }),
	metroId: integer("metro_id"),
}, (table) => [
	index("event_geo_idx").using("gist", table.geom.asc().nullsLast().op("gist_geography_ops")),
	foreignKey({
			columns: [table.eventId],
			foreignColumns: [eventRaw.eventId],
			name: "event_clean_event_id_event_raw_event_id_fk"
		}),
	foreignKey({
			columns: [table.eventId],
			foreignColumns: [eventRaw.eventId],
			name: "event_clean_event_id_fkey"
		}),
	foreignKey({
			columns: [table.metroId],
			foreignColumns: [metro.metroId],
			name: "event_clean_metro_id_fkey"
		}),
	foreignKey({
			columns: [table.metroId],
			foreignColumns: [metro.metroId],
			name: "event_clean_metro_id_metro_metro_id_fk"
		}),
]);

export const eventRaw = pgTable("event_raw", {
	eventId: uuid("event_id").primaryKey().notNull(),
	sourceUrl: text("source_url"),
	jsonld: jsonb(),
	fetchedAt: timestamp("fetched_at", { withTimezone: true, mode: 'string' }),
});

export const spatialRefSys = pgTable("spatial_ref_sys", {
	srid: integer().notNull(),
	authName: varchar("auth_name", { length: 256 }),
	authSrid: integer("auth_srid"),
	srtext: varchar({ length: 2048 }),
	proj4Text: varchar({ length: 2048 }),
}, (table) => [
	check("spatial_ref_sys_srid_check", sql`(srid > 0) AND (srid <= 998999)`),
]);
export const geographyColumns = pgView("geography_columns", {	// TODO: failed to parse database type 'name'
	fTableCatalog: unknown("f_table_catalog"),
	// TODO: failed to parse database type 'name'
	fTableSchema: unknown("f_table_schema"),
	// TODO: failed to parse database type 'name'
	fTableName: unknown("f_table_name"),
	// TODO: failed to parse database type 'name'
	fGeographyColumn: unknown("f_geography_column"),
	coordDimension: integer("coord_dimension"),
	srid: integer(),
	type: text(),
}).as(sql`SELECT current_database() AS f_table_catalog, n.nspname AS f_table_schema, c.relname AS f_table_name, a.attname AS f_geography_column, postgis_typmod_dims(a.atttypmod) AS coord_dimension, postgis_typmod_srid(a.atttypmod) AS srid, postgis_typmod_type(a.atttypmod) AS type FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n WHERE t.typname = 'geography'::name AND a.attisdropped = false AND a.atttypid = t.oid AND a.attrelid = c.oid AND c.relnamespace = n.oid AND (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'm'::"char", 'f'::"char", 'p'::"char"])) AND NOT pg_is_other_temp_schema(c.relnamespace) AND has_table_privilege(c.oid, 'SELECT'::text)`);

export const geometryColumns = pgView("geometry_columns", {	fTableCatalog: varchar("f_table_catalog", { length: 256 }),
	// TODO: failed to parse database type 'name'
	fTableSchema: unknown("f_table_schema"),
	// TODO: failed to parse database type 'name'
	fTableName: unknown("f_table_name"),
	// TODO: failed to parse database type 'name'
	fGeometryColumn: unknown("f_geometry_column"),
	coordDimension: integer("coord_dimension"),
	srid: integer(),
	type: varchar({ length: 30 }),
}).as(sql`SELECT current_database()::character varying(256) AS f_table_catalog, n.nspname AS f_table_schema, c.relname AS f_table_name, a.attname AS f_geometry_column, COALESCE(postgis_typmod_dims(a.atttypmod), sn.ndims, 2) AS coord_dimension, COALESCE(NULLIF(postgis_typmod_srid(a.atttypmod), 0), sr.srid, 0) AS srid, replace(replace(COALESCE(NULLIF(upper(postgis_typmod_type(a.atttypmod)), 'GEOMETRY'::text), st.type, 'GEOMETRY'::text), 'ZM'::text, ''::text), 'Z'::text, ''::text)::character varying(30) AS type FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid AND NOT a.attisdropped JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN ( SELECT s.connamespace, s.conrelid, s.conkey, replace(split_part(s.consrc, ''''::text, 2), ')'::text, ''::text) AS type FROM ( SELECT pg_constraint.connamespace, pg_constraint.conrelid, pg_constraint.conkey, pg_get_constraintdef(pg_constraint.oid) AS consrc FROM pg_constraint) s WHERE s.consrc ~~* '%geometrytype(% = %'::text) st ON st.connamespace = n.oid AND st.conrelid = c.oid AND (a.attnum = ANY (st.conkey)) LEFT JOIN ( SELECT s.connamespace, s.conrelid, s.conkey, replace(split_part(s.consrc, ' = '::text, 2), ')'::text, ''::text)::integer AS ndims FROM ( SELECT pg_constraint.connamespace, pg_constraint.conrelid, pg_constraint.conkey, pg_get_constraintdef(pg_constraint.oid) AS consrc FROM pg_constraint) s WHERE s.consrc ~~* '%ndims(% = %'::text) sn ON sn.connamespace = n.oid AND sn.conrelid = c.oid AND (a.attnum = ANY (sn.conkey)) LEFT JOIN ( SELECT s.connamespace, s.conrelid, s.conkey, replace(replace(split_part(s.consrc, ' = '::text, 2), ')'::text, ''::text), '('::text, ''::text)::integer AS srid FROM ( SELECT pg_constraint.connamespace, pg_constraint.conrelid, pg_constraint.conkey, pg_get_constraintdef(pg_constraint.oid) AS consrc FROM pg_constraint) s WHERE s.consrc ~~* '%srid(% = %'::text) sr ON sr.connamespace = n.oid AND sr.conrelid = c.oid AND (a.attnum = ANY (sr.conkey)) WHERE (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'm'::"char", 'f'::"char", 'p'::"char"])) AND NOT c.relname = 'raster_columns'::name AND t.typname = 'geometry'::name AND NOT pg_is_other_temp_schema(c.relnamespace) AND has_table_privilege(c.oid, 'SELECT'::text)`);