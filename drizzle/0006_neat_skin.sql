CREATE TABLE "event_clean" (
	"id" serial PRIMARY KEY NOT NULL,
	"event_raw_id" integer,
	"metro_id" integer,
	"source" text,
	"source_event_id" text,
	"title" text,
	"description" text,
	"url" text,
	"start_ts" timestamp with time zone,
	"end_ts" timestamp with time zone,
	"venue_name" text,
	"venue_address" text,
	"venue_geom" GEOGRAPHY(Point, 4326),
	"image_url" text,
	"tags" jsonb,
	"quality_score" numeric DEFAULT '0',
	"fingerprint" char(16),
	"normalized_at" timestamp(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL
);
--> statement-breakpoint
CREATE TABLE "event_raw" (
	"id" serial PRIMARY KEY NOT NULL,
	"source" text NOT NULL,
	"source_event_id" text,
	"metro_id" integer,
	"raw_json" jsonb NOT NULL,
	"discovered_at" timestamp(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
	"parsed_at" timestamp(3),
	"normalized_at" timestamp(3),
	"normalization_status" text
);
--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_event_raw_id_event_raw_id_fk" FOREIGN KEY ("event_raw_id") REFERENCES "public"."event_raw"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_metro_id_metro_geonameid_fk" FOREIGN KEY ("metro_id") REFERENCES "public"."metro"("geonameid") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "event_raw" ADD CONSTRAINT "event_raw_metro_id_metro_geonameid_fk" FOREIGN KEY ("metro_id") REFERENCES "public"."metro"("geonameid") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "event_dup_idx" ON "event_clean" USING btree ("metro_id","fingerprint");--> statement-breakpoint
CREATE UNIQUE INDEX "source_event_idx" ON "event_raw" USING btree ("source","source_event_id");