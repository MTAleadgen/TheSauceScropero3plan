ALTER TABLE "dance_events" DISABLE ROW LEVEL SECURITY;--> statement-breakpoint
DROP TABLE "dance_events" CASCADE;--> statement-breakpoint
ALTER TABLE "event_clean" DROP CONSTRAINT "event_clean_event_id_event_raw_event_id_fk";
--> statement-breakpoint
ALTER TABLE "event_clean" DROP CONSTRAINT "event_clean_metro_id_metro_metro_id_fk";
--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "id" serial PRIMARY KEY NOT NULL;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "event_raw_id" integer;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "source" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "source_event_id" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "description" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "url" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "venue_name" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "venue_address" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "venue_geom" "geography(Point, 4326)";--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "image_url" text;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "tags" jsonb;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "quality_score" numeric;--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "normalized_at" timestamp(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "id" serial PRIMARY KEY NOT NULL;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "source" text NOT NULL;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "source_event_id" text;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "metro_id" integer;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "raw_json" jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "discovered_at" timestamp(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL;--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "parsed_at" timestamp(3);--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_event_raw_id_event_raw_id_fk" FOREIGN KEY ("event_raw_id") REFERENCES "public"."event_raw"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_metro_id_metro_geonameid_fk" FOREIGN KEY ("metro_id") REFERENCES "public"."metro"("geonameid") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "event_raw" ADD CONSTRAINT "event_raw_metro_id_metro_geonameid_fk" FOREIGN KEY ("metro_id") REFERENCES "public"."metro"("geonameid") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "source_event_idx" ON "event_raw" USING btree ("source","source_event_id");--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "event_id";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "venue";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "raw_addr";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "lat";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "lon";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "geom";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "dance_tags";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "price_val";--> statement-breakpoint
ALTER TABLE "event_clean" DROP COLUMN "price_ccy";--> statement-breakpoint
ALTER TABLE "event_raw" DROP COLUMN "event_id";--> statement-breakpoint
ALTER TABLE "event_raw" DROP COLUMN "source_url";--> statement-breakpoint
ALTER TABLE "event_raw" DROP COLUMN "jsonld";--> statement-breakpoint
ALTER TABLE "event_raw" DROP COLUMN "fetched_at";