-- Removed CREATE TABLE "dance_events" as it exists or is not managed by Drizzle here.
-- Removed CREATE TABLE "event_clean" as it already exists.
-- Removed CREATE TABLE "event_raw" as it already exists.

--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_event_id_event_raw_event_id_fk" FOREIGN KEY ("event_id") REFERENCES "public"."event_raw"("event_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "event_clean" ADD CONSTRAINT "event_clean_metro_id_metro_metro_id_fk" FOREIGN KEY ("metro_id") REFERENCES "public"."metro"("metro_id") ON DELETE no action ON UPDATE no action;