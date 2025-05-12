ALTER TABLE "event_clean" ALTER COLUMN "quality_score" SET DEFAULT '0';--> statement-breakpoint
ALTER TABLE "event_clean" ADD COLUMN "fingerprint" char(16);--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "normalized_at" timestamp(3);--> statement-breakpoint
ALTER TABLE "event_raw" ADD COLUMN "normalization_status" text;--> statement-breakpoint
CREATE UNIQUE INDEX "event_dup_idx" ON "event_clean" USING btree ("metro_id","fingerprint");