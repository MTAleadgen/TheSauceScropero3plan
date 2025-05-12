import { relations } from "drizzle-orm/relations";
import { eventRaw, eventClean, metro } from "./schema";

export const eventCleanRelations = relations(eventClean, ({one}) => ({
	eventRaw_eventId: one(eventRaw, {
		fields: [eventClean.eventId],
		references: [eventRaw.eventId],
		relationName: "eventClean_eventId_eventRaw_eventId"
	}),
	eventRaw_eventId: one(eventRaw, {
		fields: [eventClean.eventId],
		references: [eventRaw.eventId],
		relationName: "eventClean_eventId_eventRaw_eventId"
	}),
	metro_metroId: one(metro, {
		fields: [eventClean.metroId],
		references: [metro.metroId],
		relationName: "eventClean_metroId_metro_metroId"
	}),
	metro_metroId: one(metro, {
		fields: [eventClean.metroId],
		references: [metro.metroId],
		relationName: "eventClean_metroId_metro_metroId"
	}),
}));

export const eventRawRelations = relations(eventRaw, ({many}) => ({
	eventCleans_eventId: many(eventClean, {
		relationName: "eventClean_eventId_eventRaw_eventId"
	}),
	eventCleans_eventId: many(eventClean, {
		relationName: "eventClean_eventId_eventRaw_eventId"
	}),
}));

export const metroRelations = relations(metro, ({many}) => ({
	eventCleans_metroId: many(eventClean, {
		relationName: "eventClean_metroId_metro_metroId"
	}),
	eventCleans_metroId: many(eventClean, {
		relationName: "eventClean_metroId_metro_metroId"
	}),
}));