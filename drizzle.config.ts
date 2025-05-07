import { defineConfig } from 'drizzle-kit';
import * as dotenv from 'dotenv';

dotenv.config({
  path: '.env.local',
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  // It's good practice to ensure DATABASE_URL is actually a string if found.
  // However, the guard rail for it not being set is the most critical part here.
  throw new Error('DATABASE_URL is not set or is empty in .env.local');
}

// This is the standard configuration structure for PostgreSQL with Drizzle Kit.
// The `driver: 'pg'` should discriminate the union type `Config`.
export default defineConfig({
  dialect: 'postgresql', // Specifies the SQL dialect
  schema: './src/db/schema.ts',
  out: './drizzle',
  dbCredentials: {
    url: databaseUrl, // For postgresql dialect, it often expects 'url' instead of 'connectionString'
  },
  verbose: true,
  strict: true,
}); 