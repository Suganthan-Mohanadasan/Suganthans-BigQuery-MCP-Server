/**
 * URL normalisation for joining GA4 and GSC BigQuery data.
 *
 * GA4 page_location includes query params, fragments, UTMs.
 * GSC url is canonicalised and clean.
 * Both must be normalised identically for joins to work.
 *
 * Steps:
 * 1. Strip query parameters (everything after ?)
 * 2. Strip fragment identifiers (everything after #)
 * 3. Lowercase for consistency
 */
export function normaliseURL(sqlColumn: string): string {
  return `LOWER(REGEXP_REPLACE(${sqlColumn}, r'[\\?#].*$', ''))`;
}
