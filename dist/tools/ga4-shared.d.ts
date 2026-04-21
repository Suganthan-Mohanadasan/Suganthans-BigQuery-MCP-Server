/**
 * Shared GA4 session aggregation logic for GA4+GSC blending tools.
 *
 * GA4 BigQuery export is event-level: every page_view, scroll, click,
 * purchase is its own row. The 6 GA4+GSC tools all need to:
 *
 * 1. Filter to organic Google sessions (excluding paid that auto-tagging
 *    contaminates into the "google/organic" bucket).
 * 2. Identify the landing page for each session (first page_view).
 * 3. Aggregate engagement, conversion, and revenue at the session level.
 * 4. Roll up to landing-page level for joining with GSC.
 *
 * Without this aggregation, conversions count as zero because the WHERE
 * filter on event_name = 'session_start' excludes purchase, Lead_signup,
 * etc. Conversions fire on different events than session starts.
 */
/**
 * Returns the SQL for `organic_sessions` and `ga4_organic` CTEs.
 *
 * `organic_sessions` has one row per organic Google session with:
 *   user_pseudo_id, session_id, landing_page, is_engaged, conversions, revenue
 *
 * `ga4_organic` rolls those up to landing_page with:
 *   landing_page, sessions, users, engaged_sessions, conversions, revenue
 *
 * Filter logic uses GA4's authoritative channel grouping
 * (`session_traffic_source_last_click.cross_channel_campaign.default_channel_group`)
 * which correctly excludes paid traffic that Google auto-tagging puts into
 * the "google/organic" bucket via gclid parameters.
 *
 * @param projectId - BigQuery project containing the GA4 dataset
 * @param ga4Dataset - The GA4 dataset (e.g. `analytics_123456789`)
 * @param days - Lookback window in days
 */
export declare function ga4OrganicSessionsCTEs(projectId: string, ga4Dataset: string, days: number): string;
/**
 * Normalise a GSC URL column to match the GA4 page_location normalisation.
 * Strips query params and fragments, lowercases.
 */
export declare function normaliseGSCUrl(sqlColumn: string): string;
