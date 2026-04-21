"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4OrganicSessionsCTEs = ga4OrganicSessionsCTEs;
exports.normaliseGSCUrl = normaliseGSCUrl;
const KEY_EVENTS = [
    "purchase",
    "generate_lead",
    "sign_up",
    "begin_checkout",
    "Lead_signup",
];
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
function ga4OrganicSessionsCTEs(projectId, ga4Dataset, days) {
    const keyEventsList = KEY_EVENTS.map((e) => `'${e}'`).join(", ");
    return `
    organic_events AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        event_name,
        event_timestamp,
        LOWER(REGEXP_REPLACE(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
          r'[\\?#].*$', ''
        )) AS page_url,
        IF((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1', 1, 0) AS event_engaged,
        IF(event_name IN (${keyEventsList}), 1, 0) AS is_conversion,
        IFNULL(ecommerce.purchase_revenue_in_usd, 0) AS event_revenue
      FROM \`${projectId}.${ga4Dataset}.events_*\`
      WHERE session_traffic_source_last_click.cross_channel_campaign.default_channel_group = 'Organic Search'
        AND collected_traffic_source.gclid IS NULL
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY))
                               AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    ),

    organic_sessions AS (
      SELECT
        user_pseudo_id,
        session_id,
        ARRAY_AGG(
          IF(event_name = 'page_view', page_url, NULL)
          IGNORE NULLS
          ORDER BY event_timestamp ASC
          LIMIT 1
        )[SAFE_OFFSET(0)] AS landing_page,
        MAX(event_engaged) AS is_engaged,
        SUM(is_conversion) AS conversions,
        SUM(event_revenue) AS revenue
      FROM organic_events
      WHERE session_id IS NOT NULL
      GROUP BY user_pseudo_id, session_id
    ),

    ga4_organic AS (
      SELECT
        landing_page,
        COUNT(*) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        SUM(is_engaged) AS engaged_sessions,
        SUM(conversions) AS key_events,
        SUM(revenue) AS revenue
      FROM organic_sessions
      WHERE landing_page IS NOT NULL
      GROUP BY landing_page
    )
  `;
}
/**
 * Normalise a GSC URL column to match the GA4 page_location normalisation.
 * Strips query params and fragments, lowercases.
 */
function normaliseGSCUrl(sqlColumn) {
    return `LOWER(REGEXP_REPLACE(${sqlColumn}, r'[\\?#].*$', ''))`;
}
