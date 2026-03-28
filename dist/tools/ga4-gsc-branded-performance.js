"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4GscBrandedPerformance = ga4GscBrandedPerformance;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const url_normalise_js_1 = require("./url-normalise.js");
async function ga4GscBrandedPerformance(brandTerms, days = 28, gscDataset, ga4Dataset) {
    const config = (0, client_js_1.getConfig)();
    const gscDs = gscDataset || config.defaultDataset || "searchconsole";
    const ga4Ds = ga4Dataset || process.env.BIGQUERY_GA4_DATASET;
    (0, client_js_1.validateIdentifier)(gscDs, "gsc_dataset");
    if (!ga4Ds) {
        throw new Error("GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter.");
    }
    (0, client_js_1.validateIdentifier)(ga4Ds, "ga4_dataset");
    // Build regex from comma-separated brand terms
    const terms = brandTerms
        .split(",")
        .map((t) => t.trim().toLowerCase())
        .filter((t) => t.length > 0);
    if (terms.length === 0) {
        throw new Error("brand_terms is required. Provide comma-separated brand terms, e.g. 'suganthan,snippet digital'");
    }
    const brandRegex = terms.join("|");
    const normGa4 = (0, url_normalise_js_1.normaliseURL)(`(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')`);
    const normGsc = (0, url_normalise_js_1.normaliseURL)(`url`);
    const sql = `
    WITH gsc_branded AS (
      SELECT
        ${normGsc} AS landing_page,
        CASE
          WHEN REGEXP_CONTAINS(LOWER(query), r'${brandRegex}') THEN 'Branded'
          ELSE 'Non-branded'
        END AS traffic_type,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
        SUM(sum_top_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND NOT is_anonymized_query
      GROUP BY landing_page, traffic_type
    ),

    ga4_organic AS (
      SELECT
        ${normGa4} AS landing_page,
        COUNT(DISTINCT CONCAT(
          user_pseudo_id,
          CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) AS sessions,
        COUNTIF(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1'
        ) AS engaged_sessions,
        COUNTIF(event_name IN ('purchase', 'generate_lead', 'sign_up', 'begin_checkout')) AS key_events,
        SUM(ecommerce.purchase_revenue_in_usd) AS revenue
      FROM \`${ga4Ds}.events_*\`
      WHERE event_name = 'session_start'
        AND session_traffic_source_last_click.manual_campaign.source = 'google'
        AND session_traffic_source_last_click.manual_campaign.medium = 'organic'
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY))
                               AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      GROUP BY landing_page
    )

    SELECT
      gsc.traffic_type,
      SUM(gsc.clicks) AS clicks,
      SUM(gsc.impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(gsc.clicks), SUM(gsc.impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(gsc.clicks * gsc.avg_position), SUM(gsc.clicks)), 1) AS weighted_avg_pos,
      SUM(COALESCE(ga4.sessions, 0)) AS ga4_sessions,
      ROUND(SAFE_DIVIDE(
        SUM(COALESCE(ga4.engaged_sessions, 0)),
        SUM(COALESCE(ga4.sessions, 0))
      ) * 100, 1) AS engagement_rate_pct,
      SUM(COALESCE(ga4.key_events, 0)) AS conversions,
      ROUND(SAFE_DIVIDE(
        SUM(COALESCE(ga4.key_events, 0)),
        SUM(COALESCE(ga4.sessions, 0))
      ) * 100, 2) AS conversion_rate_pct,
      ROUND(SUM(COALESCE(ga4.revenue, 0)), 2) AS revenue
    FROM gsc_branded gsc
    LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
    GROUP BY gsc.traffic_type
    ORDER BY gsc.traffic_type
  `;
    return (0, query_js_1.runQuery)(sql, 10);
}
