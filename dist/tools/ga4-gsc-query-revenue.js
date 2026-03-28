"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4GscQueryRevenue = ga4GscQueryRevenue;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const url_normalise_js_1 = require("./url-normalise.js");
async function ga4GscQueryRevenue(days = 28, minClicks = 5, maxRows = 50, gscDataset, ga4Dataset) {
    const config = (0, client_js_1.getConfig)();
    const gscDs = gscDataset || config.defaultDataset || "searchconsole";
    const ga4Ds = ga4Dataset || process.env.BIGQUERY_GA4_DATASET;
    (0, client_js_1.validateIdentifier)(gscDs, "gsc_dataset");
    if (!ga4Ds) {
        throw new Error("GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter.");
    }
    (0, client_js_1.validateIdentifier)(ga4Ds, "ga4_dataset");
    const normGa4 = (0, url_normalise_js_1.normaliseURL)(`(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')`);
    const normGsc = (0, url_normalise_js_1.normaliseURL)(`url`);
    const sql = `
    WITH gsc_queries AS (
      SELECT
        query,
        ${normGsc} AS landing_page,
        SUM(clicks) AS query_clicks,
        SUM(impressions) AS impressions,
        SUM(sum_top_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND NOT is_anonymized_query
        AND clicks > 0
      GROUP BY query, landing_page
    ),

    gsc_with_share AS (
      SELECT *,
        SUM(query_clicks) OVER (PARTITION BY landing_page) AS total_page_clicks,
        SAFE_DIVIDE(query_clicks, SUM(query_clicks) OVER (PARTITION BY landing_page)) AS click_share
      FROM gsc_queries
    ),

    ga4_page_revenue AS (
      SELECT
        ${normGa4} AS landing_page,
        COUNT(DISTINCT CONCAT(
          user_pseudo_id,
          CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) AS sessions,
        COUNTIF(event_name IN ('purchase', 'generate_lead', 'sign_up', 'begin_checkout')) AS key_events,
        SUM(ecommerce.purchase_revenue_in_usd) AS revenue
      FROM \`${ga4Ds}.events_*\`
      WHERE session_traffic_source_last_click.manual_campaign.source = 'google'
        AND session_traffic_source_last_click.manual_campaign.medium = 'organic'
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY))
                               AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      GROUP BY landing_page
    )

    SELECT
      gsc.query,
      gsc.landing_page AS page,
      gsc.query_clicks AS clicks,
      gsc.impressions,
      ROUND(gsc.avg_position, 1) AS avg_pos,
      ROUND(gsc.click_share * 100, 1) AS click_share_pct,
      CAST(ROUND(COALESCE(ga4.sessions, 0) * gsc.click_share) AS INT64) AS attributed_sessions,
      CAST(ROUND(COALESCE(ga4.key_events, 0) * gsc.click_share) AS INT64) AS attributed_conversions,
      ROUND(COALESCE(ga4.revenue, 0) * gsc.click_share, 2) AS attributed_revenue
    FROM gsc_with_share gsc
    LEFT JOIN ga4_page_revenue ga4 ON gsc.landing_page = ga4.landing_page
    WHERE gsc.query_clicks >= ${minClicks}
    ORDER BY attributed_revenue DESC, attributed_conversions DESC
    LIMIT ${maxRows}
  `;
    return (0, query_js_1.runQuery)(sql, maxRows);
}
