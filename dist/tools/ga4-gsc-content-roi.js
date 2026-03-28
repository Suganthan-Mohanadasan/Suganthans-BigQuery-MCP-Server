"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4GscContentRoi = ga4GscContentRoi;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const url_normalise_js_1 = require("./url-normalise.js");
async function ga4GscContentRoi(days = 28, minClicks = 20, maxRows = 50, gscDataset, ga4Dataset) {
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
    WITH ga4_organic AS (
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
    ),

    gsc AS (
      SELECT
        ${normGsc} AS landing_page,
        SUM(clicks) AS gsc_clicks,
        SUM(impressions) AS gsc_impressions,
        SUM(sum_top_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY landing_page
    ),

    joined AS (
      SELECT
        gsc.landing_page AS page,
        gsc.gsc_clicks AS clicks,
        gsc.gsc_impressions AS impressions,
        ROUND(gsc.avg_position, 1) AS avg_pos,
        COALESCE(ga4.sessions, 0) AS ga4_sessions,
        ROUND(SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) * 100, 1) AS engagement_rate_pct,
        ROUND(SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100, 2) AS conversion_rate_pct,
        ROUND(COALESCE(ga4.revenue, 0), 2) AS revenue,
        CASE
          WHEN gsc.avg_position <= 10 AND SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100 < 1 THEN 'High rank, low conversion'
          WHEN gsc.avg_position > 20 AND SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100 >= 3 THEN 'Low rank, high conversion'
          WHEN gsc.avg_position <= 5 AND SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100 >= 3 THEN 'Top performer'
          WHEN SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) * 100 < 40 THEN 'Engagement problem'
          ELSE 'Normal'
        END AS diagnosis
      FROM gsc
      LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
      WHERE gsc.gsc_clicks >= ${minClicks}
        AND ga4.sessions IS NOT NULL
        AND ga4.sessions > 0
    )

    SELECT *
    FROM joined
    WHERE diagnosis != 'Normal'
    ORDER BY
      CASE diagnosis
        WHEN 'Low rank, high conversion' THEN 1
        WHEN 'High rank, low conversion' THEN 2
        WHEN 'Engagement problem' THEN 3
        WHEN 'Top performer' THEN 4
        ELSE 5
      END,
      clicks DESC
    LIMIT ${maxRows}
  `;
    return (0, query_js_1.runQuery)(sql, maxRows);
}
