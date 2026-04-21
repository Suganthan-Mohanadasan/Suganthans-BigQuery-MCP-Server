"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4GscPagePerformance = ga4GscPagePerformance;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const url_normalise_js_1 = require("./url-normalise.js");
async function ga4GscPagePerformance(days = 28, minClicks = 10, maxRows = 50, gscDataset, ga4Dataset, projectId) {
    const config = (0, client_js_1.getConfig)();
    const gscDs = gscDataset || config.defaultDataset || "searchconsole";
    const ga4Ds = ga4Dataset || process.env.BIGQUERY_GA4_DATASET;
    const targetProject = projectId || config.ga4ProjectId || config.projectId;
    (0, client_js_1.validateIdentifier)(gscDs, "gsc_dataset");
    if (!ga4Ds) {
        throw new Error("GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter. " +
            "The dataset name looks like 'analytics_123456789'.");
    }
    (0, client_js_1.validateIdentifier)(ga4Ds, "ga4_dataset");
    if (projectId)
        (0, client_js_1.validateIdentifier)(projectId, "project_id");
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
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1'
        ) AS engaged_sessions,
        COUNTIF(event_name IN ('purchase', 'generate_lead', 'sign_up', 'begin_checkout', 'Lead_signup')) AS key_events,
        SUM(ecommerce.purchase_revenue_in_usd) AS revenue
      FROM \`${targetProject}.${ga4Ds}.events_*\`
      WHERE event_name = 'session_start'
        AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group = 'Organic Search'
        AND collected_traffic_source.gclid IS NULL
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY))
                               AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      GROUP BY landing_page
    ),

    gsc AS (
      SELECT
        ${normGsc} AS landing_page,
        SUM(clicks) AS gsc_clicks,
        SUM(impressions) AS gsc_impressions,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS gsc_ctr,
        SUM(sum_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${targetProject}.${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY landing_page
    )

    SELECT
      COALESCE(gsc.landing_page, ga4.landing_page) AS page,
      gsc.gsc_clicks AS clicks,
      gsc.gsc_impressions AS impressions,
      ROUND(gsc.gsc_ctr * 100, 2) AS ctr_pct,
      ROUND(gsc.avg_position, 1) AS avg_pos,
      ga4.sessions AS ga4_sessions,
      ga4.users AS ga4_users,
      ga4.engaged_sessions,
      ROUND(SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) * 100, 1) AS engagement_rate_pct,
      ga4.key_events AS conversions,
      ROUND(SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100, 2) AS conversion_rate_pct,
      ROUND(COALESCE(ga4.revenue, 0), 2) AS revenue
    FROM gsc
    LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
    WHERE gsc.gsc_clicks >= ${minClicks}
    ORDER BY gsc.gsc_clicks DESC
    LIMIT ${maxRows}
  `;
    return (0, query_js_1.runQuery)(sql, maxRows, targetProject);
}
