"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ga4GscPositionValue = ga4GscPositionValue;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const ga4_shared_js_1 = require("./ga4-shared.js");
async function ga4GscPositionValue(days = 90, maxRows = 20, gscDataset, ga4Dataset, projectId) {
    const config = (0, client_js_1.getConfig)();
    const gscDs = gscDataset || config.defaultDataset || "searchconsole";
    const ga4Ds = ga4Dataset || process.env.BIGQUERY_GA4_DATASET;
    const targetProject = projectId || config.ga4ProjectId || config.projectId;
    (0, client_js_1.validateIdentifier)(gscDs, "gsc_dataset");
    if (!ga4Ds) {
        throw new Error("GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter.");
    }
    (0, client_js_1.validateIdentifier)(ga4Ds, "ga4_dataset");
    if (projectId)
        (0, client_js_1.validateIdentifier)(projectId, "project_id");
    const sql = `
    WITH ${(0, ga4_shared_js_1.ga4OrganicSessionsCTEs)(targetProject, ga4Ds, days)},

    page_positions AS (
      SELECT
        ${(0, ga4_shared_js_1.normaliseGSCUrl)("url")} AS landing_page,
        SUM(sum_position) / SUM(impressions) + 1 AS avg_position,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        CASE
          WHEN SUM(sum_position) / SUM(impressions) + 1 <= 1.5 THEN 'Position 1'
          WHEN SUM(sum_position) / SUM(impressions) + 1 <= 3.5 THEN 'Position 2-3'
          WHEN SUM(sum_position) / SUM(impressions) + 1 <= 5.5 THEN 'Position 4-5'
          WHEN SUM(sum_position) / SUM(impressions) + 1 <= 10.5 THEN 'Position 6-10'
          WHEN SUM(sum_position) / SUM(impressions) + 1 <= 20.5 THEN 'Position 11-20'
          ELSE 'Position 20+'
        END AS position_bucket
      FROM \`${targetProject}.${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY landing_page
    )

    SELECT
      pp.position_bucket,
      SUM(pp.clicks) AS total_clicks,
      SUM(pp.impressions) AS total_impressions,
      ROUND(SAFE_DIVIDE(SUM(pp.clicks), SUM(pp.impressions)) * 100, 2) AS avg_ctr_pct,
      SUM(COALESCE(ga4.sessions, 0)) AS total_sessions,
      SUM(COALESCE(ga4.key_events, 0)) AS total_conversions,
      ROUND(SAFE_DIVIDE(SUM(COALESCE(ga4.key_events, 0)), SUM(COALESCE(ga4.sessions, 0))) * 100, 2) AS conversion_rate_pct,
      ROUND(SUM(COALESCE(ga4.revenue, 0)), 2) AS total_revenue,
      ROUND(SAFE_DIVIDE(SUM(COALESCE(ga4.revenue, 0)), SUM(pp.clicks)), 2) AS revenue_per_click
    FROM page_positions pp
    LEFT JOIN ga4_organic ga4 ON pp.landing_page = ga4.landing_page
    GROUP BY pp.position_bucket
    ORDER BY
      CASE pp.position_bucket
        WHEN 'Position 1' THEN 1
        WHEN 'Position 2-3' THEN 2
        WHEN 'Position 4-5' THEN 3
        WHEN 'Position 6-10' THEN 4
        WHEN 'Position 11-20' THEN 5
        ELSE 6
      END
    LIMIT ${maxRows}
  `;
    return (0, query_js_1.runQuery)(sql, maxRows, targetProject);
}
