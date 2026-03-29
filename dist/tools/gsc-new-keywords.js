"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscNewKeywords = gscNewKeywords;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscNewKeywords(recentDays = 7, baselineDays = 60, minImpressions = 10, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    WITH recent_queries AS (
      SELECT
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
      GROUP BY query
      HAVING impressions >= ${minImpressions}
    ),
    baseline_queries AS (
      SELECT DISTINCT query
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date BETWEEN
          DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays + baselineDays} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays + 1} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
    )
    SELECT
      r.query,
      r.clicks,
      r.impressions,
      r.avg_position
    FROM recent_queries r
    LEFT JOIN baseline_queries b ON r.query = b.query
    WHERE b.query IS NULL
    ORDER BY r.impressions DESC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
