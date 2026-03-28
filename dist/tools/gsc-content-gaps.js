"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscContentGaps = gscContentGaps;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscContentGaps(days = 90, minImpressions = 50, minPosition = 20, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position,
      ROUND(SUM(impressions) * 0.072, 0) AS estimated_clicks_at_pos5
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND is_anonymized_query = false
      AND search_type = 'WEB'
    GROUP BY query
    HAVING
      avg_position >= ${minPosition}
      AND impressions >= ${minImpressions}
    ORDER BY impressions DESC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
