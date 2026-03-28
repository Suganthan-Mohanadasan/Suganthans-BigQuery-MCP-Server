"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscIntentBreakdown = gscIntentBreakdown;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscIntentBreakdown(days = 28, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    SELECT
      CASE
        WHEN REGEXP_CONTAINS(query, r'(?i)\\b(how|what|why|when|where|who|guide|tutorial|learn|explain|meaning|definition|example)\\b') THEN 'informational'
        WHEN REGEXP_CONTAINS(query, r'(?i)\\b(buy|price|cheap|deal|discount|order|shop|coupon|purchase|pricing|cost|free trial)\\b') THEN 'transactional'
        WHEN REGEXP_CONTAINS(query, r'(?i)\\b(best|top|review|comparison|vs|versus|alternative|compared)\\b') THEN 'commercial'
        WHEN REGEXP_CONTAINS(query, r'(?i)\\b(login|sign in|dashboard|account|support|contact|address|phone|hours)\\b') THEN 'navigational'
        ELSE 'unclassified'
      END AS intent,
      COUNT(DISTINCT query) AS unique_queries,
      SUM(clicks) AS total_clicks,
      SUM(impressions) AS total_impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS avg_ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND is_anonymized_query = false
      AND search_type = 'WEB'
    GROUP BY 1
    ORDER BY total_clicks DESC
  `;
    return (0, query_js_1.runQuery)(sql, 10);
}
