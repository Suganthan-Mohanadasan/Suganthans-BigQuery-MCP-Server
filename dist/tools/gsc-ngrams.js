"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscNgrams = gscNgrams;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscNgrams(days = 28, minQueryCount = 5, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    WITH query_data AS (
      SELECT
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
      GROUP BY query
    ),
    words AS (
      SELECT
        word,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        COUNT(DISTINCT query) AS query_count
      FROM query_data,
        UNNEST(SPLIT(LOWER(query), ' ')) AS word
      WHERE LENGTH(word) > 3
      GROUP BY word
    )
    SELECT
      word AS term,
      query_count AS queries_containing,
      total_clicks,
      total_impressions,
      ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS avg_ctr_pct
    FROM words
    WHERE query_count >= ${minQueryCount}
    ORDER BY total_clicks DESC
    LIMIT 100
  `;
    return (0, query_js_1.runQuery)(sql, 100);
}
