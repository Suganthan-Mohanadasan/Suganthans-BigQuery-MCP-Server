"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscCannibalisation = gscCannibalisation;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscCannibalisation(days = 28, minImpressions = 50, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    const sql = `
    WITH query_urls AS (
      SELECT
        query,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
      GROUP BY query, url
    ),
    multi_url AS (
      SELECT query
      FROM query_urls
      GROUP BY query
      HAVING COUNT(DISTINCT url) >= 2 AND SUM(impressions) >= ${minImpressions}
    )
    SELECT
      qu.query,
      qu.url,
      qu.clicks,
      qu.impressions,
      qu.avg_position
    FROM query_urls qu
    INNER JOIN multi_url mu ON qu.query = mu.query
    ORDER BY qu.query, qu.avg_position ASC
    LIMIT 200
  `;
    return (0, query_js_1.runQuery)(sql, 200);
}
