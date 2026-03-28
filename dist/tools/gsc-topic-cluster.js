"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscTopicCluster = gscTopicCluster;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscTopicCluster(urlPattern, days = 28, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    // Escape single quotes in the pattern for SQL safety
    const safePattern = urlPattern.replace(/'/g, "\\'");
    const summarySQL = `
    SELECT
      '${safePattern}' AS url_pattern,
      COUNT(DISTINCT url) AS page_count,
      SUM(clicks) AS total_clicks,
      SUM(impressions) AS total_impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS avg_ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
    FROM \`${ds}.searchdata_url_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND url LIKE '%${safePattern}%'
      AND search_type = 'WEB'
  `;
    const topPagesSQL = `
    SELECT
      url,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
    FROM \`${ds}.searchdata_url_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND url LIKE '%${safePattern}%'
      AND search_type = 'WEB'
    GROUP BY url
    ORDER BY clicks DESC
    LIMIT 10
  `;
    const topQueriesSQL = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND url LIKE '%${safePattern}%'
      AND is_anonymized_query = false
      AND search_type = 'WEB'
    GROUP BY query
    ORDER BY clicks DESC
    LIMIT 10
  `;
    const [summary, topPages, topQueries] = await Promise.all([
        (0, query_js_1.runQuery)(summarySQL, 1),
        (0, query_js_1.runQuery)(topPagesSQL, 10),
        (0, query_js_1.runQuery)(topQueriesSQL, 10),
    ]);
    return { summary, topPages, topQueries };
}
