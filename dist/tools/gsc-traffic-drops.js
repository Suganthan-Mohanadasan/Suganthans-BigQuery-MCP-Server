"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscTrafficDrops = gscTrafficDrops;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscTrafficDrops(days = 28, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    const sql = `
    WITH current_period AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      GROUP BY url
    ),
    prior_period AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ${days * 2} DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY)
      GROUP BY url
    )
    SELECT
      c.url,
      p.clicks AS prev_clicks,
      c.clicks AS curr_clicks,
      c.clicks - p.clicks AS click_change,
      ROUND(SAFE_DIVIDE(c.clicks - p.clicks, p.clicks) * 100, 1) AS click_change_pct,
      p.avg_position AS prev_position,
      c.avg_position AS curr_position,
      ROUND(c.avg_position - p.avg_position, 1) AS position_change,
      p.ctr_pct AS prev_ctr,
      c.ctr_pct AS curr_ctr,
      p.impressions AS prev_impressions,
      c.impressions AS curr_impressions,
      CASE
        WHEN c.avg_position - p.avg_position > 3 THEN 'ranking_loss'
        WHEN p.ctr_pct - c.ctr_pct > 2 AND c.avg_position - p.avg_position <= 1 THEN 'ctr_collapse'
        WHEN p.impressions - c.impressions > p.impressions * 0.3 AND c.avg_position - p.avg_position <= 1 THEN 'demand_decline'
        ELSE 'mixed'
      END AS diagnosis
    FROM current_period c
    INNER JOIN prior_period p ON c.url = p.url
    WHERE c.clicks < p.clicks AND p.clicks >= 5
    ORDER BY (p.clicks - c.clicks) DESC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
