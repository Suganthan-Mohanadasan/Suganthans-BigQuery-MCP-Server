"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscSiteSnapshot = gscSiteSnapshot;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscSiteSnapshot(days = 28, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    WITH current_period AS (
      SELECT
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position,
        COUNT(DISTINCT url) AS unique_pages,
        COUNT(DISTINCT query) AS unique_queries
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
    ),
    prior_period AS (
      SELECT
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position,
        COUNT(DISTINCT url) AS unique_pages,
        COUNT(DISTINCT query) AS unique_queries
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ${days * 2} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY)
        AND search_type = 'WEB'
    )
    SELECT
      c.clicks AS current_clicks,
      p.clicks AS prior_clicks,
      c.clicks - p.clicks AS click_change,
      ROUND(SAFE_DIVIDE(c.clicks - p.clicks, p.clicks) * 100, 1) AS click_change_pct,
      c.impressions AS current_impressions,
      p.impressions AS prior_impressions,
      c.impressions - p.impressions AS impression_change,
      ROUND(SAFE_DIVIDE(c.impressions - p.impressions, p.impressions) * 100, 1) AS impression_change_pct,
      c.ctr_pct AS current_ctr,
      p.ctr_pct AS prior_ctr,
      ROUND(c.ctr_pct - p.ctr_pct, 2) AS ctr_change,
      c.avg_position AS current_position,
      p.avg_position AS prior_position,
      ROUND(c.avg_position - p.avg_position, 1) AS position_change,
      c.unique_pages AS current_pages,
      c.unique_queries AS current_queries
    FROM current_period c
    CROSS JOIN prior_period p
  `;
    return (0, query_js_1.runQuery)(sql, 1);
}
