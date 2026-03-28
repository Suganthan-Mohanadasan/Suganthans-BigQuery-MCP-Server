"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscSeasonal = gscSeasonal;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscSeasonal(dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    WITH monthly AS (
      SELECT
        EXTRACT(YEAR FROM data_date) AS year,
        EXTRACT(MONTH FROM data_date) AS month,
        FORMAT_DATE('%b', data_date) AS month_name,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE search_type = 'WEB'
      GROUP BY 1, 2, 3
    ),
    with_yoy AS (
      SELECT
        m.*,
        LAG(clicks) OVER (PARTITION BY month ORDER BY year) AS prev_year_clicks,
        ROUND(SAFE_DIVIDE(
          clicks - LAG(clicks) OVER (PARTITION BY month ORDER BY year),
          LAG(clicks) OVER (PARTITION BY month ORDER BY year)
        ) * 100, 1) AS yoy_change_pct
      FROM monthly m
    )
    SELECT *
    FROM with_yoy
    ORDER BY year DESC, month DESC
  `;
    return (0, query_js_1.runQuery)(sql, 100);
}
