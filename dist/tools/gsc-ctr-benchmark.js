"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscCtrBenchmark = gscCtrBenchmark;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscCtrBenchmark(days = 28, minImpressions = 200, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const sql = `
    WITH page_metrics AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS actual_ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY url
      HAVING impressions >= ${minImpressions} AND avg_position <= 20
    ),
    with_benchmark AS (
      SELECT *,
        CASE
          WHEN avg_position <= 1 THEN 28.5
          WHEN avg_position <= 2 THEN 15.7
          WHEN avg_position <= 3 THEN 11.0
          WHEN avg_position <= 4 THEN 8.0
          WHEN avg_position <= 5 THEN 7.2
          WHEN avg_position <= 6 THEN 5.1
          WHEN avg_position <= 7 THEN 4.0
          WHEN avg_position <= 8 THEN 3.2
          WHEN avg_position <= 9 THEN 2.8
          WHEN avg_position <= 10 THEN 2.5
          ELSE GREATEST(0.5, 2.5 - (avg_position - 10) * 0.2)
        END AS benchmark_ctr_pct
      FROM page_metrics
    )
    SELECT
      url,
      clicks,
      impressions,
      actual_ctr_pct,
      avg_position,
      benchmark_ctr_pct,
      ROUND(actual_ctr_pct - benchmark_ctr_pct, 2) AS gap_pct,
      CASE
        WHEN actual_ctr_pct - benchmark_ctr_pct >= 2.0 THEN 'Above benchmark'
        WHEN actual_ctr_pct - benchmark_ctr_pct >= -2.0 THEN 'At benchmark'
        WHEN actual_ctr_pct - benchmark_ctr_pct >= -5.0 THEN 'Below benchmark'
        ELSE 'Significantly below benchmark'
      END AS verdict
    FROM with_benchmark
    ORDER BY gap_pct ASC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
