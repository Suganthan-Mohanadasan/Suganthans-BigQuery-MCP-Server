"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscCtrOpportunities = gscCtrOpportunities;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const gsc_shared_js_1 = require("./gsc-shared.js");
async function gscCtrOpportunities(days = 28, minImpressions = 500, device, country, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const extra = (0, gsc_shared_js_1.deviceCountryConditions)(device, country, "WEB");
    const scopeSQL = extra.length ? `AND ${extra.join("\n        AND ")}` : "";
    // CTR benchmarks by position (positions 1-10), extrapolates beyond 10
    const sql = `
    WITH page_metrics AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS actual_ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1, 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(${(0, gsc_shared_js_1.lastExportDay)(ds, "searchdata_url_impression")}, INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        ${scopeSQL}
      GROUP BY url
      HAVING impressions >= ${minImpressions} AND avg_position <= 20
    ),
${(0, gsc_shared_js_1.measuredCurveCTEs)(ds, Math.max(days, 90), 100, scopeSQL)},
    benchmarks AS (
      SELECT
        page_metrics.*,
        ROUND(COALESCE(curve.measured_ctr_pct, ${(0, gsc_shared_js_1.studyBenchmarkCaseSQL)("page_metrics.avg_position")}), 2)
          AS benchmark_ctr_pct,
        IF(curve.measured_ctr_pct IS NULL, 'study', 'measured') AS benchmark_source
      FROM page_metrics
      LEFT JOIN curve
        ON CAST(ROUND(page_metrics.avg_position) AS INT64) = curve.rank
    )
    SELECT
      url,
      clicks,
      impressions,
      actual_ctr_pct,
      avg_position,
      benchmark_ctr_pct,
      benchmark_source,
      ROUND(benchmark_ctr_pct - actual_ctr_pct, 2) AS ctr_gap_pct,
      ROUND(impressions * (benchmark_ctr_pct - actual_ctr_pct) / 100, 0) AS potential_extra_clicks
    FROM benchmarks
    WHERE benchmark_ctr_pct - actual_ctr_pct > 1.0
    ORDER BY potential_extra_clicks DESC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
