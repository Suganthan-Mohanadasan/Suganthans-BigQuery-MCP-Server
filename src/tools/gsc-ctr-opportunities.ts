import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import {
  deviceCountryConditions,
  lastExportDay,
  measuredCurveCTEs,
  studyBenchmarkCaseSQL,
} from "./gsc-shared.js";

export async function gscCtrOpportunities(
  days: number = 28,
  minImpressions: number = 500,
  device?: string,
  country?: string,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const extra = deviceCountryConditions(device, country, "WEB");
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
        data_date >= DATE_SUB(${lastExportDay(ds, "searchdata_url_impression")}, INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        ${scopeSQL}
      GROUP BY url
      HAVING impressions >= ${minImpressions} AND avg_position <= 20
    ),
${measuredCurveCTEs(ds, Math.max(days, 90), 100, scopeSQL)},
    benchmarks AS (
      SELECT
        page_metrics.*,
        ROUND(COALESCE(curve.measured_ctr_pct, ${studyBenchmarkCaseSQL("page_metrics.avg_position")}), 2)
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

  return runQuery(sql, 50);
}
