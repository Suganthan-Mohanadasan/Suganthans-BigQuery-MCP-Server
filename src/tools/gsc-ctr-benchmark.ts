import { runQuery } from "./query.js";
import {
  lastExportDay,
  measuredCurveCTEs,
  studyBenchmarkCaseSQL,
} from "./gsc-shared.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscCtrBenchmark(
  days: number = 28,
  minImpressions: number = 200,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

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
      GROUP BY url
      HAVING impressions >= ${minImpressions} AND avg_position <= 20
    ),
${measuredCurveCTEs(ds, Math.max(days, 90), 100)},
    with_benchmark AS (
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

  return runQuery(sql, 50);
}
