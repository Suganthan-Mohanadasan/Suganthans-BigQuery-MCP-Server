import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { measuredCurveCTEs, studyBenchmarkCaseSQL } from "./gsc-shared.js";

export type BenchmarkSource = "measured" | "study";

/**
 * CTR per page against a benchmark curve.
 *
 * The benchmark defaults to the property's own click curve, measured from the
 * export, with the published study table used only for ranks the property has
 * too little data to measure. Pass benchmark="study" to force the old
 * behaviour.
 *
 * Why the default changed: the study numbers are averages over unrelated sites.
 * On the property this was measured against, position 1 carried a 3.54% CTR
 * against the table's 28.5%. Judged against that, essentially every page came
 * back as "significantly below benchmark" - a verdict about the reference, not
 * about the page.
 *
 * Three caveats, all of them visible in the output rather than buried:
 *
 * 1. The curve is built at (url, query) level while the verdict is applied to a
 *    page's blended position across all its queries. A page with an unusual
 *    query mix sits against a rank that does not describe it exactly. Still far
 *    closer than a foreign study, but not a per-query judgement.
 * 2. A measured curve is not necessarily monotonic. On the reference property
 *    rank 2 came out above rank 1 (4.08% against 3.73%), because the query mix
 *    differs between ranks. The curve is left as measured rather than smoothed
 *    into the shape a curve is "supposed" to have - the whole point is to report
 *    this property, not an idealised one.
 * 3. If the dataset holds less history than curveDays, the curve quietly uses
 *    what exists, and a short window makes a thinner curve. Every row therefore
 *    carries benchmark_rank_impressions: how much data stood behind the rank it
 *    was judged against.
 */
export async function gscCtrBenchmark(
  days: number = 28,
  minImpressions: number = 200,
  benchmark: BenchmarkSource = "measured",
  curveDays: number = 90,
  minImpressionsPerRank: number = 100,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const studyCase = studyBenchmarkCaseSQL("page_metrics.avg_position");

  // benchmark="study" keeps the pre-existing behaviour and, more to the point,
  // skips the curve CTE entirely so the query still scans only `days`.
  const benchmarkCTE =
    benchmark === "measured"
      ? `${measuredCurveCTEs(ds, Math.max(days, curveDays), minImpressionsPerRank)},\n    `
      : "";

  const benchmarkSelect =
    benchmark === "measured"
      ? `ROUND(COALESCE(curve.measured_ctr_pct, ${studyCase}), 2) AS benchmark_ctr_pct,
        IF(curve.measured_ctr_pct IS NULL, 'study', 'measured') AS benchmark_source,
        curve.rank_impressions AS benchmark_rank_impressions`
      : `ROUND(${studyCase}, 2) AS benchmark_ctr_pct,
        'study' AS benchmark_source,
        CAST(NULL AS INT64) AS benchmark_rank_impressions`;

  const benchmarkJoin =
    benchmark === "measured"
      ? `LEFT JOIN curve
        ON CAST(ROUND(page_metrics.avg_position) AS INT64) = curve.rank`
      : "";

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
        data_date >= DATE_SUB((SELECT MAX(data_date) FROM \`${ds}.searchdata_url_impression\`), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY url
      HAVING impressions >= ${minImpressions} AND avg_position <= 20
    ),
    ${benchmarkCTE}with_benchmark AS (
      SELECT
        page_metrics.*,
        ${benchmarkSelect}
      FROM page_metrics
      ${benchmarkJoin}
    )
    SELECT
      url,
      clicks,
      impressions,
      actual_ctr_pct,
      avg_position,
      benchmark_ctr_pct,
      benchmark_source,
      benchmark_rank_impressions,
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
