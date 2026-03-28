import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscCtrOpportunities(
  days: number = 28,
  minImpressions: number = 500,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  // CTR benchmarks by position (positions 1-10), extrapolates beyond 10
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
    benchmarks AS (
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
      ROUND(benchmark_ctr_pct - actual_ctr_pct, 2) AS ctr_gap_pct,
      ROUND(impressions * (benchmark_ctr_pct - actual_ctr_pct) / 100, 0) AS potential_extra_clicks
    FROM benchmarks
    WHERE benchmark_ctr_pct - actual_ctr_pct > 1.0
    ORDER BY potential_extra_clicks DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
