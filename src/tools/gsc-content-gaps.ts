import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscContentGaps(
  days: number = 90,
  minImpressions: number = 50,
  minPosition: number = 20,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)), 1) AS avg_position,
      ROUND(SUM(impressions) * 0.072, 0) AS estimated_clicks_at_pos5
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND is_anonymized_query = false
      AND search_type = 'WEB'
    GROUP BY query
    HAVING
      avg_position >= ${minPosition}
      AND impressions >= ${minImpressions}
    ORDER BY impressions DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
