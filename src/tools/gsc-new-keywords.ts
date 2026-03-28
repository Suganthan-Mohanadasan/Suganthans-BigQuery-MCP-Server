import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscNewKeywords(
  recentDays: number = 7,
  baselineDays: number = 60,
  minImpressions: number = 10,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    WITH recent_queries AS (
      SELECT
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
      GROUP BY query
      HAVING impressions >= ${minImpressions}
    ),
    baseline_queries AS (
      SELECT DISTINCT query
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date BETWEEN
          DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays + baselineDays} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${recentDays + 1} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
    )
    SELECT
      r.query,
      r.clicks,
      r.impressions,
      r.avg_position
    FROM recent_queries r
    LEFT JOIN baseline_queries b ON r.query = b.query
    WHERE b.query IS NULL
    ORDER BY r.impressions DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
