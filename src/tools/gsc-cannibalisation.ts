import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscCannibalisation(
  days: number = 28,
  minImpressions: number = 50,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    WITH query_urls AS (
      SELECT
        query,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
      GROUP BY query, url
    ),
    multi_url AS (
      SELECT query
      FROM query_urls
      GROUP BY query
      HAVING COUNT(DISTINCT url) >= 2 AND SUM(impressions) >= ${minImpressions}
    )
    SELECT
      qu.query,
      qu.url,
      qu.clicks,
      qu.impressions,
      qu.avg_position
    FROM query_urls qu
    INNER JOIN multi_url mu ON qu.query = mu.query
    ORDER BY qu.query, qu.avg_position ASC
    LIMIT 200
  `;

  return runQuery(sql, 200);
}
