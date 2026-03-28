import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscAnonymousTraffic(
  days: number = 28,
  dataset?: string
): Promise<{
  summary: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };
  topPages: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const summarySQL = `
    SELECT
      CASE WHEN is_anonymized_query THEN 'anonymous' ELSE 'known' END AS query_type,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      COUNT(DISTINCT url) AS unique_urls
    FROM \`${ds}.searchdata_url_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND search_type = 'WEB'
    GROUP BY 1
    ORDER BY clicks DESC
  `;

  const topPagesSQL = `
    SELECT
      url,
      SUM(IF(is_anonymized_query, clicks, 0)) AS anonymous_clicks,
      SUM(IF(NOT is_anonymized_query, clicks, 0)) AS known_clicks,
      SUM(clicks) AS total_clicks,
      ROUND(SAFE_DIVIDE(
        SUM(IF(is_anonymized_query, clicks, 0)),
        SUM(clicks)
      ) * 100, 1) AS anonymous_share_pct
    FROM \`${ds}.searchdata_url_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND search_type = 'WEB'
    GROUP BY url
    HAVING total_clicks > 10
    ORDER BY anonymous_clicks DESC
    LIMIT 50
  `;

  const [summary, topPages] = await Promise.all([
    runQuery(summarySQL, 10),
    runQuery(topPagesSQL, 50),
  ]);

  return { summary, topPages };
}
