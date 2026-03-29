import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscDeviceSplit(
  days: number = 28,
  minClicks: number = 5,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  // Find queries where mobile and desktop rank different pages
  const sql = `
    WITH device_pages AS (
      SELECT
        query,
        device,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position,
        ROW_NUMBER() OVER (
          PARTITION BY query, device
          ORDER BY SUM(clicks) DESC
        ) AS rn
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND is_anonymized_query = false
        AND search_type = 'WEB'
        AND device IN ('MOBILE', 'DESKTOP')
      GROUP BY query, device, url
    )
    SELECT
      m.query,
      m.url AS mobile_url,
      d.url AS desktop_url,
      m.clicks AS mobile_clicks,
      d.clicks AS desktop_clicks,
      m.avg_position AS mobile_position,
      d.avg_position AS desktop_position,
      m.impressions AS mobile_impressions,
      d.impressions AS desktop_impressions
    FROM device_pages m
    JOIN device_pages d ON m.query = d.query
    WHERE
      m.device = 'MOBILE' AND d.device = 'DESKTOP'
      AND m.rn = 1 AND d.rn = 1
      AND m.url != d.url
      AND (m.clicks >= ${minClicks} OR d.clicks >= ${minClicks})
    ORDER BY (m.clicks + d.clicks) DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
