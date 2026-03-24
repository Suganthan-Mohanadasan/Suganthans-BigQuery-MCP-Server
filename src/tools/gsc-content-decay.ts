import { runQuery } from "./query.js";
import { getConfig } from "../client.js";

export async function gscContentDecay(
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";

  const sql = `
    WITH monthly AS (
      SELECT
        url,
        DATE_TRUNC(data_date, MONTH) AS month,
        SUM(clicks) AS clicks
      FROM \`${ds}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
      GROUP BY url, month
    ),
    ranked AS (
      SELECT
        url,
        month,
        clicks,
        LAG(clicks, 1) OVER (PARTITION BY url ORDER BY month) AS prev_clicks,
        LAG(clicks, 2) OVER (PARTITION BY url ORDER BY month) AS prev2_clicks
      FROM monthly
    )
    SELECT
      url,
      prev2_clicks AS clicks_3_months_ago,
      prev_clicks AS clicks_2_months_ago,
      clicks AS clicks_last_month,
      ROUND(SAFE_DIVIDE(clicks - prev2_clicks, prev2_clicks) * 100, 1) AS total_decline_pct
    FROM ranked
    WHERE
      prev2_clicks IS NOT NULL
      AND prev_clicks IS NOT NULL
      AND clicks < prev_clicks
      AND prev_clicks < prev2_clicks
      AND prev2_clicks >= 10
    ORDER BY (prev2_clicks - clicks) DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
