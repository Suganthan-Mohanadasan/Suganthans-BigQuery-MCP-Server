import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscTrafficDrops(
  days: number = 28,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    WITH current_period AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY url
    ),
    prior_period AS (
      SELECT
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ${days * 2} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY)
        AND search_type = 'WEB'
      GROUP BY url
    )
    SELECT
      c.url,
      p.clicks AS prev_clicks,
      c.clicks AS curr_clicks,
      c.clicks - p.clicks AS click_change,
      ROUND(SAFE_DIVIDE(c.clicks - p.clicks, p.clicks) * 100, 1) AS click_change_pct,
      p.avg_position AS prev_position,
      c.avg_position AS curr_position,
      ROUND(c.avg_position - p.avg_position, 1) AS position_change,
      p.ctr_pct AS prev_ctr,
      c.ctr_pct AS curr_ctr,
      p.impressions AS prev_impressions,
      c.impressions AS curr_impressions,
      CASE
        WHEN c.avg_position - p.avg_position > 3 THEN 'ranking_loss'
        WHEN p.ctr_pct - c.ctr_pct > 2 AND c.avg_position - p.avg_position <= 1 THEN 'ctr_collapse'
        WHEN p.impressions - c.impressions > p.impressions * 0.3 AND c.avg_position - p.avg_position <= 1 THEN 'demand_decline'
        ELSE 'mixed'
      END AS diagnosis
    FROM current_period c
    INNER JOIN prior_period p ON c.url = p.url
    WHERE c.clicks < p.clicks AND p.clicks >= 5
    ORDER BY (p.clicks - c.clicks) DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
