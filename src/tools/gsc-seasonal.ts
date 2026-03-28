import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscSeasonal(
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    WITH monthly AS (
      SELECT
        EXTRACT(YEAR FROM data_date) AS year,
        EXTRACT(MONTH FROM data_date) AS month,
        FORMAT_DATE('%b', data_date) AS month_name,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE search_type = 'WEB'
      GROUP BY 1, 2, 3
    ),
    with_yoy AS (
      SELECT
        m.*,
        LAG(clicks) OVER (PARTITION BY month ORDER BY year) AS prev_year_clicks,
        ROUND(SAFE_DIVIDE(
          clicks - LAG(clicks) OVER (PARTITION BY month ORDER BY year),
          LAG(clicks) OVER (PARTITION BY month ORDER BY year)
        ) * 100, 1) AS yoy_change_pct
      FROM monthly m
    )
    SELECT *
    FROM with_yoy
    ORDER BY year DESC, month DESC
  `;

  return runQuery(sql, 100);
}
