import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { deviceCountryConditions } from "./gsc-shared.js";

export async function gscQuickWins(
  days: number = 28,
  minImpressions: number = 100,
  maxPosition: number = 15,
  device?: string,
  country?: string,
  dataset?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const extra = deviceCountryConditions(device, country, "WEB");
  const scopeSQL = extra.length ? `AND ${extra.join("\n      AND ")}` : "";

  const sql = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1, 1) AS avg_position,
      ROUND(SUM(impressions) * (0.11 - SAFE_DIVIDE(SUM(clicks), SUM(impressions))), 0) AS opportunity
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      AND is_anonymized_query = false
      AND search_type = 'WEB'
      ${scopeSQL}
    GROUP BY query
    HAVING
      avg_position BETWEEN 4 AND ${maxPosition}
      AND impressions >= ${minImpressions}
    ORDER BY opportunity DESC
    LIMIT 50
  `;

  return runQuery(sql, 50);
}
