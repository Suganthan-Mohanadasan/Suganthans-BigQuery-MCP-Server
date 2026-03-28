import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

interface AlertResult {
  alerts: Record<string, unknown>[];
  disappeared: Record<string, unknown>[];
  summary: { total: number; critical: number; warning: number };
  bytesProcessed: string;
}

export async function gscAlerts(
  days: number = 7,
  positionDropThreshold: number = 20,
  ctrDropPct: number = 50,
  clickDropPct: number = 30,
  dataset?: string
): Promise<AlertResult> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  // Main alerts: position drops, CTR drops, click drops
  const alertsSQL = `
    WITH current_period AS (
      SELECT
        query,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND is_anonymized_query = false
      GROUP BY query, url
    ),
    prior_period AS (
      SELECT
        query,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ${days * 2} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY)
        AND search_type = 'WEB'
        AND is_anonymized_query = false
      GROUP BY query, url
    )
    SELECT
      c.query,
      c.url,
      p.clicks AS prev_clicks,
      c.clicks AS curr_clicks,
      p.avg_position AS prev_position,
      c.avg_position AS curr_position,
      ROUND(c.avg_position - p.avg_position, 1) AS position_change,
      p.ctr_pct AS prev_ctr,
      c.ctr_pct AS curr_ctr,
      CASE
        WHEN c.avg_position - p.avg_position > ${positionDropThreshold * 2} THEN 'critical'
        WHEN c.avg_position - p.avg_position > ${positionDropThreshold} THEN 'warning'
        WHEN p.ctr_pct > 0 AND (p.ctr_pct - c.ctr_pct) / p.ctr_pct * 100 > ${ctrDropPct * 2} THEN 'critical'
        WHEN p.ctr_pct > 0 AND (p.ctr_pct - c.ctr_pct) / p.ctr_pct * 100 > ${ctrDropPct} THEN 'warning'
        WHEN p.clicks >= 5 AND (p.clicks - c.clicks) / p.clicks * 100 > ${clickDropPct * 2} THEN 'critical'
        WHEN p.clicks >= 5 AND (p.clicks - c.clicks) / p.clicks * 100 > ${clickDropPct} THEN 'warning'
        ELSE NULL
      END AS severity,
      CASE
        WHEN c.avg_position - p.avg_position > ${positionDropThreshold} THEN 'position_drop'
        WHEN p.ctr_pct > 0 AND (p.ctr_pct - c.ctr_pct) / p.ctr_pct * 100 > ${ctrDropPct} THEN 'ctr_drop'
        WHEN p.clicks >= 5 AND (p.clicks - c.clicks) / p.clicks * 100 > ${clickDropPct} THEN 'click_drop'
        ELSE NULL
      END AS alert_type
    FROM current_period c
    INNER JOIN prior_period p ON c.query = p.query AND c.url = p.url
    WHERE
      c.avg_position - p.avg_position > ${positionDropThreshold}
      OR (p.ctr_pct > 0 AND (p.ctr_pct - c.ctr_pct) / p.ctr_pct * 100 > ${ctrDropPct})
      OR (p.clicks >= 5 AND (p.clicks - c.clicks) / p.clicks * 100 > ${clickDropPct})
    ORDER BY
      CASE WHEN severity = 'critical' THEN 0 ELSE 1 END,
      (p.clicks - c.clicks) DESC
    LIMIT 100
  `;

  // Disappeared: in prior period but not in current
  const disappearedSQL = `
    WITH current_period AS (
      SELECT DISTINCT query, url
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND is_anonymized_query = false
    ),
    prior_period AS (
      SELECT
        query,
        url,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)), 1) AS avg_position
      FROM \`${ds}.searchdata_site_impression\`
      WHERE
        data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ${days * 2} DAY)
          AND DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY)
        AND search_type = 'WEB'
        AND is_anonymized_query = false
      GROUP BY query, url
      HAVING clicks >= 5
    )
    SELECT
      p.query,
      p.url,
      p.clicks AS prior_clicks,
      p.impressions AS prior_impressions,
      p.avg_position AS prior_position,
      CASE WHEN p.clicks >= 20 THEN 'critical' ELSE 'warning' END AS severity
    FROM prior_period p
    LEFT JOIN current_period c ON p.query = c.query AND p.url = c.url
    WHERE c.query IS NULL
    ORDER BY p.clicks DESC
    LIMIT 50
  `;

  const [alertsResult, disappearedResult] = await Promise.all([
    runQuery(alertsSQL, 100),
    runQuery(disappearedSQL, 50),
  ]);

  const critical =
    alertsResult.rows.filter((r: Record<string, unknown>) => r.severity === "critical").length +
    disappearedResult.rows.filter((r: Record<string, unknown>) => r.severity === "critical").length;
  const warning =
    alertsResult.rows.filter((r: Record<string, unknown>) => r.severity === "warning").length +
    disappearedResult.rows.filter((r: Record<string, unknown>) => r.severity === "warning").length;

  return {
    alerts: alertsResult.rows,
    disappeared: disappearedResult.rows,
    summary: {
      total: alertsResult.totalRows + disappearedResult.totalRows,
      critical,
      warning,
    },
    bytesProcessed: alertsResult.bytesProcessed,
  };
}
