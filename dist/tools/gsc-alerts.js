"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscAlerts = gscAlerts;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscAlerts(days = 7, positionDropThreshold = 20, ctrDropPct = 50, clickDropPct = 30, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
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
      FROM \`${ds}.searchdata_url_impression\`
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
      FROM \`${ds}.searchdata_url_impression\`
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
      FROM \`${ds}.searchdata_url_impression\`
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
      FROM \`${ds}.searchdata_url_impression\`
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
        (0, query_js_1.runQuery)(alertsSQL, 100),
        (0, query_js_1.runQuery)(disappearedSQL, 50),
    ]);
    const critical = alertsResult.rows.filter((r) => r.severity === "critical").length +
        disappearedResult.rows.filter((r) => r.severity === "critical").length;
    const warning = alertsResult.rows.filter((r) => r.severity === "warning").length +
        disappearedResult.rows.filter((r) => r.severity === "warning").length;
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
