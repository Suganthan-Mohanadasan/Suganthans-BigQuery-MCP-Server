import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { ga4OrganicSessionsCTEs, normaliseGSCUrl } from "./ga4-shared.js";

export async function ga4GscPagePerformance(
  days: number = 28,
  minClicks: number = 10,
  maxRows: number = 50,
  gscDataset?: string,
  ga4Dataset?: string,
  projectId?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  const config = getConfig();
  const gscDs = gscDataset || config.defaultDataset || "searchconsole";
  const ga4Ds = ga4Dataset || process.env.BIGQUERY_GA4_DATASET;
  const targetProject = projectId || config.ga4ProjectId || config.projectId;
  validateIdentifier(gscDs, "gsc_dataset");
  if (!ga4Ds) {
    throw new Error(
      "GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter. " +
      "The dataset name looks like 'analytics_123456789'."
    );
  }
  validateIdentifier(ga4Ds, "ga4_dataset");
  if (projectId) validateIdentifier(projectId, "project_id");

  const sql = `
    WITH ${ga4OrganicSessionsCTEs(targetProject, ga4Ds, days)},

    gsc AS (
      SELECT
        ${normaliseGSCUrl("url")} AS landing_page,
        SUM(clicks) AS gsc_clicks,
        SUM(impressions) AS gsc_impressions,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS gsc_ctr,
        SUM(sum_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${targetProject}.${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
      GROUP BY landing_page
    )

    SELECT
      gsc.landing_page AS page,
      gsc.gsc_clicks AS clicks,
      gsc.gsc_impressions AS impressions,
      ROUND(gsc.gsc_ctr * 100, 2) AS ctr_pct,
      ROUND(gsc.avg_position, 1) AS avg_pos,
      COALESCE(ga4.sessions, 0) AS ga4_sessions,
      COALESCE(ga4.users, 0) AS ga4_users,
      COALESCE(ga4.engaged_sessions, 0) AS engaged_sessions,
      ROUND(SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) * 100, 1) AS engagement_rate_pct,
      COALESCE(ga4.key_events, 0) AS conversions,
      ROUND(SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100, 2) AS conversion_rate_pct,
      ROUND(COALESCE(ga4.revenue, 0), 2) AS revenue
    FROM gsc
    LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
    WHERE gsc.gsc_clicks >= ${minClicks}
    ORDER BY gsc.gsc_clicks DESC
    LIMIT ${maxRows}
  `;

  return runQuery(sql, maxRows, targetProject);
}
