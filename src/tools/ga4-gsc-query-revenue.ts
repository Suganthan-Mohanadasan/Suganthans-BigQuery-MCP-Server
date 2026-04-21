import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { ga4OrganicSessionsCTEs, normaliseGSCUrl } from "./ga4-shared.js";

export async function ga4GscQueryRevenue(
  days: number = 28,
  minClicks: number = 5,
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
      "GA4 dataset not configured. Set BIGQUERY_GA4_DATASET environment variable or pass ga4_dataset parameter."
    );
  }
  validateIdentifier(ga4Ds, "ga4_dataset");
  if (projectId) validateIdentifier(projectId, "project_id");

  const sql = `
    WITH ${ga4OrganicSessionsCTEs(targetProject, ga4Ds, days)},

    gsc_queries AS (
      SELECT
        query,
        ${normaliseGSCUrl("url")} AS landing_page,
        SUM(clicks) AS query_clicks,
        SUM(impressions) AS impressions,
        SUM(sum_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${targetProject}.${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND NOT is_anonymized_query
        AND clicks > 0
      GROUP BY query, landing_page
    ),

    gsc_with_share AS (
      SELECT *,
        SUM(query_clicks) OVER (PARTITION BY landing_page) AS total_page_clicks,
        SAFE_DIVIDE(query_clicks, SUM(query_clicks) OVER (PARTITION BY landing_page)) AS click_share
      FROM gsc_queries
    )

    SELECT
      gsc.query,
      gsc.landing_page AS page,
      gsc.query_clicks AS clicks,
      gsc.impressions,
      ROUND(gsc.avg_position, 1) AS avg_pos,
      ROUND(gsc.click_share * 100, 1) AS click_share_pct,
      CAST(ROUND(COALESCE(ga4.sessions, 0) * gsc.click_share) AS INT64) AS attributed_sessions,
      CAST(ROUND(COALESCE(ga4.key_events, 0) * gsc.click_share) AS INT64) AS attributed_conversions,
      ROUND(COALESCE(ga4.revenue, 0) * gsc.click_share, 2) AS attributed_revenue
    FROM gsc_with_share gsc
    LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
    WHERE gsc.query_clicks >= ${minClicks}
    ORDER BY attributed_revenue DESC, attributed_conversions DESC
    LIMIT ${maxRows}
  `;

  return runQuery(sql, maxRows, targetProject);
}
