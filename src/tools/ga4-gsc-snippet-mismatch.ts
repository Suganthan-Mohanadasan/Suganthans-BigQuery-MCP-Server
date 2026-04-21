import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { ga4OrganicSessionsCTEs, normaliseGSCUrl } from "./ga4-shared.js";

export async function ga4GscSnippetMismatch(
  days: number = 28,
  minClicks: number = 20,
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
    ),

    joined AS (
      SELECT
        gsc.landing_page AS page,
        gsc.gsc_clicks AS clicks,
        gsc.gsc_impressions AS impressions,
        ROUND(gsc.gsc_ctr * 100, 2) AS ctr_pct,
        ROUND(gsc.avg_position, 1) AS avg_pos,
        ga4.sessions AS ga4_sessions,
        ROUND(SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) * 100, 1) AS engagement_rate_pct,
        ROUND(SAFE_DIVIDE(ga4.key_events, ga4.sessions) * 100, 2) AS conversion_rate_pct,
        CASE
          WHEN gsc.gsc_ctr > 0.08 AND SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) < 0.40
            THEN 'Misleading snippet'
          WHEN gsc.gsc_ctr < 0.03 AND SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) > 0.70
            THEN 'Underselling snippet'
          WHEN gsc.gsc_ctr > 0.08 AND SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) > 0.70
            THEN 'Strong match'
          WHEN gsc.gsc_ctr < 0.03 AND SAFE_DIVIDE(ga4.engaged_sessions, ga4.sessions) < 0.40
            THEN 'Both need work'
          ELSE 'Normal'
        END AS snippet_diagnosis
      FROM gsc
      LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
      WHERE gsc.gsc_clicks >= ${minClicks}
        AND ga4.sessions IS NOT NULL
        AND ga4.sessions > 0
    )

    SELECT *
    FROM joined
    WHERE snippet_diagnosis IN ('Misleading snippet', 'Underselling snippet', 'Both need work')
    ORDER BY impressions DESC
    LIMIT ${maxRows}
  `;

  return runQuery(sql, maxRows, targetProject);
}
