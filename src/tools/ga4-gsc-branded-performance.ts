import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import { ga4OrganicSessionsCTEs, normaliseGSCUrl } from "./ga4-shared.js";

export async function ga4GscBrandedPerformance(
  brandTerms: string,
  days: number = 28,
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

  // Build regex from comma-separated brand terms (escape special chars)
  const terms = brandTerms
    .split(",")
    .map((t) => t.trim().toLowerCase())
    .filter((t) => t.length > 0)
    .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  if (terms.length === 0) {
    throw new Error("brand_terms is required. Provide comma-separated brand terms, e.g. 'suganthan,snippet digital'");
  }
  const brandRegex = terms.join("|");

  const sql = `
    WITH ${ga4OrganicSessionsCTEs(targetProject, ga4Ds, days)},

    gsc_branded AS (
      SELECT
        ${normaliseGSCUrl("url")} AS landing_page,
        CASE
          WHEN REGEXP_CONTAINS(LOWER(query), r'${brandRegex}') THEN 'Branded'
          ELSE 'Non-branded'
        END AS traffic_type,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
        SUM(sum_position) / SUM(impressions) + 1 AS avg_position
      FROM \`${targetProject}.${gscDs}.searchdata_url_impression\`
      WHERE data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
        AND search_type = 'WEB'
        AND NOT is_anonymized_query
      GROUP BY landing_page, traffic_type
    )

    SELECT
      gsc.traffic_type,
      SUM(gsc.clicks) AS clicks,
      SUM(gsc.impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(gsc.clicks), SUM(gsc.impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(gsc.clicks * gsc.avg_position), SUM(gsc.clicks)), 1) AS weighted_avg_pos,
      SUM(COALESCE(ga4.sessions, 0)) AS ga4_sessions,
      ROUND(SAFE_DIVIDE(
        SUM(COALESCE(ga4.engaged_sessions, 0)),
        SUM(COALESCE(ga4.sessions, 0))
      ) * 100, 1) AS engagement_rate_pct,
      SUM(COALESCE(ga4.key_events, 0)) AS conversions,
      ROUND(SAFE_DIVIDE(
        SUM(COALESCE(ga4.key_events, 0)),
        SUM(COALESCE(ga4.sessions, 0))
      ) * 100, 2) AS conversion_rate_pct,
      ROUND(SUM(COALESCE(ga4.revenue, 0)), 2) AS revenue
    FROM gsc_branded gsc
    LEFT JOIN ga4_organic ga4 ON gsc.landing_page = ga4.landing_page
    GROUP BY gsc.traffic_type
    ORDER BY gsc.traffic_type
  `;

  return runQuery(sql, 10, targetProject);
}
