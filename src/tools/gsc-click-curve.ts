import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import {
  AVG_POSITION_SQL,
  normaliseSearchType,
  escapeSQLString,
  getLastExportDate,
} from "./gsc-shared.js";

type QueryResult = { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };

export type CurveSegment = "none" | "device" | "country" | "search_type" | "branded";

/**
 * Your own click curve: CTR by ranking position, measured on your data.
 *
 * Every "expected CTR by position" table in an SEO tool is somebody else's
 * average - usually a study across unrelated sites. This builds the curve from
 * the export: aggregate to (url, query), take that pair's average position,
 * round it to a rank, then sum clicks and impressions per rank. CTR is the
 * ratio of those sums, never an average of ratios, so high-volume pairs carry
 * the weight they should.
 *
 * With segment_by="branded" and a brand_pattern, the branded and non-branded
 * curves come back separately - the split that matters most, because branded
 * queries inflate a blended curve at the top and make everything else look
 * like it underperforms.
 */
export async function gscClickCurve(
  days: number = 90,
  maxPosition: number = 20,
  minImpressionsPerRank: number = 100,
  segmentBy: CurveSegment = "none",
  brandPattern?: string,
  searchType: string = "WEB",
  urlContains?: string,
  dataset?: string
): Promise<{
  period: { startDate: string; endDate: string; days: number; anchoredTo: string };
  method: string;
  curve: QueryResult;
  coverage: QueryResult;
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");
  const type = normaliseSearchType(searchType);

  if (type === "DISCOVER") {
    throw new Error(
      "Discover has no ranking positions, so there is no click curve to build. Use gsc_discover instead."
    );
  }
  if (segmentBy === "branded" && !brandPattern) {
    throw new Error(
      'segment_by="branded" needs brand_pattern, e.g. "homeandsmart" or "brand|brandname".'
    );
  }

  const lastDay = await getLastExportDate(ds, runQuery);
  const table = `\`${ds}.searchdata_url_impression\``;

  const window = `data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY)
      AND data_date <= DATE '${lastDay}'`;
  const urlScope = urlContains ? `AND url LIKE '%${escapeSQLString(urlContains)}%'` : "";

  let segmentExpr = "'all'";
  if (segmentBy === "device") segmentExpr = "device";
  if (segmentBy === "country") segmentExpr = "country";
  if (segmentBy === "search_type") segmentExpr = "search_type";
  if (segmentBy === "branded") {
    segmentExpr = `IF(REGEXP_CONTAINS(query, r'(?i)${escapeSQLString(brandPattern!)}'), 'branded', 'non-branded')`;
  }

  // search_type stays unfiltered when it is the segment, otherwise it is pinned.
  const typeScope = segmentBy === "search_type" ? "" : `AND search_type = '${type}'`;

  const curveSQL = `
    WITH per_pair AS (
      SELECT
        ${segmentExpr} AS segment,
        url,
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ${AVG_POSITION_SQL} AS position
      FROM ${table}
      WHERE ${window}
        AND query IS NOT NULL
        ${typeScope}
        ${urlScope}
      GROUP BY segment, url, query
    ),
    ranked AS (
      SELECT
        segment,
        CAST(ROUND(position) AS INT64) AS rank,
        clicks,
        impressions
      FROM per_pair
    )
    SELECT
      segment,
      rank AS position,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 3) AS ctr_pct,
      COUNT(*) AS url_query_pairs
    FROM ranked
    WHERE rank BETWEEN 1 AND ${maxPosition}
    GROUP BY segment, rank
    HAVING impressions >= ${minImpressionsPerRank}
    ORDER BY segment, position
    LIMIT 2000
  `;

  // What the curve does NOT cover, stated rather than left implicit: anonymized
  // rows carry no query and no position, so they can never enter it.
  const coverageSQL = `
    SELECT
      SUM(clicks) AS total_clicks,
      SUM(IF(is_anonymized_query, clicks, 0)) AS anonymized_clicks,
      ROUND(SAFE_DIVIDE(SUM(IF(is_anonymized_query, clicks, 0)), SUM(clicks)) * 100, 2)
        AS anonymized_clicks_share_pct,
      COUNT(DISTINCT query) AS distinct_queries
    FROM ${table}
    WHERE ${window}
      ${typeScope}
      ${urlScope}
    LIMIT 1
  `;

  const [curve, coverage] = await Promise.all([runQuery(curveSQL, 2000), runQuery(coverageSQL, 1)]);

  const start = new Date(lastDay);
  start.setUTCDate(start.getUTCDate() - days + 1);

  return {
    period: {
      startDate: start.toISOString().split("T")[0],
      endDate: lastDay,
      days,
      anchoredTo: `latest day in the export (${lastDay}), not today`,
    },
    method:
      "CTR per rank = SUM(clicks) / SUM(impressions) over (url, query) pairs whose average position rounds to that rank. Average position uses sum_position/impressions + 1, because sum_position is zero-based. Ranks below min_impressions_per_rank are dropped rather than reported as noise.",
    curve,
    coverage,
  };
}
