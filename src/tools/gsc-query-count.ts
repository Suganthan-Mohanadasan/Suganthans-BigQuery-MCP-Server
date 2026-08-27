import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";
import {
  AVG_POSITION_SQL,
  positionGroupSQL,
  GRANULARITY_TRUNC,
  Granularity,
  normaliseSearchType,
  escapeSQLString,
  getLastExportDate,
  deviceCountryConditions,
} from "./gsc-shared.js";

type QueryResult = { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };

/**
 * Query counting on the warehouse instead of the API.
 *
 * Two things this can do that the API version cannot: it counts across the full
 * export instead of a 1,000-row page, and it reads the anonymized share from
 * the `is_anonymized_query` flag rather than inferring it from a click gap.
 */
export async function gscQueryCount(
  days: number = 28,
  url?: string,
  urlContains?: string,
  granularity: Granularity = "none",
  minPosition?: number,
  maxPosition?: number,
  searchType: string = "WEB",
  topPages?: number,
  device?: string,
  country?: string,
  dataset?: string
): Promise<{
  period: { startDate: string; endDate: string; days: number; anchoredTo: string };
  totals: QueryResult;
  byPositionGroup: QueryResult;
  periodComparison: QueryResult;
  timeSeries: QueryResult | null;
  topPagesByQueryCount: QueryResult | null;
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");
  const type = normaliseSearchType(searchType);

  if (type === "DISCOVER") {
    throw new Error(
      "Discover carries no query dimension, so there is nothing to count. Use gsc_discover instead."
    );
  }

  const lastDay = await getLastExportDate(ds, runQuery);
  const table = `\`${ds}.searchdata_url_impression\``;

  const scope: string[] = [`search_type = '${type}'`];
  if (url) scope.push(`url = '${escapeSQLString(url)}'`);
  if (urlContains) scope.push(`url LIKE '%${escapeSQLString(urlContains)}%'`);
  scope.push(...deviceCountryConditions(device, country, type));
  const scopeSQL = scope.length ? `AND ${scope.join("\n      AND ")}` : "";

  const currentWindow = `data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY)
      AND data_date <= DATE '${lastDay}'`;

  const positionFilter: string[] = [];
  if (minPosition !== undefined) positionFilter.push(`position >= ${minPosition}`);
  if (maxPosition !== undefined) positionFilter.push(`position <= ${maxPosition}`);
  const positionFilterSQL = positionFilter.length ? `WHERE ${positionFilter.join(" AND ")}` : "";

  // The anonymized gap is deliberately NOT position-filtered: anonymized rows
  // carry no query and therefore no position, so filtering would just hide them.
  const totalsSQL = `
    SELECT
      COUNT(DISTINCT query) AS visible_queries,
      SUM(IF(is_anonymized_query, 0, clicks)) AS clicks_from_visible_queries,
      SUM(clicks) AS total_clicks,
      SUM(IF(is_anonymized_query, clicks, 0)) AS anonymized_clicks,
      ROUND(SAFE_DIVIDE(SUM(IF(is_anonymized_query, clicks, 0)), SUM(clicks)) * 100, 2)
        AS anonymized_clicks_share_pct,
      SUM(impressions) AS total_impressions
    FROM ${table}
    WHERE ${currentWindow}
      ${scopeSQL}
    LIMIT 1
  `;

  const byPositionGroupSQL = `
    WITH per_query AS (
      SELECT
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ${AVG_POSITION_SQL} AS position
      FROM ${table}
      WHERE ${currentWindow}
        AND query IS NOT NULL
        ${scopeSQL}
      GROUP BY query
    )
    SELECT
      ${positionGroupSQL("position")} AS position_group,
      COUNT(*) AS queries,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct
    FROM per_query
    ${positionFilterSQL}
    GROUP BY position_group
    ORDER BY MIN(position)
    LIMIT 10
  `;

  const periodComparisonSQL = `
    WITH tagged AS (
      SELECT
        query,
        clicks,
        impressions,
        sum_position,
        IF(
          data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY),
          'current',
          'previous'
        ) AS period
      FROM ${table}
      WHERE data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days * 2} DAY)
        AND data_date <= DATE '${lastDay}'
        AND query IS NOT NULL
        ${scopeSQL}
    ),
    per_period_query AS (
      SELECT
        period,
        query,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ${AVG_POSITION_SQL} AS position
      FROM tagged
      GROUP BY period, query
    )
    SELECT
      period,
      COUNT(*) AS visible_queries,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions
    FROM per_period_query
    ${positionFilterSQL}
    GROUP BY period
    ORDER BY period DESC
    LIMIT 2
  `;

  let timeSeriesSQL: string | null = null;
  if (granularity !== "none") {
    const trunc = GRANULARITY_TRUNC[granularity];
    timeSeriesSQL = `
      WITH per_bucket_query AS (
        SELECT
          DATE_TRUNC(data_date, ${trunc}) AS bucket,
          query,
          SUM(clicks) AS clicks,
          SUM(impressions) AS impressions,
          ${AVG_POSITION_SQL} AS position
        FROM ${table}
        WHERE ${currentWindow}
          AND query IS NOT NULL
          ${scopeSQL}
        GROUP BY bucket, query
      )
      SELECT
        bucket,
        COUNT(*) AS visible_queries,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct
      FROM per_bucket_query
      ${positionFilterSQL}
      GROUP BY bucket
      ORDER BY bucket
      LIMIT 400
    `;
  }

  let topPagesSQL: string | null = null;
  if (topPages && topPages > 0) {
    topPagesSQL = `
      WITH per_page_query AS (
        SELECT
          url,
          query,
          SUM(clicks) AS clicks,
          SUM(impressions) AS impressions,
          ${AVG_POSITION_SQL} AS position
        FROM ${table}
        WHERE ${currentWindow}
          AND query IS NOT NULL
          ${scopeSQL}
        GROUP BY url, query
      )
      SELECT
        url,
        COUNT(*) AS queries,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(AVG(position), 1) AS avg_position
      FROM per_page_query
      ${positionFilterSQL}
      GROUP BY url
      ORDER BY queries DESC
      LIMIT ${Math.min(topPages, 500)}
    `;
  }

  const [totals, byPositionGroup, periodComparison, timeSeries, topPagesResult] = await Promise.all([
    runQuery(totalsSQL, 1),
    runQuery(byPositionGroupSQL, 10),
    runQuery(periodComparisonSQL, 2),
    timeSeriesSQL ? runQuery(timeSeriesSQL, 400) : Promise.resolve(null),
    topPagesSQL ? runQuery(topPagesSQL, Math.min(topPages || 0, 500)) : Promise.resolve(null),
  ]);

  const startDateSQL = new Date(lastDay);
  startDateSQL.setUTCDate(startDateSQL.getUTCDate() - days + 1);

  return {
    period: {
      startDate: startDateSQL.toISOString().split("T")[0],
      endDate: lastDay,
      days,
      anchoredTo: `latest day in the export (${lastDay}), not today`,
    },
    totals,
    byPositionGroup,
    periodComparison,
    timeSeries,
    topPagesByQueryCount: topPagesResult,
  };
}
