/**
 * Shared SQL building blocks for the GSC bulk export tables.
 */

/**
 * Average position from the GSC bulk export.
 *
 * `sum_position` is ZERO-BASED, so the average rank is sum/impressions + 1.
 * Verified against a live export: over one week, 12,544 of 411,617 (url, query)
 * pairs have sum_position/impressions below 1 and the minimum is exactly 0 -
 * and position 0 does not exist in Google Search.
 *
 * This matches the `+ 1` the rest of the server applies since v4.1.1.
 */
export const AVG_POSITION_SQL = "SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1";

/** Position group buckets, matching query_count in the GSC API MCP server. */
export function positionGroupSQL(column: string): string {
  return `CASE
        WHEN ${column} <= 3 THEN '1-3'
        WHEN ${column} <= 10 THEN '4-10'
        WHEN ${column} <= 20 THEN '11-20'
        WHEN ${column} <= 50 THEN '21-50'
        ELSE '51+'
      END`;
}

export type Granularity = "none" | "day" | "week" | "month";

export const GRANULARITY_TRUNC: Record<Exclude<Granularity, "none">, string> = {
  day: "DAY",
  week: "WEEK(MONDAY)",
  month: "MONTH",
};

/** Search types as spelled in the export (uppercase, unlike the API). */
export const SEARCH_TYPES = ["WEB", "IMAGE", "VIDEO", "NEWS", "GOOGLE_NEWS", "DISCOVER"] as const;
export type ExportSearchType = (typeof SEARCH_TYPES)[number];

export function normaliseSearchType(value: string): ExportSearchType {
  const upper = value.toUpperCase().replace(/-/g, "_") as ExportSearchType;
  if (!SEARCH_TYPES.includes(upper)) {
    throw new Error(
      `Unknown search_type "${value}". Allowed: ${SEARCH_TYPES.join(", ")}.`
    );
  }
  return upper;
}

/** Devices as spelled in the export. */
export const DEVICES = ["MOBILE", "DESKTOP", "TABLET"] as const;
export type Device = (typeof DEVICES)[number];

export function normaliseDevice(value: string): Device {
  const upper = value.toUpperCase() as Device;
  if (!DEVICES.includes(upper)) {
    throw new Error(`Unknown device "${value}". Allowed: ${DEVICES.join(", ")}.`);
  }
  return upper;
}

/** Countries are ISO-3166-1 alpha-3, lowercase in the export (deu, aut, che, usa). */
export function normaliseCountry(value: string): string {
  const lower = value.toLowerCase();
  if (!/^[a-z]{3}$/.test(lower)) {
    throw new Error(
      `Country must be an ISO-3166-1 alpha-3 code such as deu, aut, che or usa - got "${value}".`
    );
  }
  return lower;
}

/**
 * WHERE conditions for device and country.
 *
 * Both are optional and unset means unfiltered - every tool keeps returning all
 * devices and all countries unless asked otherwise.
 *
 * Discover carries no device: the column is NULL on every DISCOVER row, so a
 * device filter there would silently return nothing. That fails loudly instead.
 */
export function deviceCountryConditions(
  device?: string,
  country?: string,
  searchType?: string
): string[] {
  const conditions: string[] = [];

  if (device) {
    if (searchType && searchType.toUpperCase() === "DISCOVER") {
      throw new Error(
        "Discover rows carry no device - the column is NULL for every Discover impression, so a device filter cannot match. Drop the device argument, or query a different surface."
      );
    }
    conditions.push(`device = '${normaliseDevice(device)}'`);
  }

  if (country) {
    conditions.push(`country = '${normaliseCountry(country)}'`);
  }

  return conditions;
}

/** Doubles single quotes, matching the escaping the other tools in this server use. */
export function escapeSQLString(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * The published study curve as a SQL CASE - the fallback, not the default.
 *
 * These are somebody else's averages, taken across unrelated sites. Measured
 * against one content property, the real CTR at position 1 was 3.54% against
 * the 28.5% here: a factor of eight. Benchmarking a site against these numbers
 * marks almost every page as underperforming, which is why this is only used
 * for ranks where the property's own data is too thin to measure.
 */
export function studyBenchmarkCaseSQL(positionColumn: string): string {
  return `CASE
          WHEN ${positionColumn} <= 1 THEN 28.5
          WHEN ${positionColumn} <= 2 THEN 15.7
          WHEN ${positionColumn} <= 3 THEN 11.0
          WHEN ${positionColumn} <= 4 THEN 8.0
          WHEN ${positionColumn} <= 5 THEN 7.2
          WHEN ${positionColumn} <= 6 THEN 5.1
          WHEN ${positionColumn} <= 7 THEN 4.0
          WHEN ${positionColumn} <= 8 THEN 3.2
          WHEN ${positionColumn} <= 9 THEN 2.8
          WHEN ${positionColumn} <= 10 THEN 2.5
          ELSE GREATEST(0.5, 2.5 - (${positionColumn} - 10) * 0.2)
        END`;
}

/**
 * CTEs that build the property's own click curve from the export:
 * `curve_pairs` and `curve`.
 *
 * Aggregate to (url, query) first and take that pair's average position, then
 * bundle by rounded rank. CTR per rank is summed clicks over summed
 * impressions, never a mean of ratios, so high-volume pairs carry the weight
 * they should. Ranks below minImpressionsPerRank are dropped rather than
 * reported as noise; callers are expected to fall back to studyBenchmarkCaseSQL
 * for those.
 *
 * The window is separate from the caller's analysis window on purpose: a curve
 * needs volume per rank, so it reads further back than the period being judged.
 * It is also the cost lever - this CTE scans curveDays of
 * searchdata_url_impression.
 */
export function measuredCurveCTEs(
  ds: string,
  curveDays: number = 90,
  minImpressionsPerRank: number = 100,
  scopeSQL: string = ""
): string {
  return `curve_pairs AS (
      SELECT
        SUM(clicks) AS curve_clicks,
        SUM(impressions) AS curve_impressions,
        ${AVG_POSITION_SQL} AS curve_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB((SELECT MAX(data_date) FROM \`${ds}.searchdata_url_impression\`), INTERVAL ${curveDays} DAY)
        AND search_type = 'WEB'
        AND query IS NOT NULL
        ${scopeSQL}
      GROUP BY url, query
    ),
    curve AS (
      SELECT
        CAST(ROUND(curve_position) AS INT64) AS rank,
        SAFE_DIVIDE(SUM(curve_clicks), SUM(curve_impressions)) * 100 AS measured_ctr_pct,
        SUM(curve_impressions) AS rank_impressions
      FROM curve_pairs
      GROUP BY rank
      HAVING SUM(curve_impressions) >= ${minImpressionsPerRank}
    )`;
}

/**
 * Latest day present in the export.
 *
 * Anchoring ranges here instead of CURRENT_DATE() matters: the bulk export
 * trails by two to three days, so a CURRENT_DATE()-based window silently
 * includes empty days and drags period comparisons down with it. Same principle
 * as the inline `(SELECT MAX(data_date) ...)` subqueries v4.1.1 introduced,
 * resolved once per call so these tools can also label the window they used.
 */
export async function getLastExportDate(
  ds: string,
  runQuery: (sql: string, maxRows?: number) => Promise<{ rows: Record<string, unknown>[] }>
): Promise<string> {
  const result = await runQuery(
    `SELECT MAX(data_date) AS last_day FROM \`${ds}.searchdata_url_impression\` LIMIT 1`,
    1
  );
  const raw = result.rows[0]?.last_day as { value?: string } | string | undefined;
  const value = typeof raw === "object" && raw !== null ? raw.value : raw;
  if (!value) {
    throw new Error(
      `No data found in \`${ds}.searchdata_url_impression\`. Is the GSC bulk export set up for this dataset?`
    );
  }
  return value;
}
