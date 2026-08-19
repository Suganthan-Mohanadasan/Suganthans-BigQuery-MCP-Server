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
 * Note: the older gsc_* tools in this server omit the +1 and therefore report
 * every position one rank better than it is.
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

/** Doubles single quotes, matching the escaping the other tools in this server use. */
export function escapeSQLString(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * Latest day present in the export.
 *
 * Anchoring ranges here instead of CURRENT_DATE() matters: the bulk export
 * trails by two to three days, so a CURRENT_DATE()-based window silently
 * includes empty days and drags period comparisons down with it.
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
