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
export declare const AVG_POSITION_SQL = "SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1";
/** Position group buckets, matching query_count in the GSC API MCP server. */
export declare function positionGroupSQL(column: string): string;
export type Granularity = "none" | "day" | "week" | "month";
export declare const GRANULARITY_TRUNC: Record<Exclude<Granularity, "none">, string>;
/** Search types as spelled in the export (uppercase, unlike the API). */
export declare const SEARCH_TYPES: readonly ["WEB", "IMAGE", "VIDEO", "NEWS", "GOOGLE_NEWS", "DISCOVER"];
export type ExportSearchType = (typeof SEARCH_TYPES)[number];
export declare function normaliseSearchType(value: string): ExportSearchType;
/** Doubles single quotes, matching the escaping the other tools in this server use. */
export declare function escapeSQLString(value: string): string;
/**
 * Latest day present in the export.
 *
 * Anchoring ranges here instead of CURRENT_DATE() matters: the bulk export
 * trails by two to three days, so a CURRENT_DATE()-based window silently
 * includes empty days and drags period comparisons down with it.
 */
export declare function getLastExportDate(ds: string, runQuery: (sql: string, maxRows?: number) => Promise<{
    rows: Record<string, unknown>[];
}>): Promise<string>;
