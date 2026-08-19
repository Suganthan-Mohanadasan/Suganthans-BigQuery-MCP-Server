"use strict";
/**
 * Shared SQL building blocks for the GSC bulk export tables.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.SEARCH_TYPES = exports.GRANULARITY_TRUNC = exports.AVG_POSITION_SQL = void 0;
exports.positionGroupSQL = positionGroupSQL;
exports.normaliseSearchType = normaliseSearchType;
exports.escapeSQLString = escapeSQLString;
exports.getLastExportDate = getLastExportDate;
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
exports.AVG_POSITION_SQL = "SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1";
/** Position group buckets, matching query_count in the GSC API MCP server. */
function positionGroupSQL(column) {
    return `CASE
        WHEN ${column} <= 3 THEN '1-3'
        WHEN ${column} <= 10 THEN '4-10'
        WHEN ${column} <= 20 THEN '11-20'
        WHEN ${column} <= 50 THEN '21-50'
        ELSE '51+'
      END`;
}
exports.GRANULARITY_TRUNC = {
    day: "DAY",
    week: "WEEK(MONDAY)",
    month: "MONTH",
};
/** Search types as spelled in the export (uppercase, unlike the API). */
exports.SEARCH_TYPES = ["WEB", "IMAGE", "VIDEO", "NEWS", "GOOGLE_NEWS", "DISCOVER"];
function normaliseSearchType(value) {
    const upper = value.toUpperCase().replace(/-/g, "_");
    if (!exports.SEARCH_TYPES.includes(upper)) {
        throw new Error(`Unknown search_type "${value}". Allowed: ${exports.SEARCH_TYPES.join(", ")}.`);
    }
    return upper;
}
/** Doubles single quotes, matching the escaping the other tools in this server use. */
function escapeSQLString(value) {
    return value.replace(/'/g, "''");
}
/**
 * Latest day present in the export.
 *
 * Anchoring ranges here instead of CURRENT_DATE() matters: the bulk export
 * trails by two to three days, so a CURRENT_DATE()-based window silently
 * includes empty days and drags period comparisons down with it.
 */
async function getLastExportDate(ds, runQuery) {
    const result = await runQuery(`SELECT MAX(data_date) AS last_day FROM \`${ds}.searchdata_url_impression\` LIMIT 1`, 1);
    const raw = result.rows[0]?.last_day;
    const value = typeof raw === "object" && raw !== null ? raw.value : raw;
    if (!value) {
        throw new Error(`No data found in \`${ds}.searchdata_url_impression\`. Is the GSC bulk export set up for this dataset?`);
    }
    return value;
}
