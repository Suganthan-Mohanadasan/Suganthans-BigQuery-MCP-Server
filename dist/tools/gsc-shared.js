"use strict";
/**
 * Shared SQL building blocks for the GSC bulk export tables.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEVICES = exports.SEARCH_TYPES = exports.GRANULARITY_TRUNC = exports.AVG_POSITION_SQL = void 0;
exports.positionGroupSQL = positionGroupSQL;
exports.normaliseSearchType = normaliseSearchType;
exports.lastExportDay = lastExportDay;
exports.studyBenchmarkCaseSQL = studyBenchmarkCaseSQL;
exports.measuredCurveCTEs = measuredCurveCTEs;
exports.normaliseDevice = normaliseDevice;
exports.normaliseCountry = normaliseCountry;
exports.deviceCountryConditions = deviceCountryConditions;
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
/**
 * Der letzte Tag, den der Export enthaelt - als SQL-Ausdruck.
 *
 * Ersetzt CURRENT_DATE() in den Zeitfenstern. Der Bulk-Export laeuft zwei bis drei
 * Tage nach, ein Fenster ab heute nimmt also leere Tage mit: gemessen deckte
 * "letzte 7 Tage" real nur vier Tage ab und meldete entsprechend zu wenig.
 *
 * Die Unterabfrage laeuft nur ueber die Partitionsspalte und ist damit billig.
 */
function lastExportDay(ds, table = "searchdata_url_impression") {
    return `(SELECT MAX(data_date) FROM \`${ds}.${table}\`)`;
}
/**
 * Die Studienkurve als SQL-CASE - nur der Rueckfall.
 *
 * Branchenweite Fremdwerte. Gemessen an einer Content-Property liegt die echte
 * CTR auf Position 1 bei 3,5 % gegen die 28,5 % hier, Faktor 8. Wer dagegen
 * bewertet, stempelt fast jede Seite als unterdurchschnittlich.
 */
function studyBenchmarkCaseSQL(positionColumn) {
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
 * CTEs, die die eigene Klickkurve aus dem Export bauen: curve_pairs und curve.
 *
 * Erst auf (url, query) aggregieren und daraus die Durchschnittsposition nehmen,
 * dann auf den gerundeten Rang buendeln. CTR je Rang ist Summe der Klicks durch
 * Summe der Impressionen, nie ein Mittel aus Verhaeltnissen. Raenge unter
 * minImpressionsPerRank fallen raus und werden von der Studienkurve gedeckt.
 */
function measuredCurveCTEs(ds, curveDays = 90, minImpressionsPerRank = 100, scopeSQL = "") {
    return `curve_pairs AS (
      SELECT
        SUM(clicks) AS curve_clicks,
        SUM(impressions) AS curve_impressions,
        ${exports.AVG_POSITION_SQL} AS curve_position
      FROM \`${ds}.searchdata_url_impression\`
      WHERE
        data_date >= DATE_SUB(${lastExportDay(ds, "searchdata_url_impression")}, INTERVAL ${curveDays} DAY)
        AND search_type = 'WEB'
        AND query IS NOT NULL
        ${scopeSQL}
      GROUP BY url, query
    ),
    curve AS (
      SELECT
        CAST(ROUND(curve_position) AS INT64) AS rank,
        SAFE_DIVIDE(SUM(curve_clicks), SUM(curve_impressions)) * 100 AS measured_ctr_pct
      FROM curve_pairs
      GROUP BY rank
      HAVING SUM(curve_impressions) >= ${minImpressionsPerRank}
    )`;
}
/** Devices as spelled in the export. */
exports.DEVICES = ["MOBILE", "DESKTOP", "TABLET"];
function normaliseDevice(value) {
    const upper = value.toUpperCase();
    if (!exports.DEVICES.includes(upper)) {
        throw new Error(`Unknown device "${value}". Allowed: ${exports.DEVICES.join(", ")}.`);
    }
    return upper;
}
/** Countries are ISO-3166-1 alpha-3, lowercase in the export (deu, aut, che, usa). */
function normaliseCountry(value) {
    const lower = value.toLowerCase();
    if (!/^[a-z]{3}$/.test(lower)) {
        throw new Error(`Country must be an ISO-3166-1 alpha-3 code such as deu, aut, che or usa - got "${value}".`);
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
function deviceCountryConditions(device, country, searchType) {
    const conditions = [];
    if (device) {
        if (searchType && searchType.toUpperCase() === "DISCOVER") {
            throw new Error("Discover rows carry no device - the column is NULL for every Discover impression, so a device filter cannot match. Drop the device argument, or query a different surface.");
        }
        conditions.push(`device = '${normaliseDevice(device)}'`);
    }
    if (country) {
        conditions.push(`country = '${normaliseCountry(country)}'`);
    }
    return conditions;
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
