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
/**
 * Der letzte Tag, den der Export enthaelt - als SQL-Ausdruck.
 *
 * Ersetzt CURRENT_DATE() in den Zeitfenstern. Der Bulk-Export laeuft zwei bis drei
 * Tage nach, ein Fenster ab heute nimmt also leere Tage mit: gemessen deckte
 * "letzte 7 Tage" real nur vier Tage ab und meldete entsprechend zu wenig.
 *
 * Die Unterabfrage laeuft nur ueber die Partitionsspalte und ist damit billig.
 */
export declare function lastExportDay(ds: string, table?: "searchdata_url_impression" | "searchdata_site_impression"): string;
/**
 * Die Studienkurve als SQL-CASE - nur der Rueckfall.
 *
 * Branchenweite Fremdwerte. Gemessen an einer Content-Property liegt die echte
 * CTR auf Position 1 bei 3,5 % gegen die 28,5 % hier, Faktor 8. Wer dagegen
 * bewertet, stempelt fast jede Seite als unterdurchschnittlich.
 */
export declare function studyBenchmarkCaseSQL(positionColumn: string): string;
/**
 * CTEs, die die eigene Klickkurve aus dem Export bauen: curve_pairs und curve.
 *
 * Erst auf (url, query) aggregieren und daraus die Durchschnittsposition nehmen,
 * dann auf den gerundeten Rang buendeln. CTR je Rang ist Summe der Klicks durch
 * Summe der Impressionen, nie ein Mittel aus Verhaeltnissen. Raenge unter
 * minImpressionsPerRank fallen raus und werden von der Studienkurve gedeckt.
 */
export declare function measuredCurveCTEs(ds: string, curveDays?: number, minImpressionsPerRank?: number, scopeSQL?: string): string;
/** Devices as spelled in the export. */
export declare const DEVICES: readonly ["MOBILE", "DESKTOP", "TABLET"];
export type Device = (typeof DEVICES)[number];
export declare function normaliseDevice(value: string): Device;
/** Countries are ISO-3166-1 alpha-3, lowercase in the export (deu, aut, che, usa). */
export declare function normaliseCountry(value: string): string;
/**
 * WHERE conditions for device and country.
 *
 * Both are optional and unset means unfiltered - every tool keeps returning all
 * devices and all countries unless asked otherwise.
 *
 * Discover carries no device: the column is NULL on every DISCOVER row, so a
 * device filter there would silently return nothing. That fails loudly instead.
 */
export declare function deviceCountryConditions(device?: string, country?: string, searchType?: string): string[];
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
