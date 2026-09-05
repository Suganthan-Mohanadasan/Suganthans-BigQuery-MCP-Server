import { Granularity } from "./gsc-shared.js";
type QueryResult = {
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
};
/**
 * Google Discover performance from the bulk export.
 *
 * Discover is page-based: there is no query dimension, so nothing here groups
 * by query. Its own anonymisation flag is `is_anonymized_discover`, separate
 * from `is_anonymized_query`.
 *
 * There is deliberately no device breakdown: `device` is NULL on every Discover
 * row, so the block only ever returned a single empty bucket holding every
 * click. Mobile versus desktop is not answerable for this surface.
 */
export declare function gscDiscover(days?: number, granularity?: Granularity, urlContains?: string, topUrls?: number, dataset?: string): Promise<{
    period: {
        startDate: string;
        endDate: string;
        days: number;
        anchoredTo: string;
    };
    summary: QueryResult;
    shareOfSite: QueryResult;
    timeSeries: QueryResult | null;
    topUrls: QueryResult;
    byCountry: QueryResult;
}>;
export {};
