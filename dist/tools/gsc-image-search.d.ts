import { Granularity } from "./gsc-shared.js";
type QueryResult = {
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
};
/**
 * Google Images performance from the bulk export.
 *
 * Unlike Discover, image search does carry queries, so this groups by both url
 * and query. The url is the page the image sits on, never the image file - the
 * export has no image-level dimension, which is the honest limit of this
 * surface and the reason a low image CTR says little about the image itself.
 *
 * `is_amp_image_result` is reported alongside, because an AMP image result is a
 * different SERP element from an ordinary image result.
 */
export declare function gscImageSearch(days?: number, granularity?: Granularity, urlContains?: string, topRows?: number, dataset?: string): Promise<{
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
    topQueries: QueryResult;
    byDevice: QueryResult;
    byCountry: QueryResult;
    note: string;
}>;
export {};
