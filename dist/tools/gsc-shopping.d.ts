import { Granularity } from "./gsc-shared.js";
type QueryResult = {
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
};
/**
 * Organic shopping surfaces: free product listings, merchant listings and
 * product snippets.
 *
 * These are search appearances, not search types - they live inside WEB rows,
 * flagged by their own boolean columns. The GSC API exposes them only through
 * the searchAppearance dimension, which cannot be combined with other
 * groupings; here they are ordinary columns, so they can be crossed with url,
 * query, device and date freely.
 *
 * A property that sells nothing returns zeros. That is an answer, not a bug,
 * and the result says so explicitly instead of looking like a failed query.
 */
export declare const SHOPPING_APPEARANCES: {
    readonly organic_shopping: "is_organic_shopping";
    readonly merchant_listings: "is_merchant_listings";
    readonly product_snippets: "is_product_snippets";
};
export type ShoppingAppearance = keyof typeof SHOPPING_APPEARANCES;
export declare function gscShopping(days?: number, appearance?: ShoppingAppearance, granularity?: Granularity, urlContains?: string, topRows?: number, dataset?: string): Promise<{
    period: {
        startDate: string;
        endDate: string;
        days: number;
        anchoredTo: string;
    };
    overview: QueryResult;
    dataAvailable: boolean;
    note: string;
    drilldown: {
        appearance: ShoppingAppearance;
        topUrls: QueryResult;
        topQueries: QueryResult;
        timeSeries: QueryResult | null;
    } | null;
}>;
export {};
