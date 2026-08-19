import { Granularity } from "./gsc-shared.js";
type QueryResult = {
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
};
/**
 * Query counting on the warehouse instead of the API.
 *
 * Two things this can do that the API version cannot: it counts across the full
 * export instead of a 1,000-row page, and it reads the anonymized share from
 * the `is_anonymized_query` flag rather than inferring it from a click gap.
 */
export declare function gscQueryCount(days?: number, url?: string, urlContains?: string, granularity?: Granularity, minPosition?: number, maxPosition?: number, searchType?: string, topPages?: number, dataset?: string): Promise<{
    period: {
        startDate: string;
        endDate: string;
        days: number;
        anchoredTo: string;
    };
    totals: QueryResult;
    byPositionGroup: QueryResult;
    periodComparison: QueryResult;
    timeSeries: QueryResult | null;
    topPagesByQueryCount: QueryResult | null;
}>;
export {};
