type QueryResult = {
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
};
export type CurveSegment = "none" | "device" | "country" | "search_type" | "branded";
/**
 * Your own click curve: CTR by ranking position, measured on your data.
 *
 * Every "expected CTR by position" table in an SEO tool is somebody else's
 * average - usually a study across unrelated sites. This builds the curve from
 * the export: aggregate to (url, query), take that pair's average position,
 * round it to a rank, then sum clicks and impressions per rank. CTR is the
 * ratio of those sums, never an average of ratios, so high-volume pairs carry
 * the weight they should.
 *
 * With segment_by="branded" and a brand_pattern, the branded and non-branded
 * curves come back separately - the split that matters most, because branded
 * queries inflate a blended curve at the top and make everything else look
 * like it underperforms.
 */
export declare function gscClickCurve(days?: number, maxPosition?: number, minImpressionsPerRank?: number, segmentBy?: CurveSegment, brandPattern?: string, searchType?: string, urlContains?: string, dataset?: string): Promise<{
    period: {
        startDate: string;
        endDate: string;
        days: number;
        anchoredTo: string;
    };
    method: string;
    curve: QueryResult;
    coverage: QueryResult;
}>;
export {};
