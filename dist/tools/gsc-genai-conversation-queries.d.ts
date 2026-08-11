export type Bucket = "artefact" | "pivot" | "conversational" | "tracker_probe" | "agent_harness" | "pasted_string" | "long_uncategorised";
export declare function classifyQuery(query: string): Bucket | null;
interface BucketRow {
    query: string;
    impressions: number;
    clicks: number;
    ctr: number;
    position: number;
    top_pages: Array<{
        page: string;
        impressions: number;
    }>;
}
export declare function gscGenaiConversationQueries(days?: number, minImpressions?: number, maxRowsPerBucket?: number, includeTimeline?: boolean, dataset?: string): Promise<{
    window: {
        start: {} | null;
        end: {} | null;
        days_requested: number;
        note: string;
    };
    export_retention: Record<string, unknown>;
    summary: Record<string, {
        queries: number;
        impressions: number;
        clicks: number;
    }>;
    total_conversation_queries: number;
    total_conversation_impressions: number;
    excluded_ordinary_matches: number;
    anonymised_visibility: {
        anonymised_impressions: number;
        visible_impressions: number;
        anonymised_share_pct: number | null;
        anonymised_clicks: number;
        visible_clicks: number;
        distinct_visible_queries: number;
        meaning: string;
    };
    buckets: {
        [k: string]: {
            meaning: string;
            total_matched: number;
            rows: BucketRow[];
        };
    };
    artefact_timeline_monthly: Record<string, unknown>[] | undefined;
    what_this_cannot_see: string[];
}>;
export {};
