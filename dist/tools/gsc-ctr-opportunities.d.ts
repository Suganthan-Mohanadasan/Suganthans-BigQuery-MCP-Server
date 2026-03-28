export declare function gscCtrOpportunities(days?: number, minImpressions?: number, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
