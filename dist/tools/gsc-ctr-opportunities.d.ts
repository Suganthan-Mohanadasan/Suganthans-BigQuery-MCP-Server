export declare function gscCtrOpportunities(days?: number, minImpressions?: number, device?: string, country?: string, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
