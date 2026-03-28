export declare function gscNewKeywords(recentDays?: number, baselineDays?: number, minImpressions?: number, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
