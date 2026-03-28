export declare function ga4GscPagePerformance(days?: number, minClicks?: number, maxRows?: number, gscDataset?: string, ga4Dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
