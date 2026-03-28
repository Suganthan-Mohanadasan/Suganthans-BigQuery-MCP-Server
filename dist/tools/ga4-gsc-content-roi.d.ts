export declare function ga4GscContentRoi(days?: number, minClicks?: number, maxRows?: number, gscDataset?: string, ga4Dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
