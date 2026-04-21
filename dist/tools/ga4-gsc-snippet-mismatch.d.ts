export declare function ga4GscSnippetMismatch(days?: number, minClicks?: number, maxRows?: number, gscDataset?: string, ga4Dataset?: string, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
