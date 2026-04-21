export declare function ga4GscBrandedPerformance(brandTerms: string, days?: number, gscDataset?: string, ga4Dataset?: string, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
