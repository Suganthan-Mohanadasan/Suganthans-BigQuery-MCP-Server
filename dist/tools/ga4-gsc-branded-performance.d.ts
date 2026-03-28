export declare function ga4GscBrandedPerformance(brandTerms: string, days?: number, gscDataset?: string, ga4Dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
