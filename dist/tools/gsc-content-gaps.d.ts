export declare function gscContentGaps(days?: number, minImpressions?: number, minPosition?: number, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
