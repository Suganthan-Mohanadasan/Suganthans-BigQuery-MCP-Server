export declare function gscSiteSnapshot(days?: number, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
