export declare function gscQuickWins(days?: number, minImpressions?: number, maxPosition?: number, device?: string, country?: string, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
