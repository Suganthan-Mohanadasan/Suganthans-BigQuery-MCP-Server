export declare function gscTrafficDrops(days?: number, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
