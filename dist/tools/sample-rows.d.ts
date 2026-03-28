export declare function sampleRows(dataset: string, table: string, limit?: number, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
