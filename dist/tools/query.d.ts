export declare function runQuery(sql: string, maxRows?: number, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
