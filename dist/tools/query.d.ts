export declare function formatBytes(bytesNum: number): string;
export declare function runQuery(sql: string, maxRows?: number, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
export declare function dryRunQuery(sql: string, projectId?: string): Promise<{
    bytesProcessed: string;
    bytesRaw: number;
}>;
