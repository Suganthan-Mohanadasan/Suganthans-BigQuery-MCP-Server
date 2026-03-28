export declare function formatBytes(bytesNum: number): string;
export declare function runQuery(sql: string, maxRows?: number, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
}>;
/**
 * Run a BigQuery ML statement (CREATE MODEL, ML.FORECAST, ML.DETECT_ANOMALIES, etc.).
 * Only allows CREATE OR REPLACE MODEL and SELECT/ML.* statements.
 */
export declare function runMLStatement(sql: string, maxRows?: number, projectId?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
    message?: string;
}>;
export declare function dryRunQuery(sql: string, projectId?: string): Promise<{
    bytesProcessed: string;
    bytesRaw: number;
}>;
