export declare function gscTopicCluster(urlPattern: string, days?: number, dataset?: string): Promise<{
    summary: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
    };
    topPages: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
    };
    topQueries: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
    };
}>;
