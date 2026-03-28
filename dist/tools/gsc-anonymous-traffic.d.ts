export declare function gscAnonymousTraffic(days?: number, dataset?: string): Promise<{
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
}>;
