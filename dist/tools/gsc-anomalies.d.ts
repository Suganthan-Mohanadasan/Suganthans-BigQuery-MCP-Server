export declare function gscAnomalies(lookbackDays?: number, anomalyThreshold?: number, dataset?: string): Promise<{
    model: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
        message?: string;
    };
    anomalies: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
    };
}>;
