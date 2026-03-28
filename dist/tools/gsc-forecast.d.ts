export declare function gscForecast(horizon?: number, confidenceLevel?: number, dataset?: string): Promise<{
    model: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
        message?: string;
    };
    forecast: {
        rows: Record<string, unknown>[];
        totalRows: number;
        bytesProcessed: string;
    };
}>;
