interface AlertResult {
    alerts: Record<string, unknown>[];
    disappeared: Record<string, unknown>[];
    summary: {
        total: number;
        critical: number;
        warning: number;
    };
    bytesProcessed: string;
}
export declare function gscAlerts(days?: number, positionDropThreshold?: number, ctrDropPct?: number, clickDropPct?: number, dataset?: string): Promise<AlertResult>;
export {};
