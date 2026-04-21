import { BigQuery } from "@google-cloud/bigquery";
export declare function validateIdentifier(value: string, label: string): void;
export declare function getConfig(): {
    keyFile: string | undefined;
    projectId: string;
    defaultDataset: string | undefined;
    location: string;
    ga4ProjectId: string | undefined;
    ga4Location: string | undefined;
};
/**
 * Resolve the correct BigQuery location for a given project.
 * If the target project matches the GA4 project and a separate GA4 location
 * is configured, use that. Otherwise fall back to the default location.
 */
export declare function resolveLocation(targetProject?: string): string;
export declare function getBigQueryClient(): BigQuery;
