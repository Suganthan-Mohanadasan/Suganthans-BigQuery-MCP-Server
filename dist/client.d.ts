import { BigQuery } from "@google-cloud/bigquery";
export declare function validateIdentifier(value: string, label: string): void;
export declare function getConfig(): {
    keyFile: string | undefined;
    projectId: string;
    defaultDataset: string | undefined;
    location: string;
    ga4ProjectId: string | undefined;
    ga4Location: string | undefined;
    datasetLocations: Map<string, string>;
};
/**
 * Extract the datasets a SQL statement touches, so a query can be routed to the
 * location its data actually lives in.
 *
 * Handles backticked three-part (`project.dataset.table`) and two-part
 * (`dataset.table`) references as well as unquoted FROM/JOIN targets. Wildcard
 * suffixes (events_*), INFORMATION_SCHEMA and __TABLES__ are covered because only
 * the dataset segment is read.
 */
export declare function extractDatasets(sql: string): string[];
/**
 * Resolve the correct BigQuery location for a job.
 *
 * Precedence:
 *   1. BIGQUERY_DATASET_LOCATIONS entry for a dataset the job touches
 *   2. BIGQUERY_GA4_LOCATION when the target project is the GA4 project
 *   3. BIGQUERY_LOCATION (default)
 *
 * If the job touches datasets that are mapped to different locations, this throws
 * instead of letting BigQuery fail with a confusing "Dataset not found in location"
 * error — a single query cannot span locations.
 */
export declare function resolveLocation(targetProject?: string, datasets?: string | string[]): string;
export declare function getBigQueryClient(): BigQuery;
