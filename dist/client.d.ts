import { BigQuery } from "@google-cloud/bigquery";
export declare function validateIdentifier(value: string, label: string): void;
export declare function getConfig(): {
    keyFile: string | undefined;
    projectId: string;
    defaultDataset: string | undefined;
    location: string;
};
export declare function getBigQueryClient(): BigQuery;
