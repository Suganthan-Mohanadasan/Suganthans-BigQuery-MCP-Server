import { BigQuery } from "@google-cloud/bigquery";
export declare function getConfig(): {
    keyFile: string | undefined;
    projectId: string;
    defaultDataset: string | undefined;
};
export declare function getBigQueryClient(): BigQuery;
