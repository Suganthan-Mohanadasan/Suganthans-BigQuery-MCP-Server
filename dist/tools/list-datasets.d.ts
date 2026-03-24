interface DatasetInfo {
    id: string;
    location: string;
    created: string;
}
export declare function listDatasets(projectId?: string): Promise<DatasetInfo[]>;
export {};
