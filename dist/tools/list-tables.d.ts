interface TableInfo {
    id: string;
    type: string;
    rows: string;
    sizeBytes: string;
    created: string;
    columns: {
        name: string;
        type: string;
        mode: string;
    }[];
}
export declare function listTables(dataset: string, projectId?: string): Promise<TableInfo[]>;
export {};
