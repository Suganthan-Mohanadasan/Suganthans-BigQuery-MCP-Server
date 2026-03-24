interface ColumnDetail {
    name: string;
    type: string;
    mode: string;
    description: string;
}
interface TableDescription {
    id: string;
    dataset: string;
    type: string;
    rows: string;
    sizeBytes: string;
    sizeMB: string;
    created: string;
    lastModified: string;
    partitioning: string | null;
    clustering: string[] | null;
    columns: ColumnDetail[];
}
export declare function describeTable(dataset: string, table: string, projectId?: string): Promise<TableDescription>;
export {};
