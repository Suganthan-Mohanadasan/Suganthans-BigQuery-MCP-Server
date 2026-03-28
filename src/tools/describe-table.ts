import { getBigQueryClient, getConfig, validateIdentifier } from "../client.js";

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

export async function describeTable(
  dataset: string,
  table: string,
  projectId?: string
): Promise<TableDescription> {
  validateIdentifier(dataset, "dataset");
  validateIdentifier(table, "table");
  if (projectId) {
    validateIdentifier(projectId, "project_id");
  }

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const tableRef = client.dataset(dataset, { projectId: targetProject }).table(table);
  const [meta] = await tableRef.getMetadata();

  const sizeBytes = parseInt(meta.numBytes || "0", 10);

  return {
    id: table,
    dataset,
    type: meta.type || "TABLE",
    rows: meta.numRows || "0",
    sizeBytes: meta.numBytes || "0",
    sizeMB: (sizeBytes / (1024 * 1024)).toFixed(2),
    created: meta.creationTime
      ? new Date(parseInt(meta.creationTime)).toISOString()
      : "unknown",
    lastModified: meta.lastModifiedTime
      ? new Date(parseInt(meta.lastModifiedTime)).toISOString()
      : "unknown",
    partitioning: meta.timePartitioning?.field || null,
    clustering: meta.clustering?.fields || null,
    columns: (meta.schema?.fields || []).map(
      (f: { name: string; type: string; mode?: string; description?: string }) => ({
        name: f.name,
        type: f.type,
        mode: f.mode || "NULLABLE",
        description: f.description || "",
      })
    ),
  };
}
