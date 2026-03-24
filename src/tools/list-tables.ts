import { getBigQueryClient, getConfig } from "../client.js";

interface TableInfo {
  id: string;
  type: string;
  rows: string;
  sizeBytes: string;
  created: string;
  columns: { name: string; type: string; mode: string }[];
}

export async function listTables(
  dataset: string,
  projectId?: string
): Promise<TableInfo[]> {
  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const ds = client.dataset(dataset, { projectId: targetProject });
  const [tables] = await ds.getTables();

  const results: TableInfo[] = [];

  for (const table of tables) {
    const [meta] = await table.getMetadata();
    results.push({
      id: table.id || "unknown",
      type: meta.type || "TABLE",
      rows: meta.numRows || "0",
      sizeBytes: meta.numBytes || "0",
      created: meta.creationTime
        ? new Date(parseInt(meta.creationTime)).toISOString()
        : "unknown",
      columns: (meta.schema?.fields || []).map(
        (f: { name: string; type: string; mode?: string }) => ({
          name: f.name,
          type: f.type,
          mode: f.mode || "NULLABLE",
        })
      ),
    });
  }

  return results;
}
