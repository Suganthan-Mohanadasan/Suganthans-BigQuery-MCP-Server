import { getBigQueryClient, getConfig, validateIdentifier } from "../client.js";
import { formatBytes } from "./query.js";

export async function sampleRows(
  dataset: string,
  table: string,
  limit: number = 10,
  projectId?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  validateIdentifier(dataset, "dataset");
  validateIdentifier(table, "table");
  if (projectId) {
    validateIdentifier(projectId, "project_id");
  }

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;
  const capped = Math.min(Math.max(limit, 1), 100);

  const sql = `SELECT * FROM \`${targetProject}.${dataset}.${table}\` LIMIT ${capped}`;

  const [job] = await client.createQueryJob({
    query: sql,
    location: config.location,
    maximumBytesBilled: String(1 * 1024 * 1024 * 1024), // 1GB cap for sample queries
  });

  const [rows] = await job.getQueryResults({ maxResults: capped });
  const [metadata] = await job.getMetadata();

  const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";
  const bytesNum = parseInt(bytesProcessed, 10);

  return {
    rows,
    totalRows: rows.length,
    bytesProcessed: formatBytes(bytesNum),
  };
}
