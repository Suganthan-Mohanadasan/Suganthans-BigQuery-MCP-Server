import { getBigQueryClient, getConfig } from "../client.js";

const BLOCKED_PATTERNS = /^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE)\b/i;

export async function runQuery(
  sql: string,
  maxRows: number = 100,
  projectId?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  if (BLOCKED_PATTERNS.test(sql)) {
    throw new Error("Only SELECT queries are allowed. This server is read-only.");
  }

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const [job] = await client.createQueryJob({
    query: sql,
    location: "US",
    maximumBytesBilled: String(10 * 1024 * 1024 * 1024), // 10GB safety limit
    defaultDataset: config.defaultDataset
      ? { projectId: targetProject, datasetId: config.defaultDataset }
      : undefined,
  });

  const [rows] = await job.getQueryResults({ maxResults: maxRows });
  const [metadata] = await job.getMetadata();

  const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";
  const bytesNum = parseInt(bytesProcessed, 10);
  const bytesFormatted =
    bytesNum > 1024 * 1024 * 1024
      ? `${(bytesNum / (1024 * 1024 * 1024)).toFixed(2)} GB`
      : bytesNum > 1024 * 1024
        ? `${(bytesNum / (1024 * 1024)).toFixed(2)} MB`
        : bytesNum > 1024
          ? `${(bytesNum / 1024).toFixed(2)} KB`
          : `${bytesNum} bytes`;

  return {
    rows,
    totalRows: rows.length,
    bytesProcessed: bytesFormatted,
  };
}
