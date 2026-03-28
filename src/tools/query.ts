import { getBigQueryClient, getConfig } from "../client.js";

const BLOCKED_PATTERNS = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE)\b/i;
const COMMENT_PATTERNS = /(\/\*[\s\S]*?\*\/|--[^\n]*)/g;

function sanitiseSQL(sql: string): void {
  const stripped = sql.replace(COMMENT_PATTERNS, " ");

  if (BLOCKED_PATTERNS.test(stripped)) {
    throw new Error("Only SELECT queries are allowed. This server is read-only.");
  }

  if (stripped.includes(";")) {
    const afterSemicolon = stripped.split(";").slice(1).join(";").trim();
    if (afterSemicolon.length > 0) {
      throw new Error("Multi-statement queries are not allowed. Send one SELECT at a time.");
    }
  }
}

function hasLimitClause(sql: string): boolean {
  const stripped = sql.replace(COMMENT_PATTERNS, " ");
  return /\bLIMIT\s+\d+/i.test(stripped);
}

export function formatBytes(bytesNum: number): string {
  if (bytesNum > 1024 * 1024 * 1024) {
    return `${(bytesNum / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  } else if (bytesNum > 1024 * 1024) {
    return `${(bytesNum / (1024 * 1024)).toFixed(2)} MB`;
  } else if (bytesNum > 1024) {
    return `${(bytesNum / 1024).toFixed(2)} KB`;
  }
  return `${bytesNum} bytes`;
}

export async function runQuery(
  sql: string,
  maxRows: number = 100,
  projectId?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string }> {
  sanitiseSQL(sql);

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  let finalSQL = sql.replace(/;\s*$/, "");
  if (!hasLimitClause(finalSQL)) {
    finalSQL = `${finalSQL}\nLIMIT ${maxRows}`;
  }

  const [job] = await client.createQueryJob({
    query: finalSQL,
    location: config.location,
    maximumBytesBilled: String(10 * 1024 * 1024 * 1024), // 10GB safety limit
    defaultDataset: config.defaultDataset
      ? { projectId: targetProject, datasetId: config.defaultDataset }
      : undefined,
  });

  const [rows] = await job.getQueryResults({ maxResults: maxRows });
  const [metadata] = await job.getMetadata();

  const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";
  const bytesNum = parseInt(bytesProcessed, 10);

  return {
    rows,
    totalRows: rows.length,
    bytesProcessed: formatBytes(bytesNum),
  };
}

export async function dryRunQuery(
  sql: string,
  projectId?: string
): Promise<{ bytesProcessed: string; bytesRaw: number }> {
  sanitiseSQL(sql);

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const [job] = await client.createQueryJob({
    query: sql,
    location: config.location,
    dryRun: true,
    defaultDataset: config.defaultDataset
      ? { projectId: targetProject, datasetId: config.defaultDataset }
      : undefined,
  });

  const [metadata] = await job.getMetadata();
  const bytesRaw = parseInt(metadata.statistics?.totalBytesProcessed || "0", 10);

  return {
    bytesProcessed: formatBytes(bytesRaw),
    bytesRaw,
  };
}
