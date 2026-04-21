import { getBigQueryClient, getConfig, resolveLocation } from "../client.js";

const BLOCKED_PATTERNS = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|EXPORT|CALL|EXECUTE)\b/i;
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
    location: resolveLocation(targetProject),
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

/**
 * Run a BigQuery ML statement (CREATE MODEL, ML.FORECAST, ML.DETECT_ANOMALIES, etc.).
 * Only allows CREATE OR REPLACE MODEL and SELECT/ML.* statements.
 */
export async function runMLStatement(
  sql: string,
  maxRows: number = 1000,
  projectId?: string
): Promise<{ rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string; message?: string }> {
  const stripped = sql.replace(COMMENT_PATTERNS, " ").trim();

  // Allow only CREATE OR REPLACE MODEL, SELECT, and ML.* functions
  const isCreateModel = /^\s*CREATE\s+(OR\s+REPLACE\s+)?MODEL\b/i.test(stripped);
  const isSelect = /^\s*SELECT\b/i.test(stripped);

  if (!isCreateModel && !isSelect) {
    throw new Error("ML statements must be CREATE OR REPLACE MODEL or SELECT queries (including ML.FORECAST, ML.DETECT_ANOMALIES, ML.EXPLAIN_FORECAST).");
  }

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const [job] = await client.createQueryJob({
    query: sql,
    location: resolveLocation(targetProject),
    maximumBytesBilled: String(50 * 1024 * 1024 * 1024), // 50GB for ML training
    defaultDataset: config.defaultDataset
      ? { projectId: targetProject, datasetId: config.defaultDataset }
      : undefined,
  });

  if (isCreateModel) {
    // CREATE MODEL is a long-running job; poll until complete
    let metadata;
    while (true) {
      [metadata] = await job.getMetadata();
      const status = metadata.status?.state;
      if (status === "DONE") break;
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
    if (metadata.status?.errorResult) {
      throw new Error(metadata.status.errorResult.message);
    }
    const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";
    return {
      rows: [],
      totalRows: 0,
      bytesProcessed: formatBytes(parseInt(bytesProcessed, 10)),
      message: "Model created successfully.",
    };
  }

  const [rows] = await job.getQueryResults({ maxResults: maxRows });
  const [metadata] = await job.getMetadata();
  const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";

  return {
    rows,
    totalRows: rows.length,
    bytesProcessed: formatBytes(parseInt(bytesProcessed, 10)),
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
    location: resolveLocation(targetProject),
    dryRun: true,
    defaultDataset: config.defaultDataset
      ? { projectId: targetProject, datasetId: config.defaultDataset }
      : undefined,
  });

  // Dry run jobs are ephemeral; metadata is on the job object, not fetchable via getMetadata()
  const bytesRaw = parseInt(job.metadata?.statistics?.totalBytesProcessed || "0", 10);

  return {
    bytesProcessed: formatBytes(bytesRaw),
    bytesRaw,
  };
}
