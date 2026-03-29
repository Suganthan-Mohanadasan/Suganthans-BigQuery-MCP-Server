"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.formatBytes = formatBytes;
exports.runQuery = runQuery;
exports.runMLStatement = runMLStatement;
exports.dryRunQuery = dryRunQuery;
const client_js_1 = require("../client.js");
const BLOCKED_PATTERNS = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|EXPORT|CALL|EXECUTE)\b/i;
const COMMENT_PATTERNS = /(\/\*[\s\S]*?\*\/|--[^\n]*)/g;
function sanitiseSQL(sql) {
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
function hasLimitClause(sql) {
    const stripped = sql.replace(COMMENT_PATTERNS, " ");
    return /\bLIMIT\s+\d+/i.test(stripped);
}
function formatBytes(bytesNum) {
    if (bytesNum > 1024 * 1024 * 1024) {
        return `${(bytesNum / (1024 * 1024 * 1024)).toFixed(2)} GB`;
    }
    else if (bytesNum > 1024 * 1024) {
        return `${(bytesNum / (1024 * 1024)).toFixed(2)} MB`;
    }
    else if (bytesNum > 1024) {
        return `${(bytesNum / 1024).toFixed(2)} KB`;
    }
    return `${bytesNum} bytes`;
}
async function runQuery(sql, maxRows = 100, projectId) {
    sanitiseSQL(sql);
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
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
/**
 * Run a BigQuery ML statement (CREATE MODEL, ML.FORECAST, ML.DETECT_ANOMALIES, etc.).
 * Only allows CREATE OR REPLACE MODEL and SELECT/ML.* statements.
 */
async function runMLStatement(sql, maxRows = 1000, projectId) {
    const stripped = sql.replace(COMMENT_PATTERNS, " ").trim();
    // Allow only CREATE OR REPLACE MODEL, SELECT, and ML.* functions
    const isCreateModel = /^\s*CREATE\s+(OR\s+REPLACE\s+)?MODEL\b/i.test(stripped);
    const isSelect = /^\s*SELECT\b/i.test(stripped);
    if (!isCreateModel && !isSelect) {
        throw new Error("ML statements must be CREATE OR REPLACE MODEL or SELECT queries (including ML.FORECAST, ML.DETECT_ANOMALIES, ML.EXPLAIN_FORECAST).");
    }
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    const [job] = await client.createQueryJob({
        query: sql,
        location: config.location,
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
            if (status === "DONE")
                break;
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
async function dryRunQuery(sql, projectId) {
    sanitiseSQL(sql);
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    const [job] = await client.createQueryJob({
        query: sql,
        location: config.location,
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
