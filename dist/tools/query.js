"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runQuery = runQuery;
const client_js_1 = require("../client.js");
const BLOCKED_PATTERNS = /^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE)\b/i;
async function runQuery(sql, maxRows = 100, projectId) {
    if (BLOCKED_PATTERNS.test(sql)) {
        throw new Error("Only SELECT queries are allowed. This server is read-only.");
    }
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
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
    const bytesFormatted = bytesNum > 1024 * 1024 * 1024
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
