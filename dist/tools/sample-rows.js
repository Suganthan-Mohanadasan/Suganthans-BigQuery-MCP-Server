"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.sampleRows = sampleRows;
const client_js_1 = require("../client.js");
const query_js_1 = require("./query.js");
async function sampleRows(dataset, table, limit = 10, projectId) {
    (0, client_js_1.validateIdentifier)(dataset, "dataset");
    (0, client_js_1.validateIdentifier)(table, "table");
    if (projectId) {
        (0, client_js_1.validateIdentifier)(projectId, "project_id");
    }
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    const capped = Math.min(Math.max(limit, 1), 100);
    const sql = `SELECT * FROM \`${targetProject}.${dataset}.${table}\` LIMIT ${capped}`;
    const [job] = await client.createQueryJob({
        query: sql,
        location: (0, client_js_1.resolveLocation)(targetProject),
        maximumBytesBilled: String(1 * 1024 * 1024 * 1024), // 1GB cap for sample queries
    });
    const [rows] = await job.getQueryResults({ maxResults: capped });
    const [metadata] = await job.getMetadata();
    const bytesProcessed = metadata.statistics?.totalBytesProcessed || "0";
    const bytesNum = parseInt(bytesProcessed, 10);
    return {
        rows,
        totalRows: rows.length,
        bytesProcessed: (0, query_js_1.formatBytes)(bytesNum),
    };
}
