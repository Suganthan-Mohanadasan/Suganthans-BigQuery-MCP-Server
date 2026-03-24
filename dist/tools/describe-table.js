"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.describeTable = describeTable;
const client_js_1 = require("../client.js");
async function describeTable(dataset, table, projectId) {
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
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
        columns: (meta.schema?.fields || []).map((f) => ({
            name: f.name,
            type: f.type,
            mode: f.mode || "NULLABLE",
            description: f.description || "",
        })),
    };
}
