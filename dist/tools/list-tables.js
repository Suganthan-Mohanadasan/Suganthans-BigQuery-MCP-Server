"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.listTables = listTables;
const client_js_1 = require("../client.js");
async function listTables(dataset, projectId) {
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    const ds = client.dataset(dataset, { projectId: targetProject });
    const [tables] = await ds.getTables();
    const results = [];
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
            columns: (meta.schema?.fields || []).map((f) => ({
                name: f.name,
                type: f.type,
                mode: f.mode || "NULLABLE",
            })),
        });
    }
    return results;
}
