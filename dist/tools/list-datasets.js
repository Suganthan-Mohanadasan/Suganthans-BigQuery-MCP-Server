"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.listDatasets = listDatasets;
const client_js_1 = require("../client.js");
async function listDatasets(projectId) {
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    const [datasets] = await client.getDatasets({ projectId: targetProject });
    return datasets.map((ds) => ({
        id: ds.id || "unknown",
        location: ds.metadata?.location || "unknown",
        created: ds.metadata?.creationTime
            ? new Date(parseInt(ds.metadata.creationTime)).toISOString()
            : "unknown",
    }));
}
