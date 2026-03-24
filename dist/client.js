"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getConfig = getConfig;
exports.getBigQueryClient = getBigQueryClient;
const bigquery_1 = require("@google-cloud/bigquery");
let cachedClient = null;
function getConfig() {
    const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
    const projectId = process.env.BIGQUERY_PROJECT_ID;
    const defaultDataset = process.env.BIGQUERY_DEFAULT_DATASET;
    if (!projectId) {
        throw new Error("BIGQUERY_PROJECT_ID environment variable is required. Set it to your Google Cloud project ID.");
    }
    return { keyFile, projectId, defaultDataset };
}
function getBigQueryClient() {
    if (cachedClient)
        return cachedClient;
    const { keyFile, projectId } = getConfig();
    const options = { projectId };
    if (keyFile) {
        options.keyFilename = keyFile;
    }
    cachedClient = new bigquery_1.BigQuery(options);
    return cachedClient;
}
