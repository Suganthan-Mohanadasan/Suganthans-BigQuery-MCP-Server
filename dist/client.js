"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateIdentifier = validateIdentifier;
exports.getConfig = getConfig;
exports.getBigQueryClient = getBigQueryClient;
const bigquery_1 = require("@google-cloud/bigquery");
let cachedClient = null;
const SAFE_IDENTIFIER = /^[a-zA-Z0-9_][a-zA-Z0-9_.\-]{0,1023}$/;
function validateIdentifier(value, label) {
    if (!SAFE_IDENTIFIER.test(value)) {
        throw new Error(`Invalid ${label}: "${value}". Only alphanumeric characters, underscores, hyphens, and dots are allowed.`);
    }
}
function getConfig() {
    const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
    const projectId = process.env.BIGQUERY_PROJECT_ID;
    const defaultDataset = process.env.BIGQUERY_DEFAULT_DATASET;
    const location = process.env.BIGQUERY_LOCATION || "US";
    if (!projectId) {
        throw new Error("BIGQUERY_PROJECT_ID environment variable is required. Set it to your Google Cloud project ID.");
    }
    return { keyFile, projectId, defaultDataset, location };
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
