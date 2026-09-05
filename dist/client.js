"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateIdentifier = validateIdentifier;
exports.getConfig = getConfig;
exports.extractDatasets = extractDatasets;
exports.resolveLocation = resolveLocation;
exports.getBigQueryClient = getBigQueryClient;
const bigquery_1 = require("@google-cloud/bigquery");
let cachedClient = null;
const SAFE_IDENTIFIER = /^[a-zA-Z0-9_][a-zA-Z0-9_.\-]{0,1023}$/;
function validateIdentifier(value, label) {
    if (!SAFE_IDENTIFIER.test(value)) {
        throw new Error(`Invalid ${label}: "${value}". Only alphanumeric characters, underscores, hyphens, and dots are allowed.`);
    }
}
/**
 * Parsed form of BIGQUERY_DATASET_LOCATIONS, cached against the raw env string
 * so repeated resolveLocation() calls stay allocation-free.
 */
let locationMapCache = null;
/**
 * Parse "dataset:location,project.dataset:location" into a lookup map.
 * Keys are stored lower-cased; a bare dataset name matches any project.
 */
function parseDatasetLocations(raw) {
    if (!raw)
        return new Map();
    if (locationMapCache && locationMapCache.raw === raw)
        return locationMapCache.map;
    const map = new Map();
    for (const entry of raw.split(",")) {
        const trimmed = entry.trim();
        if (!trimmed)
            continue;
        const sep = trimmed.lastIndexOf(":");
        if (sep < 1) {
            throw new Error(`Invalid BIGQUERY_DATASET_LOCATIONS entry: "${trimmed}". Expected "dataset:location" or "project.dataset:location".`);
        }
        const key = trimmed.slice(0, sep).trim().toLowerCase();
        const location = trimmed.slice(sep + 1).trim();
        if (!key || !location) {
            throw new Error(`Invalid BIGQUERY_DATASET_LOCATIONS entry: "${trimmed}". Both dataset and location must be non-empty.`);
        }
        map.set(key, location);
    }
    locationMapCache = { raw, map };
    return map;
}
function getConfig() {
    const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
    const projectId = process.env.BIGQUERY_PROJECT_ID;
    const defaultDataset = process.env.BIGQUERY_DEFAULT_DATASET;
    const location = process.env.BIGQUERY_LOCATION || "US";
    const ga4ProjectId = process.env.BIGQUERY_GA4_PROJECT;
    const ga4Location = process.env.BIGQUERY_GA4_LOCATION;
    const datasetLocations = parseDatasetLocations(process.env.BIGQUERY_DATASET_LOCATIONS);
    if (!projectId) {
        throw new Error("BIGQUERY_PROJECT_ID environment variable is required. Set it to your Google Cloud project ID.");
    }
    return {
        keyFile,
        projectId,
        defaultDataset,
        location,
        ga4ProjectId,
        ga4Location,
        datasetLocations,
    };
}
/**
 * Look up a single dataset in the configured location map.
 * Tries "project.dataset" first, then the bare dataset name.
 */
function lookupDatasetLocation(map, dataset, project) {
    const ds = dataset.toLowerCase();
    if (project) {
        const qualified = map.get(`${project.toLowerCase()}.${ds}`);
        if (qualified)
            return qualified;
    }
    return map.get(ds);
}
/**
 * Extract the datasets a SQL statement touches, so a query can be routed to the
 * location its data actually lives in.
 *
 * Handles backticked three-part (`project.dataset.table`) and two-part
 * (`dataset.table`) references as well as unquoted FROM/JOIN targets. Wildcard
 * suffixes (events_*), INFORMATION_SCHEMA and __TABLES__ are covered because only
 * the dataset segment is read.
 */
function extractDatasets(sql) {
    const found = new Set();
    const ident = "[A-Za-z0-9_\-]+";
    // Backticked references: `a.b.c` or `a.b`
    const backticked = new RegExp("`([^`]+)`", "g");
    for (const match of sql.matchAll(backticked)) {
        const parts = match[1].split(".").map((p) => p.trim()).filter(Boolean);
        if (parts.length >= 3) {
            found.add(parts[1]);
        }
        else if (parts.length === 2) {
            found.add(parts[0]);
        }
    }
    // Unquoted FROM/JOIN targets
    const unquoted = new RegExp(`\b(?:FROM|JOIN)\s+(${ident})\.(${ident})(?:\.(${ident}))?`, "gi");
    for (const match of sql.matchAll(unquoted)) {
        found.add(match[3] ? match[2] : match[1]);
    }
    return [...found];
}
/**
 * Resolve the correct BigQuery location for a job.
 *
 * Precedence:
 *   1. BIGQUERY_DATASET_LOCATIONS entry for a dataset the job touches
 *   2. BIGQUERY_GA4_LOCATION when the target project is the GA4 project
 *   3. BIGQUERY_LOCATION (default)
 *
 * If the job touches datasets that are mapped to different locations, this throws
 * instead of letting BigQuery fail with a confusing "Dataset not found in location"
 * error — a single query cannot span locations.
 */
function resolveLocation(targetProject, datasets) {
    const config = getConfig();
    const list = datasets === undefined ? [] : Array.isArray(datasets) ? datasets : [datasets];
    if (list.length > 0 && config.datasetLocations.size > 0) {
        const hits = new Map();
        for (const ds of list) {
            const loc = lookupDatasetLocation(config.datasetLocations, ds, targetProject);
            if (!loc)
                continue;
            if (!hits.has(loc))
                hits.set(loc, []);
            hits.get(loc).push(ds);
        }
        if (hits.size > 1) {
            const detail = [...hits.entries()]
                .map(([loc, dss]) => `${loc}: ${dss.join(", ")}`)
                .join(" | ");
            throw new Error(`Cross-location query: this statement touches datasets in different BigQuery locations (${detail}). ` +
                `A single query cannot span locations. Copy the datasets into one location first.`);
        }
        if (hits.size === 1) {
            return [...hits.keys()][0];
        }
    }
    if (config.ga4ProjectId &&
        config.ga4Location &&
        targetProject === config.ga4ProjectId) {
        return config.ga4Location;
    }
    return config.location;
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
