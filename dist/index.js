#!/usr/bin/env node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const mcp_js_1 = require("@modelcontextprotocol/sdk/server/mcp.js");
const stdio_js_1 = require("@modelcontextprotocol/sdk/server/stdio.js");
const zod_1 = require("zod");
const query_js_1 = require("./tools/query.js");
const list_datasets_js_1 = require("./tools/list-datasets.js");
const list_tables_js_1 = require("./tools/list-tables.js");
const describe_table_js_1 = require("./tools/describe-table.js");
const gsc_quick_wins_js_1 = require("./tools/gsc-quick-wins.js");
const gsc_content_decay_js_1 = require("./tools/gsc-content-decay.js");
const gsc_cannibalisation_js_1 = require("./tools/gsc-cannibalisation.js");
const gsc_traffic_drops_js_1 = require("./tools/gsc-traffic-drops.js");
const server = new mcp_js_1.McpServer({
    name: "bigquery-mcp",
    version: "1.0.0",
});
// 1. Query
server.tool("query", "Run a SQL query against BigQuery and return results. Only SELECT queries are allowed. Claude should use list_datasets, list_tables, and describe_table first to understand the schema before writing queries.", {
    sql: zod_1.z.string().describe("The SQL query to execute. Only SELECT statements allowed."),
    max_rows: zod_1.z.number().default(100).describe("Maximum rows to return (default 100, max 10000)"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ sql, max_rows, project_id }) => {
    const capped = Math.min(max_rows, 10000);
    const results = await (0, query_js_1.runQuery)(sql, capped, project_id);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 2. List Datasets
server.tool("list_datasets", "List all datasets in the BigQuery project. Use this first to discover what data is available.", {
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ project_id }) => {
    const results = await (0, list_datasets_js_1.listDatasets)(project_id);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 3. List Tables
server.tool("list_tables", "List all tables in a BigQuery dataset with their schemas. Use this to understand what tables and columns are available before writing queries.", {
    dataset: zod_1.z.string().describe("Dataset name to list tables from"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ dataset, project_id }) => {
    const results = await (0, list_tables_js_1.listTables)(dataset, project_id);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 4. Describe Table
server.tool("describe_table", "Get detailed schema information for a specific BigQuery table including column names, types, descriptions, row count, size, partitioning, and clustering.", {
    dataset: zod_1.z.string().describe("Dataset name"),
    table: zod_1.z.string().describe("Table name"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ dataset, table, project_id }) => {
    const results = await (0, describe_table_js_1.describeTable)(dataset, table, project_id);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 5. GSC Quick Wins
server.tool("gsc_quick_wins", "Find keywords from GSC bulk export data at positions 4 to 15 with high impressions. These are striking distance keywords that could be pushed to page one. Requires GSC bulk data export to BigQuery.", {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(100).describe("Minimum impressions threshold"),
    max_position: zod_1.z.number().default(15).describe("Maximum position to include"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data (default: BIGQUERY_DEFAULT_DATASET env var or 'searchconsole')"),
}, async ({ days, min_impressions, max_position, dataset }) => {
    const results = await (0, gsc_quick_wins_js_1.gscQuickWins)(days, min_impressions, max_position, dataset);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 6. GSC Content Decay
server.tool("gsc_content_decay", "Find pages with consistent traffic decline over three consecutive months from GSC bulk export data. One bad month is noise; three is a problem. Requires GSC bulk data export to BigQuery.", {
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ dataset }) => {
    const results = await (0, gsc_content_decay_js_1.gscContentDecay)(dataset);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 7. GSC Cannibalisation
server.tool("gsc_cannibalisation", "Find keywords where multiple pages from your site compete against each other in GSC bulk export data. Shows which pages rank for the same query and their respective positions. Requires GSC bulk data export to BigQuery.", {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(50).describe("Minimum combined impressions for a query"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, dataset }) => {
    const results = await (0, gsc_cannibalisation_js_1.gscCannibalisation)(days, min_impressions, dataset);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
// 8. GSC Traffic Drops
server.tool("gsc_traffic_drops", "Find pages that lost the most traffic recently using GSC bulk export data. Compares current period vs prior period and diagnoses whether each drop is a ranking loss, CTR collapse, or demand decline. Requires GSC bulk data export to BigQuery.", {
    days: zod_1.z.number().default(28).describe("Number of days per comparison period"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, dataset }) => {
    const results = await (0, gsc_traffic_drops_js_1.gscTrafficDrops)(days, dataset);
    return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
    };
});
async function main() {
    const transport = new stdio_js_1.StdioServerTransport();
    await server.connect(transport);
    console.error("BigQuery MCP server running on stdio");
}
main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
});
