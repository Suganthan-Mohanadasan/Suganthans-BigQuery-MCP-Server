#!/usr/bin/env node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const mcp_js_1 = require("@modelcontextprotocol/sdk/server/mcp.js");
const stdio_js_1 = require("@modelcontextprotocol/sdk/server/stdio.js");
const zod_1 = require("zod");
const guardrails_js_1 = require("./guardrails.js");
const query_js_1 = require("./tools/query.js");
const list_datasets_js_1 = require("./tools/list-datasets.js");
const list_tables_js_1 = require("./tools/list-tables.js");
const describe_table_js_1 = require("./tools/describe-table.js");
const sample_rows_js_1 = require("./tools/sample-rows.js");
const gsc_quick_wins_js_1 = require("./tools/gsc-quick-wins.js");
const gsc_content_decay_js_1 = require("./tools/gsc-content-decay.js");
const gsc_cannibalisation_js_1 = require("./tools/gsc-cannibalisation.js");
const gsc_traffic_drops_js_1 = require("./tools/gsc-traffic-drops.js");
const gsc_ctr_opportunities_js_1 = require("./tools/gsc-ctr-opportunities.js");
const gsc_content_gaps_js_1 = require("./tools/gsc-content-gaps.js");
const gsc_site_snapshot_js_1 = require("./tools/gsc-site-snapshot.js");
const gsc_topic_cluster_js_1 = require("./tools/gsc-topic-cluster.js");
const gsc_ctr_benchmark_js_1 = require("./tools/gsc-ctr-benchmark.js");
const gsc_alerts_js_1 = require("./tools/gsc-alerts.js");
const gsc_content_recommendations_js_1 = require("./tools/gsc-content-recommendations.js");
const gsc_report_js_1 = require("./tools/gsc-report.js");
const gsc_anonymous_traffic_js_1 = require("./tools/gsc-anonymous-traffic.js");
const gsc_seasonal_js_1 = require("./tools/gsc-seasonal.js");
const gsc_device_split_js_1 = require("./tools/gsc-device-split.js");
const gsc_intent_breakdown_js_1 = require("./tools/gsc-intent-breakdown.js");
const gsc_ngrams_js_1 = require("./tools/gsc-ngrams.js");
const gsc_new_keywords_js_1 = require("./tools/gsc-new-keywords.js");
const gsc_forecast_js_1 = require("./tools/gsc-forecast.js");
const gsc_anomalies_js_1 = require("./tools/gsc-anomalies.js");
const server = new mcp_js_1.McpServer({
    name: "bigquery-mcp",
    version: "3.0.0",
});
function errorResponse(error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
        content: [{ type: "text", text: `Error: ${message}` }],
        isError: true,
    };
}
// ============================================================
// GENERAL PURPOSE TOOLS (1-6)
// ============================================================
// 1. Query
server.tool("query", "Run a SQL query against BigQuery and return results. Only SELECT queries are allowed. A LIMIT clause is automatically added if missing. Claude should use list_datasets, list_tables, and describe_table first to understand the schema before writing queries." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    sql: zod_1.z.string().describe("The SQL query to execute. Only SELECT statements allowed."),
    max_rows: zod_1.z.number().default(100).describe("Maximum rows to return (default 100, max 10000)"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ sql, max_rows, project_id }) => {
    try {
        const capped = Math.min(max_rows, 10000);
        const results = await (0, query_js_1.runQuery)(sql, capped, project_id);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "query", { sql, max_rows: capped });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 2. Query Cost Estimate
server.tool("query_cost_estimate", "Dry-run a SQL query to see how many bytes it would scan without actually executing it. Use this before running expensive queries to check cost.", {
    sql: zod_1.z.string().describe("The SQL query to estimate cost for"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ sql, project_id }) => {
    try {
        const result = await (0, query_js_1.dryRunQuery)(sql, project_id);
        const costEstimate = (result.bytesRaw / (1024 * 1024 * 1024 * 1024)) * 6.25;
        return {
            content: [{
                    type: "text",
                    text: JSON.stringify({
                        bytesProcessed: result.bytesProcessed,
                        estimatedCostUSD: `$${costEstimate.toFixed(4)}`,
                        note: "Estimate based on on-demand pricing ($6.25/TB). Actual cost may differ with reservations or free tier.",
                    }, null, 2),
                }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 3. List Datasets
server.tool("list_datasets", "List all datasets in the BigQuery project. Use this first to discover what data is available.", {
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ project_id }) => {
    try {
        const results = await (0, list_datasets_js_1.listDatasets)(project_id);
        return {
            content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 4. List Tables
server.tool("list_tables", "List all tables in a BigQuery dataset with their schemas. Uses INFORMATION_SCHEMA for efficiency. Use this to understand what tables and columns are available before writing queries.", {
    dataset: zod_1.z.string().describe("Dataset name to list tables from"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ dataset, project_id }) => {
    try {
        const results = await (0, list_tables_js_1.listTables)(dataset, project_id);
        return {
            content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 5. Describe Table
server.tool("describe_table", "Get detailed schema information for a specific BigQuery table including column names, types, descriptions, row count, size, partitioning, and clustering.", {
    dataset: zod_1.z.string().describe("Dataset name"),
    table: zod_1.z.string().describe("Table name"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ dataset, table, project_id }) => {
    try {
        const results = await (0, describe_table_js_1.describeTable)(dataset, table, project_id);
        return {
            content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 6. Sample Rows
server.tool("sample_rows", "Preview sample rows from a table without writing SQL. Useful for quickly understanding what data looks like. Limited to 1GB bytes billed.", {
    dataset: zod_1.z.string().describe("Dataset name"),
    table: zod_1.z.string().describe("Table name"),
    limit: zod_1.z.number().default(10).describe("Number of rows to return (default 10, max 100)"),
    project_id: zod_1.z.string().optional().describe("Override the default project ID"),
}, async ({ dataset, table, limit, project_id }) => {
    try {
        const results = await (0, sample_rows_js_1.sampleRows)(dataset, table, limit, project_id);
        return {
            content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// ============================================================
// GSC ANALYSIS TOOLS (7-18)
// ============================================================
// 7. GSC Quick Wins
server.tool("gsc_quick_wins", "Find keywords from GSC bulk export data at positions 4 to 15 with high impressions. These are striking distance keywords that could be pushed to page one. Sorted by traffic opportunity." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(100).describe("Minimum impressions threshold"),
    max_position: zod_1.z.number().default(15).describe("Maximum position to include"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, max_position, dataset }) => {
    try {
        const results = await (0, gsc_quick_wins_js_1.gscQuickWins)(days, min_impressions, max_position, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_quick_wins", { days, min_impressions, max_position });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 8. GSC CTR Opportunities
server.tool("gsc_ctr_opportunities", "Find pages with high impressions but CTR significantly below the expected benchmark for their ranking position. These are title and meta description optimisation candidates." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(500).describe("Minimum impressions threshold"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, dataset }) => {
    try {
        const results = await (0, gsc_ctr_opportunities_js_1.gscCtrOpportunities)(days, min_impressions, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_ctr_opportunities", { days, min_impressions });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 9. GSC Content Gaps
server.tool("gsc_content_gaps", "Find topics you should create content for. Returns queries where you get impressions but rank beyond position 20, meaning there is search demand but no real content targeting it." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(90).describe("Number of days to analyse (longer periods capture more gaps)"),
    min_impressions: zod_1.z.number().default(50).describe("Minimum impressions threshold"),
    min_position: zod_1.z.number().default(20).describe("Minimum position (queries ranking worse than this)"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, min_position, dataset }) => {
    try {
        const results = await (0, gsc_content_gaps_js_1.gscContentGaps)(days, min_impressions, min_position, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_content_gaps", { days, min_impressions, min_position });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 10. GSC Site Snapshot
server.tool("gsc_site_snapshot", "Get a quick overview of how the site is performing. Returns total clicks, impressions, CTR, position, unique pages and queries with a comparison to the prior period." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days per period"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, dataset }) => {
    try {
        const results = await (0, gsc_site_snapshot_js_1.gscSiteSnapshot)(days, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_site_snapshot", { days });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 11. GSC Content Decay
server.tool("gsc_content_decay", "Find pages with consistent traffic decline over three consecutive months from GSC bulk export data. One bad month is noise; three is a problem." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ dataset }) => {
    try {
        const results = await (0, gsc_content_decay_js_1.gscContentDecay)(dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_content_decay", {});
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 12. GSC Cannibalisation
server.tool("gsc_cannibalisation", "Find keywords where multiple pages from your site compete against each other. Shows which pages rank for the same query and their respective positions." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(50).describe("Minimum combined impressions for a query"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, dataset }) => {
    try {
        const results = await (0, gsc_cannibalisation_js_1.gscCannibalisation)(days, min_impressions, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_cannibalisation", { days, min_impressions });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 13. GSC Traffic Drops
server.tool("gsc_traffic_drops", "Find pages that lost the most traffic recently. Compares current period vs prior period and diagnoses whether each drop is a ranking loss, CTR collapse, or demand decline." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days per comparison period"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, dataset }) => {
    try {
        const results = await (0, gsc_traffic_drops_js_1.gscTrafficDrops)(days, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_traffic_drops", { days });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 14. GSC Topic Cluster Performance
server.tool("gsc_topic_cluster", "See how a group of pages performs as a whole. Aggregates clicks, impressions, CTR, and position for all pages matching a URL path pattern, plus top pages and queries." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    url_pattern: zod_1.z.string().describe("URL path pattern to match (e.g. /blog/seo)"),
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ url_pattern, days, dataset }) => {
    try {
        const results = await (0, gsc_topic_cluster_js_1.gscTopicCluster)(url_pattern, days, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_topic_cluster", { url_pattern, days });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 15. GSC CTR vs Benchmark
server.tool("gsc_ctr_benchmark", "Compare your actual CTR per page against industry benchmarks by position. Flags pages significantly underperforming for their ranking position with verdicts." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_impressions: zod_1.z.number().default(200).describe("Minimum impressions threshold"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_impressions, dataset }) => {
    try {
        const results = await (0, gsc_ctr_benchmark_js_1.gscCtrBenchmark)(days, min_impressions, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_ctr_benchmark", { days, min_impressions });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 16. GSC Alerts
server.tool("gsc_alerts", "Check for SEO alerts: position drops, CTR collapses, click losses, and pages that disappeared from search results. Returns severity-rated alerts so you know what needs attention first." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(7).describe("Number of days per period to compare"),
    position_drop_threshold: zod_1.z.number().default(20).describe("Alert if position drops more than this many spots"),
    ctr_drop_pct: zod_1.z.number().default(50).describe("Alert if CTR drops more than this percentage"),
    click_drop_pct: zod_1.z.number().default(30).describe("Alert if clicks drop more than this percentage"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, position_drop_threshold, ctr_drop_pct, click_drop_pct, dataset }) => {
    try {
        const results = await (0, gsc_alerts_js_1.gscAlerts)(days, position_drop_threshold, ctr_drop_pct, click_drop_pct, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_alerts", { days, position_drop_threshold, ctr_drop_pct, click_drop_pct });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 17. GSC Content Recommendations
server.tool("gsc_content_recommendations", "Get actionable content recommendations by cross-referencing quick wins, content gaps, and cannibalisation data. Returns prioritised actions: pages to update, content to create, and pages to consolidate." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    max_recommendations: zod_1.z.number().default(10).describe("Maximum number of recommendations"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, max_recommendations, dataset }) => {
    try {
        const results = await (0, gsc_content_recommendations_js_1.gscContentRecommendations)(days, max_recommendations, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_content_recommendations", { days, max_recommendations });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 18. GSC Report
server.tool("gsc_report", "Generate a comprehensive markdown performance report. Covers site snapshot, alerts, quick wins, traffic drops, content decay, and recommendations. Returns the full report as markdown." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    include_sections: zod_1.z.array(zod_1.z.string()).optional().describe("Sections: snapshot, alerts, quick_wins, traffic_drops, content_decay, recommendations"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, include_sections, dataset }) => {
    try {
        const results = await (0, gsc_report_js_1.gscReport)(days, include_sections, dataset);
        return {
            content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// ============================================================
// BIGQUERY-EXCLUSIVE TOOLS (19-26)
// These use capabilities only possible with BigQuery bulk export
// ============================================================
// 19. GSC Anonymous Traffic
server.tool("gsc_anonymous_traffic", "Analyse anonymous (hidden) query traffic that the GSC API cannot show. Reveals what percentage of your clicks come from queries Google redacts, and which pages get the most hidden traffic. Only possible with BigQuery bulk export." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, dataset }) => {
    try {
        const results = await (0, gsc_anonymous_traffic_js_1.gscAnonymousTraffic)(days, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_anonymous_traffic", { days });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 20. GSC Seasonal Analysis
server.tool("gsc_seasonal", "Year-over-year seasonal traffic analysis. Shows monthly clicks, impressions, CTR, and position with YoY comparison. Requires 12+ months of BigQuery data. Impossible with the 16-month rolling GSC API." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ dataset }) => {
    try {
        const results = await (0, gsc_seasonal_js_1.gscSeasonal)(dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_seasonal", {});
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 21. GSC Device Split
server.tool("gsc_device_split", "Find queries where mobile and desktop rank different pages from your site. This device cannibalisation is invisible in the GSC UI and impossible to detect via the API's 3-dimension limit." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_clicks: zod_1.z.number().default(5).describe("Minimum clicks threshold"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_clicks, dataset }) => {
    try {
        const results = await (0, gsc_device_split_js_1.gscDeviceSplit)(days, min_clicks, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_device_split", { days, min_clicks });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 22. GSC Intent Breakdown
server.tool("gsc_intent_breakdown", "Classify all your ranking queries by search intent (informational, transactional, commercial, navigational) using regex pattern matching at scale. Shows clicks, impressions, and CTR by intent category." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, dataset }) => {
    try {
        const results = await (0, gsc_intent_breakdown_js_1.gscIntentBreakdown)(days, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_intent_breakdown", { days });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 23. GSC N-Grams
server.tool("gsc_ngrams", "Extract the most common meaningful terms across your entire query set, ranked by clicks. A lightweight alternative to keyword clustering that reveals emerging topics and content themes." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    days: zod_1.z.number().default(28).describe("Number of days to analyse"),
    min_query_count: zod_1.z.number().default(5).describe("Minimum number of queries a term must appear in"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ days, min_query_count, dataset }) => {
    try {
        const results = await (0, gsc_ngrams_js_1.gscNgrams)(days, min_query_count, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_ngrams", { days, min_query_count });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 24. GSC New Keywords
server.tool("gsc_new_keywords", "Discover queries that appeared in your recent data but were not present in the baseline period. Useful for spotting new ranking opportunities, trending topics, or the impact of recently published content." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    recent_days: zod_1.z.number().default(7).describe("Number of recent days to check"),
    baseline_days: zod_1.z.number().default(60).describe("Number of days for the baseline comparison period"),
    min_impressions: zod_1.z.number().default(10).describe("Minimum impressions in recent period"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ recent_days, baseline_days, min_impressions, dataset }) => {
    try {
        const results = await (0, gsc_new_keywords_js_1.gscNewKeywords)(recent_days, baseline_days, min_impressions, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_new_keywords", { recent_days, baseline_days, min_impressions });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 25. GSC Forecast
server.tool("gsc_forecast", "Forecast organic traffic using BigQuery ML ARIMA_PLUS. Trains a time-series model on your historical click data and projects future clicks with confidence intervals. Requires sufficient historical data (ideally 6+ months). This is only possible with BigQuery ML." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    horizon: zod_1.z.number().default(30).describe("Number of days to forecast (default 30, max 365)"),
    confidence_level: zod_1.z.number().default(0.95).describe("Confidence level for prediction intervals (0.80 to 0.99)"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ horizon, confidence_level, dataset }) => {
    try {
        const results = await (0, gsc_forecast_js_1.gscForecast)(horizon, confidence_level, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_forecast", { horizon, confidence_level });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
// 26. GSC Anomalies
server.tool("gsc_anomalies", "Detect traffic anomalies using BigQuery ML. Unlike threshold-based alerts, this understands seasonality and weekly patterns, so it only flags genuinely unexpected traffic changes. Requires sufficient historical data (ideally 6+ months)." + guardrails_js_1.GUARDRAIL_SUFFIX, {
    anomaly_threshold: zod_1.z.number().default(0.95).describe("Anomaly probability threshold (0.80 to 0.99, higher = fewer but more significant anomalies)"),
    dataset: zod_1.z.string().optional().describe("BigQuery dataset containing GSC data"),
}, async ({ anomaly_threshold, dataset }) => {
    try {
        const results = await (0, gsc_anomalies_js_1.gscAnomalies)(14, anomaly_threshold, dataset);
        const wrapped = (0, guardrails_js_1.withMeta)(results, "gsc_anomalies", { anomaly_threshold });
        return {
            content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
        };
    }
    catch (error) {
        return errorResponse(error);
    }
});
async function main() {
    const transport = new stdio_js_1.StdioServerTransport();
    await server.connect(transport);
    console.error("BigQuery MCP server v3.0.0 running on stdio (26 tools)");
}
main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
});
