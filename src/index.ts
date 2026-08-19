#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { GUARDRAIL_SUFFIX, VISUAL_SUFFIX, withMeta } from "./guardrails.js";
import { runQuery, dryRunQuery } from "./tools/query.js";
import { listDatasets } from "./tools/list-datasets.js";
import { listTables } from "./tools/list-tables.js";
import { describeTable } from "./tools/describe-table.js";
import { sampleRows } from "./tools/sample-rows.js";
import { gscQuickWins } from "./tools/gsc-quick-wins.js";
import { gscContentDecay } from "./tools/gsc-content-decay.js";
import { gscCannibalisation } from "./tools/gsc-cannibalisation.js";
import { gscTrafficDrops } from "./tools/gsc-traffic-drops.js";
import { gscCtrOpportunities } from "./tools/gsc-ctr-opportunities.js";
import { gscContentGaps } from "./tools/gsc-content-gaps.js";
import { gscSiteSnapshot } from "./tools/gsc-site-snapshot.js";
import { gscTopicCluster } from "./tools/gsc-topic-cluster.js";
import { gscCtrBenchmark } from "./tools/gsc-ctr-benchmark.js";
import { gscAlerts } from "./tools/gsc-alerts.js";
import { gscContentRecommendations } from "./tools/gsc-content-recommendations.js";
import { gscReport } from "./tools/gsc-report.js";
import { gscAnonymousTraffic } from "./tools/gsc-anonymous-traffic.js";
import { gscSeasonal } from "./tools/gsc-seasonal.js";
import { gscDeviceSplit } from "./tools/gsc-device-split.js";
import { gscIntentBreakdown } from "./tools/gsc-intent-breakdown.js";
import { gscNgrams } from "./tools/gsc-ngrams.js";
import { gscNewKeywords } from "./tools/gsc-new-keywords.js";
import { gscForecast } from "./tools/gsc-forecast.js";
import { gscAnomalies } from "./tools/gsc-anomalies.js";
import { ga4GscPagePerformance } from "./tools/ga4-gsc-page-performance.js";
import { ga4GscQueryRevenue } from "./tools/ga4-gsc-query-revenue.js";
import { ga4GscContentRoi } from "./tools/ga4-gsc-content-roi.js";
import { ga4GscSnippetMismatch } from "./tools/ga4-gsc-snippet-mismatch.js";
import { ga4GscPositionValue } from "./tools/ga4-gsc-position-value.js";
import { ga4GscBrandedPerformance } from "./tools/ga4-gsc-branded-performance.js";
// v4.1 generative AI tools — AI Mode conversation exhaust in the query table.
import { gscGenaiConversationQueries } from "./tools/gsc-genai-conversation-queries.js";
import { gscQueryCount } from "./tools/gsc-query-count.js";
import { gscDiscover } from "./tools/gsc-discover.js";
import { gscClickCurve } from "./tools/gsc-click-curve.js";
import { gscShopping } from "./tools/gsc-shopping.js";
import { gscImageSearch } from "./tools/gsc-image-search.js";

const server = new McpServer({
  name: "bigquery-mcp",
  version: "4.3.0",
});

function errorResponse(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return {
    content: [{ type: "text" as const, text: `Error: ${message}` }],
    isError: true as const,
  };
}

// ============================================================
// GENERAL PURPOSE TOOLS (1-6)
// ============================================================

// 1. Query
server.tool(
  "query",
  "Run a SQL query against BigQuery and return results. Only SELECT queries are allowed. A LIMIT clause is automatically added if missing. Claude should use list_datasets, list_tables, and describe_table first to understand the schema before writing queries." + GUARDRAIL_SUFFIX,
  {
    sql: z.string().describe("The SQL query to execute. Only SELECT statements allowed."),
    max_rows: z.number().default(100).describe("Maximum rows to return (default 100, max 10000)"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ sql, max_rows, project_id }) => {
    try {
      const capped = Math.min(max_rows, 10000);
      const results = await runQuery(sql, capped, project_id);
      const wrapped = withMeta(results, "query", { sql, max_rows: capped });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 2. Query Cost Estimate
server.tool(
  "query_cost_estimate",
  "Dry-run a SQL query to see how many bytes it would scan without actually executing it. Use this before running expensive queries to check cost.",
  {
    sql: z.string().describe("The SQL query to estimate cost for"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ sql, project_id }) => {
    try {
      const result = await dryRunQuery(sql, project_id);
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
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 3. List Datasets
server.tool(
  "list_datasets",
  "List all datasets in the BigQuery project. Use this first to discover what data is available.",
  {
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ project_id }) => {
    try {
      const results = await listDatasets(project_id);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 4. List Tables
server.tool(
  "list_tables",
  "List all tables in a BigQuery dataset with their schemas. Uses INFORMATION_SCHEMA for efficiency. Use this to understand what tables and columns are available before writing queries.",
  {
    dataset: z.string().describe("Dataset name to list tables from"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ dataset, project_id }) => {
    try {
      const results = await listTables(dataset, project_id);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 5. Describe Table
server.tool(
  "describe_table",
  "Get detailed schema information for a specific BigQuery table including column names, types, descriptions, row count, size, partitioning, and clustering.",
  {
    dataset: z.string().describe("Dataset name"),
    table: z.string().describe("Table name"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ dataset, table, project_id }) => {
    try {
      const results = await describeTable(dataset, table, project_id);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 6. Sample Rows
server.tool(
  "sample_rows",
  "Preview sample rows from a table without writing SQL. Useful for quickly understanding what data looks like. Limited to 1GB bytes billed.",
  {
    dataset: z.string().describe("Dataset name"),
    table: z.string().describe("Table name"),
    limit: z.number().default(10).describe("Number of rows to return (default 10, max 100)"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ dataset, table, limit, project_id }) => {
    try {
      const results = await sampleRows(dataset, table, limit, project_id);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// ============================================================
// GSC ANALYSIS TOOLS (7-18)
// ============================================================

// 7. GSC Quick Wins
server.tool(
  "gsc_quick_wins",
  "Find keywords from GSC bulk export data at positions 4 to 15 with high impressions. These are striking distance keywords that could be pushed to page one. Sorted by traffic opportunity." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(100).describe("Minimum impressions threshold"),
    max_position: z.number().default(15).describe("Maximum position to include"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, max_position, dataset }) => {
    try {
      const results = await gscQuickWins(days, min_impressions, max_position, dataset);
      const wrapped = withMeta(results, "gsc_quick_wins", { days, min_impressions, max_position });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 8. GSC CTR Opportunities
server.tool(
  "gsc_ctr_opportunities",
  "Find pages with high impressions but CTR significantly below the expected benchmark for their ranking position. These are title and meta description optimisation candidates." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(500).describe("Minimum impressions threshold"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, dataset }) => {
    try {
      const results = await gscCtrOpportunities(days, min_impressions, dataset);
      const wrapped = withMeta(results, "gsc_ctr_opportunities", { days, min_impressions });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 9. GSC Content Gaps
server.tool(
  "gsc_content_gaps",
  "Find topics you should create content for. Returns queries where you get impressions but rank beyond position 20, meaning there is search demand but no real content targeting it." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(90).describe("Number of days to analyse (longer periods capture more gaps)"),
    min_impressions: z.number().default(50).describe("Minimum impressions threshold"),
    min_position: z.number().default(20).describe("Minimum position (queries ranking worse than this)"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, min_position, dataset }) => {
    try {
      const results = await gscContentGaps(days, min_impressions, min_position, dataset);
      const wrapped = withMeta(results, "gsc_content_gaps", { days, min_impressions, min_position });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 10. GSC Site Snapshot
server.tool(
  "gsc_site_snapshot",
  "Get a quick overview of how the site is performing. Returns total clicks, impressions, CTR, position, unique pages and queries with a comparison to the prior period." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days per period"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, dataset }) => {
    try {
      const results = await gscSiteSnapshot(days, dataset);
      const wrapped = withMeta(results, "gsc_site_snapshot", { days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 11. GSC Content Decay
server.tool(
  "gsc_content_decay",
  "Find pages with consistent traffic decline over three consecutive months from GSC bulk export data. One bad month is noise; three is a problem." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ dataset }) => {
    try {
      const results = await gscContentDecay(dataset);
      const wrapped = withMeta(results, "gsc_content_decay", {});
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 12. GSC Cannibalisation
server.tool(
  "gsc_cannibalisation",
  "Find keywords where multiple pages from your site compete against each other. Shows which pages rank for the same query and their respective positions." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(50).describe("Minimum combined impressions for a query"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, dataset }) => {
    try {
      const results = await gscCannibalisation(days, min_impressions, dataset);
      const wrapped = withMeta(results, "gsc_cannibalisation", { days, min_impressions });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 13. GSC Traffic Drops
server.tool(
  "gsc_traffic_drops",
  "Find pages that lost the most traffic recently. Compares current period vs prior period and diagnoses whether each drop is a ranking loss, CTR collapse, or demand decline." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days per comparison period"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, dataset }) => {
    try {
      const results = await gscTrafficDrops(days, dataset);
      const wrapped = withMeta(results, "gsc_traffic_drops", { days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 14. GSC Topic Cluster Performance
server.tool(
  "gsc_topic_cluster",
  "See how a group of pages performs as a whole. Aggregates clicks, impressions, CTR, and position for all pages matching a URL path pattern, plus top pages and queries." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    url_pattern: z.string().describe("URL path pattern to match (e.g. /blog/seo)"),
    days: z.number().default(28).describe("Number of days to analyse"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ url_pattern, days, dataset }) => {
    try {
      const results = await gscTopicCluster(url_pattern, days, dataset);
      const wrapped = withMeta(results, "gsc_topic_cluster", { url_pattern, days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 15. GSC CTR vs Benchmark
server.tool(
  "gsc_ctr_benchmark",
  "Compare your actual CTR per page against industry benchmarks by position. Flags pages significantly underperforming for their ranking position with verdicts." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(200).describe("Minimum impressions threshold"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, dataset }) => {
    try {
      const results = await gscCtrBenchmark(days, min_impressions, dataset);
      const wrapped = withMeta(results, "gsc_ctr_benchmark", { days, min_impressions });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 16. GSC Alerts
server.tool(
  "gsc_alerts",
  "Check for SEO alerts: position drops, CTR collapses, click losses, and pages that disappeared from search results. Returns severity-rated alerts so you know what needs attention first." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(7).describe("Number of days per period to compare"),
    position_drop_threshold: z.number().default(20).describe("Alert if position drops more than this many spots"),
    ctr_drop_pct: z.number().default(50).describe("Alert if CTR drops more than this percentage"),
    click_drop_pct: z.number().default(30).describe("Alert if clicks drop more than this percentage"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, position_drop_threshold, ctr_drop_pct, click_drop_pct, dataset }) => {
    try {
      const results = await gscAlerts(days, position_drop_threshold, ctr_drop_pct, click_drop_pct, dataset);
      const wrapped = withMeta(results, "gsc_alerts", { days, position_drop_threshold, ctr_drop_pct, click_drop_pct });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 17. GSC Content Recommendations
server.tool(
  "gsc_content_recommendations",
  "Get actionable content recommendations by cross-referencing quick wins, content gaps, and cannibalisation data. Returns prioritised actions: pages to update, content to create, and pages to consolidate." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    max_recommendations: z.number().default(10).describe("Maximum number of recommendations"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, max_recommendations, dataset }) => {
    try {
      const results = await gscContentRecommendations(days, max_recommendations, dataset);
      const wrapped = withMeta(results, "gsc_content_recommendations", { days, max_recommendations });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 18. GSC Report
server.tool(
  "gsc_report",
  "Generate a comprehensive markdown performance report. Covers site snapshot, alerts, quick wins, traffic drops, content decay, and recommendations. Returns the full report as markdown." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    include_sections: z.array(z.string()).optional().describe("Sections: snapshot, alerts, quick_wins, traffic_drops, content_decay, recommendations"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, include_sections, dataset }) => {
    try {
      const results = await gscReport(days, include_sections, dataset);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// ============================================================
// BIGQUERY-EXCLUSIVE TOOLS (19-26)
// These use capabilities only possible with BigQuery bulk export
// ============================================================

// 19. GSC Anonymous Traffic
server.tool(
  "gsc_anonymous_traffic",
  "Analyse anonymous (hidden) query traffic that the GSC API cannot show. Reveals what percentage of your clicks come from queries Google redacts, and which pages get the most hidden traffic. Only possible with BigQuery bulk export." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, dataset }) => {
    try {
      const results = await gscAnonymousTraffic(days, dataset);
      const wrapped = withMeta(results, "gsc_anonymous_traffic", { days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 20. GSC Seasonal Analysis
server.tool(
  "gsc_seasonal",
  "Year-over-year seasonal traffic analysis. Shows monthly clicks, impressions, CTR, and position with YoY comparison. Requires 12+ months of BigQuery data. Impossible with the 16-month rolling GSC API." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ dataset }) => {
    try {
      const results = await gscSeasonal(dataset);
      const wrapped = withMeta(results, "gsc_seasonal", {});
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 21. GSC Device Split
server.tool(
  "gsc_device_split",
  "Find queries where mobile and desktop rank different pages from your site. This device cannibalisation is invisible in the GSC UI and impossible to detect via the API's 3-dimension limit." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_clicks: z.number().default(5).describe("Minimum clicks threshold"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_clicks, dataset }) => {
    try {
      const results = await gscDeviceSplit(days, min_clicks, dataset);
      const wrapped = withMeta(results, "gsc_device_split", { days, min_clicks });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 22. GSC Intent Breakdown
server.tool(
  "gsc_intent_breakdown",
  "Classify all your ranking queries by search intent (informational, transactional, commercial, navigational) using regex pattern matching at scale. Shows clicks, impressions, and CTR by intent category." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, dataset }) => {
    try {
      const results = await gscIntentBreakdown(days, dataset);
      const wrapped = withMeta(results, "gsc_intent_breakdown", { days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 23. GSC N-Grams
server.tool(
  "gsc_ngrams",
  "Extract the most common meaningful terms across your entire query set, ranked by clicks. A lightweight alternative to keyword clustering that reveals emerging topics and content themes." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_query_count: z.number().default(5).describe("Minimum number of queries a term must appear in"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_query_count, dataset }) => {
    try {
      const results = await gscNgrams(days, min_query_count, dataset);
      const wrapped = withMeta(results, "gsc_ngrams", { days, min_query_count });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 24. GSC New Keywords
server.tool(
  "gsc_new_keywords",
  "Discover queries that appeared in your recent data but were not present in the baseline period. Useful for spotting new ranking opportunities, trending topics, or the impact of recently published content." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    recent_days: z.number().default(7).describe("Number of recent days to check"),
    baseline_days: z.number().default(60).describe("Number of days for the baseline comparison period"),
    min_impressions: z.number().default(10).describe("Minimum impressions in recent period"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ recent_days, baseline_days, min_impressions, dataset }) => {
    try {
      const results = await gscNewKeywords(recent_days, baseline_days, min_impressions, dataset);
      const wrapped = withMeta(results, "gsc_new_keywords", { recent_days, baseline_days, min_impressions });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 25. GSC Forecast
server.tool(
  "gsc_forecast",
  "Forecast organic traffic using BigQuery ML ARIMA_PLUS. Trains a time-series model on your historical click data and projects future clicks with confidence intervals. Requires sufficient historical data (ideally 6+ months). This is only possible with BigQuery ML." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    horizon: z.number().default(30).describe("Number of days to forecast (default 30, max 365)"),
    confidence_level: z.number().default(0.95).describe("Confidence level for prediction intervals (0.80 to 0.99)"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ horizon, confidence_level, dataset }) => {
    try {
      const results = await gscForecast(horizon, confidence_level, dataset);
      const wrapped = withMeta(results, "gsc_forecast", { horizon, confidence_level });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 26. GSC Anomalies
server.tool(
  "gsc_anomalies",
  "Detect traffic anomalies using BigQuery ML. Unlike threshold-based alerts, this understands seasonality and weekly patterns, so it only flags genuinely unexpected traffic changes. Requires sufficient historical data (ideally 6+ months)." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    anomaly_threshold: z.number().default(0.95).describe("Anomaly probability threshold (0.80 to 0.99, higher = fewer but more significant anomalies)"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ anomaly_threshold, dataset }) => {
    try {
      const results = await gscAnomalies(14, anomaly_threshold, dataset);
      const wrapped = withMeta(results, "gsc_anomalies", { anomaly_threshold });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// ============================================================
// GA4 + GSC BLENDING TOOLS (27-32)
// Require both GA4 and GSC BigQuery exports
// ============================================================

const GA4_GUARDRAIL = " IMPORTANT: GA4 and GSC data are joined on normalised landing page URL. Join rates vary by site (typically 70-90%). Numbers may not match GA4 or GSC dashboards exactly due to URL normalisation, timezone differences (GSC uses Pacific Time, GA4 uses property timezone), and sampling. Report the join rate when relevant." + GUARDRAIL_SUFFIX;

// 27. GA4+GSC Page Performance
server.tool(
  "ga4_gsc_page_performance",
  "Landing pages with BOTH search performance (clicks, impressions, position from GSC) AND engagement data (sessions, engagement rate, conversions from GA4) side by side. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_clicks: z.number().default(10).describe("Minimum GSC clicks to include a page"),
    max_rows: z.number().default(50).describe("Maximum rows to return"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data (e.g. analytics_123456789)"),
  },
  async ({ days, min_clicks, max_rows, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscPagePerformance(days, min_clicks, max_rows, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_page_performance", { days, min_clicks });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 28. GA4+GSC Query Revenue Attribution
server.tool(
  "ga4_gsc_query_revenue",
  "Which search queries actually drive revenue and conversions? Uses proportional attribution: if a page gets clicks from 3 queries, revenue is split by click share. The 'revenue per keyword' metric SEOs have wanted for years. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_clicks: z.number().default(5).describe("Minimum clicks per query"),
    max_rows: z.number().default(50).describe("Maximum rows to return"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data"),
  },
  async ({ days, min_clicks, max_rows, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscQueryRevenue(days, min_clicks, max_rows, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_query_revenue", { days, min_clicks });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 29. GA4+GSC Content ROI
server.tool(
  "ga4_gsc_content_roi",
  "Find pages that rank well but don't convert (fix the page, not the SEO) and pages that convert brilliantly but have low rankings (invest in SEO, the payoff is proven). Diagnoses each page. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_clicks: z.number().default(20).describe("Minimum GSC clicks to include"),
    max_rows: z.number().default(50).describe("Maximum rows to return"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data"),
  },
  async ({ days, min_clicks, max_rows, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscContentRoi(days, min_clicks, max_rows, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_content_roi", { days, min_clicks });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 30. GA4+GSC Snippet Mismatch
server.tool(
  "ga4_gsc_snippet_mismatch",
  "Find pages where SERP snippet performance doesn't match on-site engagement. High CTR + low engagement = misleading title/description. Low CTR + high engagement = great content with a bad snippet. Both are fixable. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_clicks: z.number().default(20).describe("Minimum GSC clicks to include"),
    max_rows: z.number().default(50).describe("Maximum rows to return"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data"),
  },
  async ({ days, min_clicks, max_rows, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscSnippetMismatch(days, min_clicks, max_rows, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_snippet_mismatch", { days, min_clicks });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 31. GA4+GSC Position Value
server.tool(
  "ga4_gsc_position_value",
  "What is each ranking position worth in revenue and conversions for YOUR site? Shows conversion rate and revenue per click by position bucket (1, 2-3, 4-5, 6-10, 11-20, 20+). Uses 90 days by default for statistical significance. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    days: z.number().default(90).describe("Number of days to analyse (longer = more reliable)"),
    max_rows: z.number().default(20).describe("Maximum rows to return"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data"),
  },
  async ({ days, max_rows, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscPositionValue(days, max_rows, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_position_value", { days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 32. GA4+GSC Branded vs Non-Branded Performance
server.tool(
  "ga4_gsc_branded_performance",
  "Compare branded vs non-branded organic traffic with engagement and conversion overlay from GA4. Shows how each traffic type performs across clicks, CTR, engagement rate, conversions, and revenue. Requires GA4 BigQuery export." + GA4_GUARDRAIL,
  {
    brand_terms: z.string().describe("Comma-separated brand terms, e.g. 'suganthan,snippet digital,keyword insights'"),
    days: z.number().default(28).describe("Number of days to analyse"),
    gsc_dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
    ga4_dataset: z.string().optional().describe("BigQuery dataset containing GA4 data"),
  },
  async ({ brand_terms, days, gsc_dataset, ga4_dataset }) => {
    try {
      const results = await ga4GscBrandedPerformance(brand_terms, days, gsc_dataset, ga4_dataset);
      const wrapped = withMeta(results, "ga4_gsc_branded_performance", { brand_terms, days });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// Generative AI: conversation exhaust detector (BigQuery twin of the GSC MCP tool)
server.tool(
  "gsc_genai_conversation_queries",
  "Surface AI-conversation exhaust hiding in your GSC query data: bare replies to Google's AI ('yes', 'go on'), 'what about X' pivot follow-ups, conversational questions, AI-visibility tracker probes, and full agent prompts logged as queries. Google counts every AI Mode follow-up as a new query, so these fragments carry real impressions, positions and clicks. Runs on the bulk export, so no API serving limits, plus the anonymised split: how many impressions carry no query string at all, which is where most of the conversation iceberg sits. Seven classified buckets with landing pages and a monthly artefact timeline. Treat probe and harness buckets as machine traffic, not demand." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(365).describe("Days to analyse, anchored to the export's latest data date (clamped to available retention)"),
    min_impressions: z.number().default(1).describe("Minimum impressions for a query to be listed (single-impression rows are evidence, not noise)"),
    max_rows_per_bucket: z.number().default(50).describe("Maximum rows returned per bucket; totals always cover everything"),
    include_timeline: z.boolean().default(true).describe("Include the monthly artefact timeline over full retention (one extra query)"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, max_rows_per_bucket, include_timeline, dataset }) => {
    try {
      const results = await gscGenaiConversationQueries(days, min_impressions, max_rows_per_bucket, include_timeline, dataset);
      const wrapped = withMeta(results, "gsc_genai_conversation_queries", { days, min_impressions, max_rows_per_bucket, include_timeline, dataset });
      return {
        content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 34. GSC Query Counting
server.tool(
  "gsc_query_count",
  "Count how many distinct queries a property, a section or a single URL is visible for, split by position group (1-3, 4-10, 11-20, 21-50, 51+), against the previous period of equal length. Scope with url or url_contains, add a time series with granularity, narrow with min_position/max_position, rank pages with top_pages. Unlike the API version this counts the whole export instead of a 1,000-row page, and reads the anonymized share from is_anonymized_query rather than inferring it from a click gap. Ranges are anchored to the latest day in the export, not today." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    url: z.string().optional().describe("Count only queries for this exact URL"),
    url_contains: z.string().optional().describe("Count only queries for URLs containing this string, e.g. /ratgeber/"),
    granularity: z.enum(["none", "day", "week", "month"]).default("none").describe("Add a time series of distinct query counts"),
    min_position: z.number().optional().describe("Only count queries at this average position or worse (e.g. 4)"),
    max_position: z.number().optional().describe("Only count queries at this average position or better (e.g. 10)"),
    search_type: z.enum(["WEB", "IMAGE", "VIDEO", "NEWS", "GOOGLE_NEWS"]).default("WEB").describe("Surface to count. Discover has no queries; use gsc_discover."),
    top_pages: z.number().optional().describe("Also rank this many pages by query count"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, url, url_contains, granularity, min_position, max_position, search_type, top_pages, dataset }) => {
    try {
      const results = await gscQueryCount(days, url, url_contains, granularity, min_position, max_position, search_type, top_pages, dataset);
      const wrapped = withMeta(results, "gsc_query_count", { days, url, url_contains, granularity, min_position, max_position, search_type, top_pages, dataset });
      return { content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }] };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 35. GSC Discover
server.tool(
  "gsc_discover",
  "Google Discover performance from the export: clicks, impressions, CTR, its share of all surfaces, a time series, top URLs, device and country split. Discover is page-based, so nothing groups by query, and its anonymisation is tracked separately via is_anonymized_discover." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    granularity: z.enum(["none", "day", "week", "month"]).default("week").describe("Bucket size for the time series"),
    url_contains: z.string().optional().describe("Restrict to URLs containing this string"),
    top_urls: z.number().default(50).describe("How many top URLs to return"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, granularity, url_contains, top_urls, dataset }) => {
    try {
      const results = await gscDiscover(days, granularity, url_contains, top_urls, dataset);
      const wrapped = withMeta(results, "gsc_discover", { days, granularity, url_contains, top_urls, dataset });
      return { content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }] };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 36. GSC Click Curve
server.tool(
  "gsc_click_curve",
  "Build the click curve from your own data: CTR per ranking position, measured instead of borrowed from a study. Aggregates to (url, query) pairs, takes each pair's average position, rounds it to a rank, then divides summed clicks by summed impressions per rank. Segment by device, country, search_type, or branded vs non-branded with a brand_pattern - the branded split matters most, because branded queries inflate a blended curve at the top. Also reports how many clicks the curve cannot cover, because anonymized rows carry no position." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(90).describe("Number of days to analyse. Longer is better here: the curve needs volume per rank."),
    max_position: z.number().default(20).describe("Highest rank to include"),
    min_impressions_per_rank: z.number().default(100).describe("Drop ranks below this many impressions instead of reporting noise"),
    segment_by: z.enum(["none", "device", "country", "search_type", "branded"]).default("none").describe("Split the curve by this dimension"),
    brand_pattern: z.string().optional().describe("Regex for branded queries, required when segment_by is branded, e.g. homeandsmart"),
    search_type: z.enum(["WEB", "IMAGE", "VIDEO", "NEWS", "GOOGLE_NEWS"]).default("WEB").describe("Surface to measure. Ignored when segment_by is search_type."),
    url_contains: z.string().optional().describe("Restrict to URLs containing this string, e.g. to get a curve for one section"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, max_position, min_impressions_per_rank, segment_by, brand_pattern, search_type, url_contains, dataset }) => {
    try {
      const results = await gscClickCurve(days, max_position, min_impressions_per_rank, segment_by, brand_pattern, search_type, url_contains, dataset);
      const wrapped = withMeta(results, "gsc_click_curve", { days, max_position, min_impressions_per_rank, segment_by, brand_pattern, search_type, url_contains, dataset });
      return { content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }] };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 37. GSC Organic Shopping / Free Listings
server.tool(
  "gsc_shopping",
  "Organic shopping surfaces: free product listings (is_organic_shopping), merchant listings (is_merchant_listings) and product snippets (is_product_snippets). These are search appearances inside WEB rows, so unlike the API searchAppearance dimension they can be crossed with url, query, device and date freely. Without an appearance argument it returns all three side by side; pass one to drill into its top URLs, top queries and time series. A property that sells nothing returns zeros - that is a finding, and the note field says so." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    appearance: z.enum(["organic_shopping", "merchant_listings", "product_snippets"]).optional().describe("Drill into one appearance. Omit for the overview of all three."),
    granularity: z.enum(["none", "day", "week", "month"]).default("week").describe("Bucket size for the drilldown time series"),
    url_contains: z.string().optional().describe("Restrict to URLs containing this string"),
    top_rows: z.number().default(50).describe("How many URLs and queries to return in the drilldown"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, appearance, granularity, url_contains, top_rows, dataset }) => {
    try {
      const results = await gscShopping(days, appearance, granularity, url_contains, top_rows, dataset);
      const wrapped = withMeta(results, "gsc_shopping", { days, appearance, granularity, url_contains, top_rows, dataset });
      return { content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }] };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 38. GSC Image Search
server.tool(
  "gsc_image_search",
  "Google Images performance from the export: clicks, impressions, CTR, average position, share of all surfaces, time series, top pages, top queries, device and country split, plus the AMP-image-result slice. Unlike Discover, image search does carry queries. Note that url is the page hosting the image, not the image file - the export has no image-level dimension." + GUARDRAIL_SUFFIX + VISUAL_SUFFIX,
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    granularity: z.enum(["none", "day", "week", "month"]).default("week").describe("Bucket size for the time series"),
    url_contains: z.string().optional().describe("Restrict to URLs containing this string"),
    top_rows: z.number().default(50).describe("How many pages and queries to return"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, granularity, url_contains, top_rows, dataset }) => {
    try {
      const results = await gscImageSearch(days, granularity, url_contains, top_rows, dataset);
      const wrapped = withMeta(results, "gsc_image_search", { days, granularity, url_contains, top_rows, dataset });
      return { content: [{ type: "text", text: JSON.stringify(wrapped, null, 2) }] };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("BigQuery MCP server v4.3.0 running on stdio (38 tools)");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
