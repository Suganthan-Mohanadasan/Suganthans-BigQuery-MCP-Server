#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { runQuery, dryRunQuery } from "./tools/query.js";
import { listDatasets } from "./tools/list-datasets.js";
import { listTables } from "./tools/list-tables.js";
import { describeTable } from "./tools/describe-table.js";
import { sampleRows } from "./tools/sample-rows.js";
import { gscQuickWins } from "./tools/gsc-quick-wins.js";
import { gscContentDecay } from "./tools/gsc-content-decay.js";
import { gscCannibalisation } from "./tools/gsc-cannibalisation.js";
import { gscTrafficDrops } from "./tools/gsc-traffic-drops.js";

const server = new McpServer({
  name: "bigquery-mcp",
  version: "1.1.0",
});

function errorResponse(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return {
    content: [{ type: "text" as const, text: `Error: ${message}` }],
    isError: true as const,
  };
}

// 1. Query
server.tool(
  "query",
  "Run a SQL query against BigQuery and return results. Only SELECT queries are allowed. A LIMIT clause is automatically added if missing. Claude should use list_datasets, list_tables, and describe_table first to understand the schema before writing queries.",
  {
    sql: z.string().describe("The SQL query to execute. Only SELECT statements allowed."),
    max_rows: z.number().default(100).describe("Maximum rows to return (default 100, max 10000)"),
    project_id: z.string().optional().describe("Override the default project ID"),
  },
  async ({ sql, max_rows, project_id }) => {
    try {
      const capped = Math.min(max_rows, 10000);
      const results = await runQuery(sql, capped, project_id);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
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
      const costEstimate = (result.bytesRaw / (1024 * 1024 * 1024 * 1024)) * 6.25; // $6.25 per TB on-demand
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

// 7. GSC Quick Wins
server.tool(
  "gsc_quick_wins",
  "Find keywords from GSC bulk export data at positions 4 to 15 with high impressions. These are striking distance keywords that could be pushed to page one. Filters to web search only. Requires GSC bulk data export to BigQuery.",
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(100).describe("Minimum impressions threshold"),
    max_position: z.number().default(15).describe("Maximum position to include"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data (default: BIGQUERY_DEFAULT_DATASET env var or 'searchconsole')"),
  },
  async ({ days, min_impressions, max_position, dataset }) => {
    try {
      const results = await gscQuickWins(days, min_impressions, max_position, dataset);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 8. GSC Content Decay
server.tool(
  "gsc_content_decay",
  "Find pages with consistent traffic decline over three consecutive months from GSC bulk export data. One bad month is noise; three is a problem. Filters to web search only. Requires GSC bulk data export to BigQuery.",
  {
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ dataset }) => {
    try {
      const results = await gscContentDecay(dataset);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 9. GSC Cannibalisation
server.tool(
  "gsc_cannibalisation",
  "Find keywords where multiple pages from your site compete against each other in GSC bulk export data. Shows which pages rank for the same query and their respective positions. Filters to web search only. Requires GSC bulk data export to BigQuery.",
  {
    days: z.number().default(28).describe("Number of days to analyse"),
    min_impressions: z.number().default(50).describe("Minimum combined impressions for a query"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, min_impressions, dataset }) => {
    try {
      const results = await gscCannibalisation(days, min_impressions, dataset);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

// 10. GSC Traffic Drops
server.tool(
  "gsc_traffic_drops",
  "Find pages that lost the most traffic recently using GSC bulk export data. Compares current period vs prior period and diagnoses whether each drop is a ranking loss, CTR collapse, or demand decline. Filters to web search only. Requires GSC bulk data export to BigQuery.",
  {
    days: z.number().default(28).describe("Number of days per comparison period"),
    dataset: z.string().optional().describe("BigQuery dataset containing GSC data"),
  },
  async ({ days, dataset }) => {
    try {
      const results = await gscTrafficDrops(days, dataset);
      return {
        content: [{ type: "text", text: JSON.stringify(results, null, 2) }],
      };
    } catch (error) {
      return errorResponse(error);
    }
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("BigQuery MCP server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
