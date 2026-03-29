# BigQuery MCP Server

An MCP server that connects Claude to Google BigQuery. Query any dataset in natural language. Comes with 26 tools including a full suite of SEO analysis tools for Google Search Console bulk export data, plus BigQuery exclusive features like ML forecasting, anomaly detection, and anonymous query analysis.

## What it does

**General purpose tools** (work with any BigQuery dataset):

| # | Tool | Description |
|---|------|-------------|
| 1 | **query** | Run any SELECT query against BigQuery. Auto-injects LIMIT if missing. |
| 2 | **query_cost_estimate** | Dry-run a query to see bytes scanned and estimated cost before executing. |
| 3 | **list_datasets** | Discover what datasets are available in your project. |
| 4 | **list_tables** | See all tables and schemas in a dataset. Uses INFORMATION_SCHEMA for efficiency. |
| 5 | **describe_table** | Get detailed column info, row counts, partitioning, and clustering. |
| 6 | **sample_rows** | Preview rows from any table without writing SQL. |

**GSC analysis tools** (require GSC bulk data export to BigQuery):

| # | Tool | Description |
|---|------|-------------|
| 7 | **gsc_quick_wins** | Keywords at positions 4 to 15 with high impressions. Striking distance opportunities. |
| 8 | **gsc_ctr_opportunities** | Pages with CTR significantly below benchmark for their position. Title/meta optimisation candidates. |
| 9 | **gsc_content_gaps** | Queries with high impressions but ranking beyond position 20. Content creation targets. |
| 10 | **gsc_site_snapshot** | Site overview with clicks, impressions, CTR, position, and period comparison. |
| 11 | **gsc_content_decay** | Pages with traffic declining over three consecutive months. |
| 12 | **gsc_cannibalisation** | Keywords where multiple pages compete against each other. |
| 13 | **gsc_traffic_drops** | Pages that lost traffic with diagnosis: ranking loss, CTR collapse, or demand decline. |
| 14 | **gsc_topic_cluster** | Aggregate performance for all pages matching a URL pattern, plus top pages and queries. |
| 15 | **gsc_ctr_benchmark** | Compare actual CTR per page against industry benchmarks by position with verdicts. |
| 16 | **gsc_alerts** | Severity rated alerts for position drops, CTR collapses, click losses, and disappeared pages. |
| 17 | **gsc_content_recommendations** | Prioritised actions by cross-referencing quick wins, content gaps, and cannibalisation data. |
| 18 | **gsc_report** | Comprehensive markdown performance report covering all sections. |

**BigQuery exclusive tools** (leverage BQ capabilities not available via the GSC API):

| # | Tool | Description |
|---|------|-------------|
| 19 | **gsc_anonymous_traffic** | Analyse the ~46% of clicks hidden behind anonymous queries. Breakdown by URL. |
| 20 | **gsc_seasonal** | Year over year monthly comparison with YoY change percentages. Spot seasonal patterns. |
| 21 | **gsc_device_split** | Find queries where mobile and desktop rank entirely different pages. |
| 22 | **gsc_intent_breakdown** | Classify all queries by search intent (informational, transactional, commercial, navigational). |
| 23 | **gsc_ngrams** | Extract common terms from queries to find recurring themes and topic clusters. |
| 24 | **gsc_new_keywords** | Discover queries appearing in recent data that weren't present in the baseline period. |
| 25 | **gsc_forecast** | ARIMA_PLUS traffic forecasting using BigQuery ML. Predict clicks up to 365 days out. |
| 26 | **gsc_anomalies** | ML powered anomaly detection. Automatically flags unusual traffic patterns. |

## Hallucination Guardrails

All GSC tools include built-in guardrails that instruct Claude to:

- Base analysis **only** on the data returned
- Report **exact numbers** from the results
- **Not speculate** about causes (algorithm updates, competitor actions) unless data supports it
- **Say clearly** when data is insufficient rather than guessing

Every response includes a `_meta` provenance field confirming the data source and parameters used.

## Why BigQuery instead of the GSC API?

The GSC API is great for quick, real-time lookups. But BigQuery bulk export gives you:

- **Unsampled data** (the API samples at high volumes)
- **Anonymous queries** (the API hides these entirely)
- **Unlimited retention** (keep data forever vs 16 months in GSC)
- **Any SQL query you want** (not limited to the API's fixed parameters)
- **BigQuery ML** (forecasting and anomaly detection built in)
- **~$12 to $24 per year** vs $15,000+ for equivalent paid tool subscriptions

If you want the real-time API approach, see [Suganthan's GSC MCP Server](https://github.com/Suganthan-Mohanadasan/Suganthans-GSC-MCP). Both complement each other.

## Setup

### 1. Enable GSC bulk data export

In Google Search Console: Settings → Bulk data export → Set up BigQuery export. Choose your Google Cloud project and a dataset name (e.g. `searchconsole`). Data starts flowing within hours.

### 2. Create a service account

In Google Cloud Console:

1. Go to IAM & Admin → Service Accounts
2. Create a service account (e.g. `bigquery-mcp-reader`)
3. Grant it 3 roles: **BigQuery Data Editor**, **BigQuery Data Viewer**, and **BigQuery Job User**
4. Create a JSON key and download it

Why Data Editor? The ML tools (forecasting and anomaly detection) need to create temporary models. If you only want read-only access, Data Viewer + Job User is enough, but the ML tools will return a clear error.

### 3. Install the server

```bash
git clone https://github.com/Suganthan-Mohanadasan/Suganthans-BigQuery-MCP-Server.git
cd Suganthans-BigQuery-MCP-Server
npm install
npm run build
```

### 4. Configure Claude Desktop

Add this to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "bigquery": {
      "command": "node",
      "args": ["/path/to/Suganthans-BigQuery-MCP-Server/dist/index.js"],
      "env": {
        "BIGQUERY_PROJECT_ID": "your-project-id",
        "BIGQUERY_KEY_FILE": "/path/to/service-account-key.json",
        "BIGQUERY_DEFAULT_DATASET": "searchconsole",
        "BIGQUERY_LOCATION": "US"
      }
    }
  }
}
```

Restart Claude Desktop. The BigQuery tools should appear.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `BIGQUERY_PROJECT_ID` | Yes | Your Google Cloud project ID |
| `BIGQUERY_KEY_FILE` | No | Path to service account JSON key (falls back to `GOOGLE_APPLICATION_CREDENTIALS`) |
| `BIGQUERY_DEFAULT_DATASET` | No | Default dataset for queries (e.g. `searchconsole`) |
| `BIGQUERY_LOCATION` | No | BigQuery dataset location (default: `US`). Set to `EU`, `asia-southeast1`, etc. if your data lives elsewhere. |

## Usage

Once connected, just ask Claude questions:

- "What are my quick win keywords?"
- "Which pages are losing traffic and why?"
- "Show me queries where multiple pages are competing"
- "Which pages have CTR below benchmark?"
- "Give me a site snapshot for the last 28 days"
- "How is my /blog/seo/ cluster performing?"
- "Are there any SEO alerts I should know about?"
- "Generate a full performance report"
- "What content should I create or update next?"
- "How much of my traffic comes from anonymous queries?"
- "Show me year over year seasonal trends"
- "Forecast my traffic for the next 90 days"
- "Are there any traffic anomalies I should investigate?"
- "What new keywords appeared this week?"
- "Break down my queries by search intent"

Claude will use the appropriate tool, write SQL if needed, and interpret the results.

## Safety

- **Read only.** Only SELECT queries are allowed. INSERT, UPDATE, DELETE, DROP, and other mutation statements are blocked. SQL comments and multi-statement queries are also caught. BigQuery ML statements are limited to CREATE OR REPLACE MODEL and SELECT only.
- **Auto-LIMIT.** If your query has no LIMIT clause, one is automatically added based on max_rows.
- **Input validation.** Dataset, table, and project names are validated against a strict pattern to prevent injection.
- **Row limits.** Default 100 rows per query, configurable up to 10,000.
- **Cost cap.** Queries are limited to 10GB bytes billed to prevent accidental cost blowout. Sample queries are capped at 1GB.
- **Cost preview.** Use `query_cost_estimate` to dry-run any query and see the bytes scanned before committing.
- **Guardrails.** All GSC tools include hallucination prevention instructions and data provenance metadata.

## Blog Post

Full walkthrough with screenshots: [suganthan.com/blog/bigquery-mcp-server/](https://suganthan.com/blog/bigquery-mcp-server/)

## License

MIT
