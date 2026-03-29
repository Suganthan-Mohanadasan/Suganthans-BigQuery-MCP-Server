# BigQuery MCP Server

26 SEO tools for your Google Search Console data warehouse. ML traffic forecasting, anomaly detection, anonymous query analysis, and 23 more. Open source, free, Apache 2.0.

> **Full setup guide with screenshots:** [suganthan.com/blog/bigquery-mcp-server/](https://suganthan.com/blog/bigquery-mcp-server/)

## What it looks like

Ask Claude a question. The right tool fires automatically.

"**What are my quick win keywords?**" → Finds queries at positions 4 to 15 with high impressions. Striking distance opportunities sorted by traffic potential.

"**Forecast my traffic for the next 90 days**" → Builds an ARIMA_PLUS model on your historical data and returns daily predicted clicks with confidence intervals.

"**How much of my traffic is hidden behind anonymous queries?**" → Analyses the ~46% of clicks the GSC API completely hides from you. Broken down by URL.

"**Which pages are losing traffic and why?**" → Diagnoses each drop as a ranking loss, CTR collapse, or demand decline. Not guesswork. Data.

Not raw query results. Actual SEO analysis with exact numbers, verdicts, and recommendations.

## 8 tools you can't get from the GSC API

These use BigQuery capabilities that the Search Console API simply doesn't have.

| Tool | What it does |
|------|-------------|
| **gsc_anonymous_traffic** | Analyse the ~46% of clicks hidden behind anonymous queries. The API hides these entirely. |
| **gsc_forecast** | ARIMA_PLUS traffic forecasting via BigQuery ML. Predict clicks up to 365 days out. |
| **gsc_anomalies** | ML anomaly detection. Flags genuinely unusual traffic patterns, not just threshold breaches. |
| **gsc_seasonal** | Year over year monthly comparison. Spot seasonal patterns with YoY percentage changes. |
| **gsc_device_split** | Find queries where mobile and desktop rank entirely different pages from your site. |
| **gsc_intent_breakdown** | Classify all queries by search intent: informational, transactional, commercial, navigational. |
| **gsc_ngrams** | Extract recurring terms from queries. Find themes your content should cover. |
| **gsc_new_keywords** | Discover queries appearing in recent data that weren't present before. |

## 12 GSC analysis tools

The same analysis tools from the [GSC MCP server](https://github.com/Suganthan-Mohanadasan/Suganthans-GSC-MCP), rebuilt to run on BigQuery's unsampled data.

| Tool | What it does |
|------|-------------|
| **gsc_quick_wins** | Striking distance keywords (positions 4 to 15). |
| **gsc_ctr_opportunities** | Pages with CTR below benchmark for their position. |
| **gsc_content_gaps** | High impression queries ranking beyond position 20. |
| **gsc_site_snapshot** | Site overview with period comparison. |
| **gsc_content_decay** | Pages declining over 3 consecutive months. |
| **gsc_cannibalisation** | Keywords where multiple pages compete. |
| **gsc_traffic_drops** | Lost traffic with diagnosis (ranking, CTR, or demand). |
| **gsc_topic_cluster** | Performance for pages matching a URL pattern. |
| **gsc_ctr_benchmark** | Actual CTR vs industry benchmarks with verdicts. |
| **gsc_alerts** | Severity rated alerts for drops and disappearances. |
| **gsc_content_recommendations** | Prioritised actions from cross-referencing all data. |
| **gsc_report** | Full markdown performance report. |

## 6 general purpose tools

Work with any BigQuery dataset, not just GSC.

| Tool | What it does |
|------|-------------|
| **query** | Run any SELECT query. Claude writes the SQL for you. |
| **query_cost_estimate** | Dry run to see bytes scanned before executing. |
| **list_datasets** | Discover available datasets in your project. |
| **list_tables** | All tables and schemas in a dataset. |
| **describe_table** | Column types, row counts, partitioning, size. |
| **sample_rows** | Preview rows without writing SQL. |

## Why BigQuery instead of the GSC API?

| | GSC API | BigQuery bulk export |
|---|---------|---------------------|
| Sampling | Sampled at high volumes | Unsampled |
| Anonymous queries | Hidden entirely | Included |
| History | Rolling 16 months | Permanent (forward only, no backfill) |
| Query flexibility | Fixed parameters | Any SQL you want |
| ML capabilities | None | ARIMA forecasting, anomaly detection |
| Cost | Free | ~$12 to $24/year |

Both complement each other. Use the [GSC MCP server](https://github.com/Suganthan-Mohanadasan/Suganthans-GSC-MCP) for quick real-time lookups. Use this for deeper analysis.

## Quick start

```bash
git clone https://github.com/Suganthan-Mohanadasan/Suganthans-BigQuery-MCP-Server.git
cd Suganthans-BigQuery-MCP-Server
npm install
npm run build
```

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

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

> **Need help with BigQuery setup, service accounts, or permissions?** The [full guide](https://suganthan.com/blog/bigquery-mcp-server/) walks through every step with screenshots.

## What you can ask

```
What are my quick win keywords?
Which pages are losing traffic and why?
Forecast my traffic for the next 90 days
Are there any traffic anomalies I should investigate?
How much of my traffic comes from anonymous queries?
Show me year over year seasonal trends
What new keywords appeared this week?
Break down my queries by search intent
Show me queries where multiple pages are competing
Generate a full performance report
```

## Safety

**Read only.** Only SELECT queries allowed. All mutations blocked. BigQuery ML limited to model creation and SELECT.

**Cost controlled.** Auto-LIMIT on queries, 10GB bytes billed cap, dry run cost preview.

**Input validated.** All identifiers checked against strict patterns. No injection vectors.

**Hallucination guardrails.** Every GSC tool instructs Claude to report exact numbers, avoid speculation, and flag insufficient data. Every response includes `_meta` provenance.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `BIGQUERY_PROJECT_ID` | Yes | Your Google Cloud project ID |
| `BIGQUERY_KEY_FILE` | No | Path to service account JSON key (falls back to `GOOGLE_APPLICATION_CREDENTIALS`) |
| `BIGQUERY_DEFAULT_DATASET` | No | Default dataset for queries (e.g. `searchconsole`) |
| `BIGQUERY_LOCATION` | No | Dataset location (default: `US`). Set to `EU`, `asia-southeast1`, etc. if needed. |

## Full guide

Step by step setup with screenshots, cost breakdowns, and honest comparison with dedicated SEO tools: **[suganthan.com/blog/bigquery-mcp-server/](https://suganthan.com/blog/bigquery-mcp-server/)**

## Licence

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for details. Use it, fork it, build on it. Just keep the attribution.
