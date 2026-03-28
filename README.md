# BigQuery MCP Server

An MCP server that connects Claude to Google BigQuery. Query any dataset in natural language. Comes with built-in SEO analysis tools for Google Search Console bulk export data.

## What it does

**General purpose tools** (work with any BigQuery dataset):

- **query** — Run any SELECT query against BigQuery. Claude writes the SQL based on your question. Auto-injects LIMIT if missing.
- **query_cost_estimate** — Dry-run a query to see how many bytes it would scan and the estimated cost before executing.
- **list_datasets** — Discover what datasets are available in your project.
- **list_tables** — See all tables and their schemas in a dataset. Uses INFORMATION_SCHEMA for efficiency.
- **describe_table** — Get detailed column info, row counts, partitioning, and clustering.
- **sample_rows** — Preview rows from any table without writing SQL.

**GSC analysis tools** (require GSC bulk data export to BigQuery):

- **gsc_quick_wins** — Keywords at positions 4 to 15 with high impressions. Striking distance opportunities.
- **gsc_content_decay** — Pages with traffic declining over three consecutive months.
- **gsc_cannibalisation** — Keywords where multiple pages from your site compete against each other.
- **gsc_traffic_drops** — Pages that lost traffic with diagnosis: ranking loss, CTR collapse, or demand decline.

## Why BigQuery instead of the GSC API?

The GSC API is great for quick, real-time lookups. But BigQuery bulk export gives you:

- **Unsampled data** (the API samples at high volumes)
- **Anonymous queries** (the API hides these entirely)
- **Full historical depth** (16 months backfill + continuous daily export)
- **Any SQL query you want** (not limited to the API's fixed parameters)

If you want the real-time API approach, see [Suganthan's GSC MCP Server](https://github.com/Suganthan-Mohanadasan/Suganthans-GSC-MCP). Both complement each other.

## Setup

### 1. Enable GSC bulk data export

In Google Search Console: Settings → Bulk data export → Set up BigQuery export. Choose your Google Cloud project and a dataset name (e.g. `searchconsole`). Data starts flowing within hours.

### 2. Create a service account

In Google Cloud Console:

1. Go to IAM & Admin → Service Accounts
2. Create a service account
3. Grant it **BigQuery Data Viewer** and **BigQuery Job User** roles
4. Create a JSON key and download it

### 3. Install the server

```bash
git clone https://github.com/Suganthan-Mohanadasan/BigQuery-MCP-Server.git
cd BigQuery-MCP-Server
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
      "args": ["/path/to/BigQuery-MCP-Server/dist/index.js"],
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
- "Run a query to find my top 20 pages by clicks this month"
- "What tables are in my searchconsole dataset?"
- "How much would it cost to query the full impressions table?"
- "Show me 10 sample rows from the url_impression table"

Claude will use the appropriate tool, write SQL if needed, and interpret the results.

## Safety

- **Read only.** Only SELECT queries are allowed. INSERT, UPDATE, DELETE, DROP, and other mutation statements are blocked. SQL comments and multi-statement queries are also caught.
- **Auto-LIMIT.** If your query has no LIMIT clause, one is automatically added based on max_rows.
- **Input validation.** Dataset, table, and project names are validated against a strict pattern to prevent injection.
- **Row limits.** Default 100 rows per query, configurable up to 10,000.
- **Cost cap.** Queries are limited to 10GB bytes billed to prevent accidental cost blowout. Sample queries are capped at 1GB.
- **Cost preview.** Use `query_cost_estimate` to dry-run any query and see the bytes scanned before committing.

## Blog Post

Full walkthrough with screenshots: [suganthan.com/blog/bigquery-mcp-server/](https://suganthan.com/blog/bigquery-mcp-server/)

## License

MIT
