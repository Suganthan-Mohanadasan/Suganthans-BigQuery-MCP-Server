# BigQuery MCP Server

An MCP server that connects Claude to Google BigQuery. Query any dataset in natural language. Comes with built-in SEO analysis tools for Google Search Console bulk export data.

## What it does

**General purpose tools** (work with any BigQuery dataset):

- **query** — Run any SELECT query against BigQuery. Claude writes the SQL based on your question.
- **list_datasets** — Discover what datasets are available in your project.
- **list_tables** — See all tables and their schemas in a dataset.
- **describe_table** — Get detailed column info, row counts, partitioning, and clustering.

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
        "BIGQUERY_DEFAULT_DATASET": "searchconsole"
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

## Usage

Once connected, just ask Claude questions:

- "What are my quick win keywords?"
- "Which pages are losing traffic and why?"
- "Show me queries where multiple pages are competing"
- "Run a query to find my top 20 pages by clicks this month"
- "What tables are in my searchconsole dataset?"

Claude will use the appropriate tool, write SQL if needed, and interpret the results.

## Safety

- **Read only.** Only SELECT queries are allowed. INSERT, UPDATE, DELETE, DROP, and other mutation statements are blocked.
- **Row limits.** Default 100 rows per query, configurable up to 10,000.
- **Cost cap.** Queries are limited to 10GB bytes billed to prevent accidental cost blowout.

## Blog Post

Full walkthrough with screenshots: [suganthan.com/blog/bigquery-mcp-server/](https://suganthan.com/blog/bigquery-mcp-server/) (coming soon)

## License

MIT
