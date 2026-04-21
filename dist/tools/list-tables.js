"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.listTables = listTables;
const client_js_1 = require("../client.js");
async function listTables(dataset, projectId) {
    (0, client_js_1.validateIdentifier)(dataset, "dataset");
    const client = (0, client_js_1.getBigQueryClient)();
    const config = (0, client_js_1.getConfig)();
    const targetProject = projectId || config.projectId;
    if (projectId) {
        (0, client_js_1.validateIdentifier)(projectId, "project_id");
    }
    const tableQuery = `
    SELECT
      table_id AS table_name,
      CASE type
        WHEN 1 THEN 'BASE TABLE'
        WHEN 2 THEN 'VIEW'
        WHEN 3 THEN 'EXTERNAL'
        ELSE 'TABLE'
      END AS table_type,
      CAST(row_count AS STRING) AS row_count,
      CAST(size_bytes AS STRING) AS size_bytes,
      CAST(TIMESTAMP_MILLIS(creation_time) AS STRING) AS creation_time
    FROM \`${targetProject}.${dataset}.__TABLES__\`
    ORDER BY table_id
  `;
    const columnQuery = `
    SELECT
      table_name,
      column_name,
      data_type,
      is_nullable
    FROM \`${targetProject}.${dataset}.INFORMATION_SCHEMA.COLUMNS\`
    ORDER BY table_name, ordinal_position
  `;
    const location = (0, client_js_1.resolveLocation)(targetProject);
    const [tableJob] = await client.createQueryJob({ query: tableQuery, location });
    const [columnJob] = await client.createQueryJob({ query: columnQuery, location });
    const [tableRows] = await tableJob.getQueryResults();
    const [columnRows] = await columnJob.getQueryResults();
    const columnsByTable = new Map();
    for (const col of columnRows) {
        const tableName = col.table_name;
        if (!columnsByTable.has(tableName)) {
            columnsByTable.set(tableName, []);
        }
        columnsByTable.get(tableName).push({
            name: col.column_name,
            type: col.data_type,
            mode: col.is_nullable === "YES" ? "NULLABLE" : "REQUIRED",
        });
    }
    return tableRows.map((t) => ({
        id: t.table_name,
        type: t.table_type || "TABLE",
        rows: t.row_count || "0",
        sizeBytes: t.size_bytes || "0",
        created: t.creation_time || "unknown",
        columns: columnsByTable.get(t.table_name) || [],
    }));
}
