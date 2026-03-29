"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscAnomalies = gscAnomalies;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscAnomalies(lookbackDays = 14, anomalyThreshold = 0.95, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    const project = config.projectId;
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const modelName = `\`${project}.${ds}.clicks_forecast_model\``;
    // Ensure model exists (create if needed)
    const createSQL = `
    CREATE OR REPLACE MODEL ${modelName}
    OPTIONS(
      model_type = 'ARIMA_PLUS',
      time_series_timestamp_col = 'date',
      time_series_data_col = 'total_clicks',
      auto_arima = TRUE,
      data_frequency = 'DAILY',
      decompose_time_series = TRUE
    ) AS
    SELECT
      data_date AS date,
      SUM(clicks) AS total_clicks
    FROM \`${ds}.searchdata_url_impression\`
    WHERE search_type = 'WEB'
    GROUP BY 1
    HAVING total_clicks > 0
    ORDER BY 1
  `;
    let model;
    try {
        model = await (0, query_js_1.runMLStatement)(createSQL);
    }
    catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("permission") || msg.includes("Access Denied")) {
            throw new Error(`BigQuery ML requires the "BigQuery Data Editor" role on your service account. ` +
                `Grant it in IAM, then restart the MCP server. Original error: ${msg}`);
        }
        throw e;
    }
    // Detect anomalies in recent data
    const anomalySQL = `
    SELECT
      *
    FROM ML.DETECT_ANOMALIES(
      MODEL ${modelName},
      STRUCT(${anomalyThreshold} AS anomaly_prob_threshold)
    )
    WHERE is_anomaly = TRUE
    ORDER BY anomaly_probability DESC
    LIMIT 50
  `;
    const anomalies = await (0, query_js_1.runMLStatement)(anomalySQL, 50);
    return { model, anomalies };
}
