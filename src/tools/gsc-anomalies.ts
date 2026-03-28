import { runMLStatement } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscAnomalies(
  lookbackDays: number = 14,
  anomalyThreshold: number = 0.95,
  dataset?: string
): Promise<{
  model: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string; message?: string };
  anomalies: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  const project = config.projectId;
  validateIdentifier(ds, "dataset");

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

  const model = await runMLStatement(createSQL);

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

  const anomalies = await runMLStatement(anomalySQL, 50);

  return { model, anomalies };
}
