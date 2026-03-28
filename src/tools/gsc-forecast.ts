import { runMLStatement } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

export async function gscForecast(
  horizon: number = 30,
  confidenceLevel: number = 0.95,
  dataset?: string
): Promise<{
  model: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string; message?: string };
  forecast: { rows: Record<string, unknown>[]; totalRows: number; bytesProcessed: string };
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  const project = config.projectId;
  validateIdentifier(ds, "dataset");

  const cappedHorizon = Math.min(Math.max(horizon, 7), 365);
  const modelName = `\`${project}.${ds}.clicks_forecast_model\``;

  // Step 1: Create/refresh the ARIMA model
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

  // Step 2: Forecast
  const forecastSQL = `
    SELECT
      forecast_timestamp AS date,
      ROUND(forecast_value, 0) AS predicted_clicks,
      ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
      ROUND(prediction_interval_upper_bound, 0) AS upper_bound,
      ROUND(forecast_value - prediction_interval_lower_bound, 0) AS uncertainty_range
    FROM ML.FORECAST(MODEL ${modelName},
      STRUCT(${cappedHorizon} AS horizon, ${confidenceLevel} AS confidence_level))
    ORDER BY forecast_timestamp
  `;

  const forecast = await runMLStatement(forecastSQL, cappedHorizon);

  return { model, forecast };
}
