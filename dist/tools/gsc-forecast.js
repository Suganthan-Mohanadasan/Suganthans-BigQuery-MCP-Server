"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscForecast = gscForecast;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
async function gscForecast(horizon = 30, confidenceLevel = 0.95, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    const project = config.projectId;
    (0, client_js_1.validateIdentifier)(ds, "dataset");
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
    const model = await (0, query_js_1.runMLStatement)(createSQL);
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
    const forecast = await (0, query_js_1.runMLStatement)(forecastSQL, cappedHorizon);
    return { model, forecast };
}
