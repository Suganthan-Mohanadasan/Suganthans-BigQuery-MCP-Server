"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscQuickWins = gscQuickWins;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const gsc_shared_js_1 = require("./gsc-shared.js");
async function gscQuickWins(days = 28, minImpressions = 100, maxPosition = 15, device, country, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const extra = (0, gsc_shared_js_1.deviceCountryConditions)(device, country, "WEB");
    const scopeSQL = extra.length ? `AND ${extra.join("\n      AND ")}` : "";
    const sql = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1, 1) AS avg_position,
      ROUND(SUM(impressions) * (0.11 - SAFE_DIVIDE(SUM(clicks), SUM(impressions))), 0) AS opportunity
    FROM \`${ds}.searchdata_site_impression\`
    WHERE
      data_date >= DATE_SUB(${(0, gsc_shared_js_1.lastExportDay)(ds, "searchdata_site_impression")}, INTERVAL ${days} DAY)
      AND is_anonymized_query = false
      AND search_type = 'WEB'
      ${scopeSQL}
    GROUP BY query
    HAVING
      avg_position BETWEEN 4 AND ${maxPosition}
      AND impressions >= ${minImpressions}
    ORDER BY opportunity DESC
    LIMIT 50
  `;
    return (0, query_js_1.runQuery)(sql, 50);
}
