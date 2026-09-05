"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscDiscover = gscDiscover;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const gsc_shared_js_1 = require("./gsc-shared.js");
/**
 * Google Discover performance from the bulk export.
 *
 * Discover is page-based: there is no query dimension, so nothing here groups
 * by query. Its own anonymisation flag is `is_anonymized_discover`, separate
 * from `is_anonymized_query`.
 *
 * There is deliberately no device breakdown: `device` is NULL on every Discover
 * row, so the block only ever returned a single empty bucket holding every
 * click. Mobile versus desktop is not answerable for this surface.
 */
async function gscDiscover(days = 28, granularity = "week", urlContains, topUrls = 50, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const lastDay = await (0, gsc_shared_js_1.getLastExportDate)(ds, query_js_1.runQuery);
    const table = `\`${ds}.searchdata_url_impression\``;
    const window = `data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY)
      AND data_date <= DATE '${lastDay}'`;
    const urlScope = urlContains ? `AND url LIKE '%${(0, gsc_shared_js_1.escapeSQLString)(urlContains)}%'` : "";
    const summarySQL = `
    SELECT
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      COUNT(DISTINCT url) AS urls,
      SUM(IF(is_anonymized_discover, clicks, 0)) AS anonymized_clicks,
      ROUND(SAFE_DIVIDE(SUM(IF(is_anonymized_discover, clicks, 0)), SUM(clicks)) * 100, 2)
        AS anonymized_clicks_share_pct
    FROM ${table}
    WHERE ${window}
      AND search_type = 'DISCOVER'
      ${urlScope}
    LIMIT 1
  `;
    // Discover next to the other surfaces, so its weight is visible rather than
    // stated in isolation.
    const shareOfSiteSQL = `
    SELECT
      search_type,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(SUM(clicks)) OVER ()) * 100, 2) AS click_share_pct
    FROM ${table}
    WHERE ${window}
      ${urlScope}
    GROUP BY search_type
    ORDER BY clicks DESC
    LIMIT 10
  `;
    let timeSeriesSQL = null;
    if (granularity !== "none") {
        timeSeriesSQL = `
      SELECT
        DATE_TRUNC(data_date, ${gsc_shared_js_1.GRANULARITY_TRUNC[granularity]}) AS bucket,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        COUNT(DISTINCT url) AS urls
      FROM ${table}
      WHERE ${window}
        AND search_type = 'DISCOVER'
        ${urlScope}
      GROUP BY bucket
      ORDER BY bucket
      LIMIT 400
    `;
    }
    const topUrlsSQL = `
    SELECT
      url,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      MIN(data_date) AS first_day,
      MAX(data_date) AS last_day
    FROM ${table}
    WHERE ${window}
      AND search_type = 'DISCOVER'
      ${urlScope}
    GROUP BY url
    ORDER BY clicks DESC
    LIMIT ${Math.min(topUrls, 500)}
  `;
    const byCountrySQL = `
    SELECT
      country,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct
    FROM ${table}
    WHERE ${window}
      AND search_type = 'DISCOVER'
      ${urlScope}
    GROUP BY country
    ORDER BY clicks DESC
    LIMIT 20
  `;
    const [summary, shareOfSite, timeSeries, topUrlsResult, byCountry] = await Promise.all([
        (0, query_js_1.runQuery)(summarySQL, 1),
        (0, query_js_1.runQuery)(shareOfSiteSQL, 10),
        timeSeriesSQL ? (0, query_js_1.runQuery)(timeSeriesSQL, 400) : Promise.resolve(null),
        (0, query_js_1.runQuery)(topUrlsSQL, Math.min(topUrls, 500)),
        (0, query_js_1.runQuery)(byCountrySQL, 20),
    ]);
    const start = new Date(lastDay);
    start.setUTCDate(start.getUTCDate() - days + 1);
    return {
        period: {
            startDate: start.toISOString().split("T")[0],
            endDate: lastDay,
            days,
            anchoredTo: `latest day in the export (${lastDay}), not today`,
        },
        summary,
        shareOfSite,
        timeSeries,
        topUrls: topUrlsResult,
        byCountry,
    };
}
