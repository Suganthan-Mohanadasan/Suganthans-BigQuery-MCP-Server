"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.gscImageSearch = gscImageSearch;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const gsc_shared_js_1 = require("./gsc-shared.js");
/**
 * Google Images performance from the bulk export.
 *
 * Unlike Discover, image search does carry queries, so this groups by both url
 * and query. The url is the page the image sits on, never the image file - the
 * export has no image-level dimension, which is the honest limit of this
 * surface and the reason a low image CTR says little about the image itself.
 *
 * `is_amp_image_result` is reported alongside, because an AMP image result is a
 * different SERP element from an ordinary image result.
 */
async function gscImageSearch(days = 28, granularity = "week", urlContains, topRows = 50, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    const lastDay = await (0, gsc_shared_js_1.getLastExportDate)(ds, query_js_1.runQuery);
    const table = `\`${ds}.searchdata_url_impression\``;
    const window = `data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY)
      AND data_date <= DATE '${lastDay}'`;
    const urlScope = urlContains ? `AND url LIKE '%${(0, gsc_shared_js_1.escapeSQLString)(urlContains)}%'` : "";
    const imageScope = `AND search_type = 'IMAGE'`;
    const limit = Math.min(topRows, 500);
    const summarySQL = `
    SELECT
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(${gsc_shared_js_1.AVG_POSITION_SQL}, 1) AS avg_position,
      COUNT(DISTINCT url) AS urls,
      COUNT(DISTINCT query) AS visible_queries,
      SUM(IF(is_anonymized_query, clicks, 0)) AS anonymized_clicks,
      ROUND(SAFE_DIVIDE(SUM(IF(is_anonymized_query, clicks, 0)), SUM(clicks)) * 100, 2)
        AS anonymized_clicks_share_pct,
      SUM(IF(is_amp_image_result, clicks, 0)) AS amp_image_result_clicks,
      SUM(IF(is_amp_image_result, impressions, 0)) AS amp_image_result_impressions
    FROM ${table}
    WHERE ${window}
      ${imageScope}
      ${urlScope}
    LIMIT 1
  `;
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
        ROUND(${gsc_shared_js_1.AVG_POSITION_SQL}, 1) AS avg_position,
        COUNT(DISTINCT url) AS urls
      FROM ${table}
      WHERE ${window}
        ${imageScope}
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
      ROUND(${gsc_shared_js_1.AVG_POSITION_SQL}, 1) AS avg_position,
      COUNT(DISTINCT query) AS queries
    FROM ${table}
    WHERE ${window}
      ${imageScope}
      ${urlScope}
    GROUP BY url
    ORDER BY impressions DESC
    LIMIT ${limit}
  `;
    const topQueriesSQL = `
    SELECT
      query,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(${gsc_shared_js_1.AVG_POSITION_SQL}, 1) AS avg_position,
      COUNT(DISTINCT url) AS urls
    FROM ${table}
    WHERE ${window}
      ${imageScope}
      AND query IS NOT NULL
      ${urlScope}
    GROUP BY query
    ORDER BY impressions DESC
    LIMIT ${limit}
  `;
    const byDeviceSQL = `
    SELECT
      device,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
      ROUND(${gsc_shared_js_1.AVG_POSITION_SQL}, 1) AS avg_position
    FROM ${table}
    WHERE ${window}
      ${imageScope}
      ${urlScope}
    GROUP BY device
    ORDER BY clicks DESC
    LIMIT 10
  `;
    const byCountrySQL = `
    SELECT
      country,
      SUM(clicks) AS clicks,
      SUM(impressions) AS impressions,
      ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct
    FROM ${table}
    WHERE ${window}
      ${imageScope}
      ${urlScope}
    GROUP BY country
    ORDER BY clicks DESC
    LIMIT 20
  `;
    const [summary, shareOfSite, timeSeries, topUrls, topQueries, byDevice, byCountry] = await Promise.all([
        (0, query_js_1.runQuery)(summarySQL, 1),
        (0, query_js_1.runQuery)(shareOfSiteSQL, 10),
        timeSeriesSQL ? (0, query_js_1.runQuery)(timeSeriesSQL, 400) : Promise.resolve(null),
        (0, query_js_1.runQuery)(topUrlsSQL, limit),
        (0, query_js_1.runQuery)(topQueriesSQL, limit),
        (0, query_js_1.runQuery)(byDeviceSQL, 10),
        (0, query_js_1.runQuery)(byCountrySQL, 20),
    ]);
    const start = new Date(lastDay);
    start.setUTCDate(start.getUTCDate() - days + 1);
    const impressions = Number(summary.rows[0]?.impressions ?? 0);
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
        topUrls,
        topQueries,
        byDevice,
        byCountry,
        note: impressions > 0
            ? "url is the page hosting the image, not the image file: the export carries no image-level dimension. A weak CTR here therefore points at the page and its thumbnail in the grid, not necessarily at the image itself."
            : "No image search impressions in this window. Either the property is not indexed in Google Images, or images are served from a different property than this one.",
    };
}
