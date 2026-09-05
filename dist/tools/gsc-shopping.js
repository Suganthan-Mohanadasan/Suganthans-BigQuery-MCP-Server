"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SHOPPING_APPEARANCES = void 0;
exports.gscShopping = gscShopping;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
const gsc_shared_js_1 = require("./gsc-shared.js");
/**
 * Organic shopping surfaces: free product listings, merchant listings and
 * product snippets.
 *
 * These are search appearances, not search types - they live inside WEB rows,
 * flagged by their own boolean columns. The GSC API exposes them only through
 * the searchAppearance dimension, which cannot be combined with other
 * groupings; here they are ordinary columns, so they can be crossed with url,
 * query, device and date freely.
 *
 * A property that sells nothing returns zeros. That is an answer, not a bug,
 * and the result says so explicitly instead of looking like a failed query.
 */
exports.SHOPPING_APPEARANCES = {
    organic_shopping: "is_organic_shopping",
    merchant_listings: "is_merchant_listings",
    product_snippets: "is_product_snippets",
};
async function gscShopping(days = 28, appearance, granularity = "week", urlContains, topRows = 50, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    if (appearance && !(appearance in exports.SHOPPING_APPEARANCES)) {
        throw new Error(`Unknown appearance "${appearance}". Allowed: ${Object.keys(exports.SHOPPING_APPEARANCES).join(", ")}.`);
    }
    const lastDay = await (0, gsc_shared_js_1.getLastExportDate)(ds, query_js_1.runQuery);
    const table = `\`${ds}.searchdata_url_impression\``;
    const window = `data_date > DATE_SUB(DATE '${lastDay}', INTERVAL ${days} DAY)
      AND data_date <= DATE '${lastDay}'`;
    const urlScope = urlContains ? `AND url LIKE '%${(0, gsc_shared_js_1.escapeSQLString)(urlContains)}%'` : "";
    const metricsFor = (name, column) => `
      SUM(IF(${column}, clicks, 0)) AS ${name}_clicks,
      SUM(IF(${column}, impressions, 0)) AS ${name}_impressions,
      ROUND(SAFE_DIVIDE(
        SUM(IF(${column}, clicks, 0)), SUM(IF(${column}, impressions, 0))
      ) * 100, 2) AS ${name}_ctr_pct,
      ROUND(SAFE_DIVIDE(
        SUM(IF(${column}, sum_position, 0)), SUM(IF(${column}, impressions, 0))
      ) + 1, 1) AS ${name}_avg_position,
      COUNT(DISTINCT IF(${column}, url, NULL)) AS ${name}_urls,
      COUNT(DISTINCT IF(${column}, query, NULL)) AS ${name}_queries`;
    const overviewSQL = `
    SELECT
      ${metricsFor("organic_shopping", "is_organic_shopping")},
      ${metricsFor("merchant_listings", "is_merchant_listings")},
      ${metricsFor("product_snippets", "is_product_snippets")},
      SUM(clicks) AS all_clicks,
      SUM(impressions) AS all_impressions
    FROM ${table}
    WHERE ${window}
      ${urlScope}
    LIMIT 1
  `;
    const overview = await (0, query_js_1.runQuery)(overviewSQL, 1);
    const row = overview.rows[0] || {};
    const clicksOf = (name) => Number(row[`${name}_clicks`] ?? 0);
    const impressionsOf = (name) => Number(row[`${name}_impressions`] ?? 0);
    const totalShoppingImpressions = impressionsOf("organic_shopping") +
        impressionsOf("merchant_listings") +
        impressionsOf("product_snippets");
    const dataAvailable = totalShoppingImpressions > 0;
    const present = Object.keys(exports.SHOPPING_APPEARANCES).filter((name) => impressionsOf(name) > 0);
    const absent = Object.keys(exports.SHOPPING_APPEARANCES).filter((name) => impressionsOf(name) === 0);
    const note = dataAvailable
        ? `Present with impressions: ${present
            .map((name) => `${name} (${clicksOf(name)} clicks)`)
            .join(", ")}.` +
            (absent.length
                ? ` No impressions at all on: ${absent.join(", ")} - the property does not appear on ${absent.length === 1 ? "that surface" : "those surfaces"}. Do not read a figure from one appearance as evidence for another.`
                : "")
        : "No impressions on any organic shopping surface in this window. The property appears in neither free product listings, merchant listings nor product snippets - typical for a site that sells nothing. Report this as a finding, not as missing data.";
    let drilldown = null;
    if (appearance) {
        const column = exports.SHOPPING_APPEARANCES[appearance];
        const flagScope = `AND ${column}`;
        const limit = Math.min(topRows, 500);
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
        ${flagScope}
        ${urlScope}
      GROUP BY url
      ORDER BY clicks DESC
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
        ${flagScope}
        AND query IS NOT NULL
        ${urlScope}
      GROUP BY query
      ORDER BY clicks DESC
      LIMIT ${limit}
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
          ${flagScope}
          ${urlScope}
        GROUP BY bucket
        ORDER BY bucket
        LIMIT 400
      `;
        }
        const [topUrls, topQueries, timeSeries] = await Promise.all([
            (0, query_js_1.runQuery)(topUrlsSQL, limit),
            (0, query_js_1.runQuery)(topQueriesSQL, limit),
            timeSeriesSQL ? (0, query_js_1.runQuery)(timeSeriesSQL, 400) : Promise.resolve(null),
        ]);
        drilldown = { appearance, topUrls, topQueries, timeSeries };
    }
    const start = new Date(lastDay);
    start.setUTCDate(start.getUTCDate() - days + 1);
    return {
        period: {
            startDate: start.toISOString().split("T")[0],
            endDate: lastDay,
            days,
            anchoredTo: `latest day in the export (${lastDay}), not today`,
        },
        overview,
        dataAvailable,
        note,
        drilldown,
    };
}
