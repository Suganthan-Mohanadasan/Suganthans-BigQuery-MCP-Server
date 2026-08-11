"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.classifyQuery = classifyQuery;
exports.gscGenaiConversationQueries = gscGenaiConversationQueries;
const query_js_1 = require("./query.js");
const client_js_1 = require("../client.js");
/**
 * gsc_genai_conversation_queries — the BigQuery twin of the GSC MCP's
 * genai_conversation_queries tool. Same seven-bucket classifier, same
 * pattern library (keep the two in sync):
 * https://github.com/Suganthan-Mohanadasan/Suganthans-GSC-MCP
 * src/tools/genai-conversation-queries.ts
 *
 * Mechanism: Google counts every AI Mode follow-up as a brand-new query and
 * folds AI Mode / AI Overviews into the WEB search type, so conversation
 * fragments, tracker probes, and full agent prompts land in the query table
 * with impressions, positions and clicks.
 *
 * What this twin adds over the API version: no serving limits (the bulk
 * export keeps long-tail rows the API drops on large sites), exact windows
 * anchored to the export's real freshness, and the anonymised split — the
 * bulk export states how many impressions carry no query string at all.
 * Rare strings get anonymised, and conversations are almost by definition
 * rare strings, so the anonymised pool is where most of the conversation
 * iceberg sits. What the API version has that this one lacks: 16 months of
 * history (bulk exports only reach back to when the property enabled them,
 * minus any partition expiry).
 */
const ARTEFACT_SRC = "^(yes|yeah|yep|ok|okay|okey|sure|no|nope|go on|continue|keep going|tell me more|more|next|why not|how so|really|exactly|show me|and then|thanks|thank you|got it|what else|anything else)[?!.,]*$";
const PIVOT_SRC = "^(what|how) about .{1,40}$";
const TRACKER_SRC = "(\\. ?my location is )|(^evaluate the .+ on )";
const AGENT_SRC = "^(search the web for|browse the web)|do not invent|return the [0-9]+ most relevant|include source urls";
const CONV_SRC = "\\b(please|can you|could you|give me|show me|tell me|i want|i need|i have|i am|should i|do i need|is it worth|how do i|how can i|help me|walk me through)\\b";
const DEICTIC_SRC = "^(what|how|why|is|does|can|will|would) (it|that|this|they)\\b";
const TEN_PLUS_WORDS_SRC = "^([^ ]+ ){9,}[^ ]+$";
const PASTED_SRC = "([^,]*,){5}|has been captured for this url prefix";
const UNION_SRC = [
    ARTEFACT_SRC,
    PIVOT_SRC,
    TRACKER_SRC,
    AGENT_SRC,
    CONV_SRC,
    DEICTIC_SRC,
    TEN_PLUS_WORDS_SRC,
    PASTED_SRC,
].join("|");
const QSTART = /^(what|how|why|when|where|which|who|can|does|do|is|are|should|will|would)\b/;
const RE = {
    artefact: new RegExp(ARTEFACT_SRC),
    pivot: new RegExp(PIVOT_SRC),
    tracker: new RegExp(TRACKER_SRC),
    agent: new RegExp(AGENT_SRC),
    conv: new RegExp(CONV_SRC),
    deictic: new RegExp(DEICTIC_SRC),
    pasted: new RegExp(PASTED_SRC),
};
function classifyQuery(query) {
    const q = query.toLowerCase().trim();
    const words = q.split(/ +/).length;
    if (RE.artefact.test(q))
        return "artefact";
    if (RE.pivot.test(q))
        return "pivot";
    if (RE.agent.test(q))
        return "agent_harness";
    if (RE.tracker.test(q))
        return "tracker_probe";
    if (RE.pasted.test(q))
        return "pasted_string";
    if (RE.conv.test(q) || RE.deictic.test(q))
        return "conversational";
    if (words >= 10 && QSTART.test(q))
        return "conversational";
    if (words >= 10)
        return "long_uncategorised";
    return null;
}
const BUCKET_ORDER = [
    "artefact",
    "pivot",
    "conversational",
    "tracker_probe",
    "agent_harness",
    "pasted_string",
    "long_uncategorised",
];
const BUCKET_MEANING = {
    artefact: "A person mid-conversation with Google's AI replied ('yes', 'go on') and the reply was logged as a query your page appeared for. Positions here are AI-block positions, not open-web rankings.",
    pivot: "Mid-conversation comparison questions ('what about resend'). Each one names the alternative users ask the AI about next; a ready-made comparison-section instruction for the landing page.",
    conversational: "Human questions shaped by AI conversation. High impressions with near-zero clicks here means your page is being read inside answers rather than clicked from results.",
    tracker_probe: "Synthetic daily prompts from AI-visibility tracking tools flowing through Google's grounding layer. Not human demand: exclude from opportunity analyses. If you run an AI tracker, this is its probes independently logged by Google; if you don't, third parties are sweeping topics you rank in.",
    agent_harness: "Complete agent instructions ('search the web for… do not invent results') logged as queries. Machine traffic; interesting for what agents surface you for, worthless as keyword demand.",
    pasted_string: "Error messages and CSV headers entered as searches, by people or pipelines.",
    long_uncategorised: "Ten or more words without another marker. A mix of quoted-sentence searches, agent fragments and rare long-tail; review manually before acting.",
};
const CANNOT_SEE = [
    "Anonymised (rare) queries have no query string in the export, so bucket contents are a floor. Unlike the API, the export does count them: see anonymised_visibility for the size of the hidden pool.",
    "AI Overviews and AI Mode cannot be told apart at query level; both fold into the WEB search type.",
    "Classification reads query shape, not certainty. Treat single rows as evidence, not statistics.",
    "The operator behind tracker probes and agent harnesses is not identifiable from this data.",
    "Impressions in probe and harness buckets are machine-generated views, not people.",
    "History reaches back only as far as this property's bulk export retention. For the full 16 months Google retains, use genai_conversation_queries in the GSC MCP (API-based twin).",
];
async function gscGenaiConversationQueries(days = 365, minImpressions = 1, maxRowsPerBucket = 50, includeTimeline = true, dataset) {
    const config = (0, client_js_1.getConfig)();
    const ds = dataset || config.defaultDataset || "searchconsole";
    (0, client_js_1.validateIdentifier)(ds, "dataset");
    // Windows anchor to MAX(data_date), not CURRENT_DATE(): the export runs
    // roughly two days behind and a today-relative window would silently
    // include empty days.
    const mainSQL = `
    WITH bounds AS (
      SELECT MAX(data_date) AS end_date
      FROM \`${ds}.searchdata_url_impression\`
      WHERE search_type = 'WEB'
    ),
    per_url AS (
      SELECT
        query,
        url,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(sum_position) AS sum_position
      FROM \`${ds}.searchdata_url_impression\`, bounds
      WHERE
        search_type = 'WEB'
        AND is_anonymized_query = FALSE
        AND query IS NOT NULL
        AND data_date BETWEEN DATE_SUB(bounds.end_date, INTERVAL ${Math.max(1, days) - 1} DAY) AND bounds.end_date
        AND REGEXP_CONTAINS(LOWER(query), r'${UNION_SRC}')
      GROUP BY query, url
    )
    SELECT
      query,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1, 1) AS position,
      ARRAY_AGG(STRUCT(url AS page, impressions AS impressions) ORDER BY impressions DESC LIMIT 3) AS top_pages
    FROM per_url
    GROUP BY query
    ORDER BY impressions DESC
    LIMIT 2000
  `;
    // The anonymised split: how much visibility carries no query string at all.
    // Site-level table so impressions are property-aggregated, not per URL.
    const icebergSQL = `
    WITH bounds AS (
      SELECT MAX(data_date) AS end_date
      FROM \`${ds}.searchdata_site_impression\`
      WHERE search_type = 'WEB'
    )
    SELECT
      MIN(data_date) AS window_start,
      MAX(data_date) AS window_end,
      SUM(IF(is_anonymized_query, impressions, 0)) AS anonymised_impressions,
      SUM(IF(NOT is_anonymized_query, impressions, 0)) AS visible_impressions,
      SUM(IF(is_anonymized_query, clicks, 0)) AS anonymised_clicks,
      SUM(IF(NOT is_anonymized_query, clicks, 0)) AS visible_clicks,
      COUNT(DISTINCT IF(NOT is_anonymized_query, query, NULL)) AS distinct_visible_queries
    FROM \`${ds}.searchdata_site_impression\`, bounds
    WHERE
      search_type = 'WEB'
      AND data_date BETWEEN DATE_SUB(bounds.end_date, INTERVAL ${Math.max(1, days) - 1} DAY) AND bounds.end_date
    LIMIT 10
  `;
    const retentionSQL = `
    SELECT MIN(data_date) AS first_available, MAX(data_date) AS last_available
    FROM \`${ds}.searchdata_url_impression\`
    LIMIT 10
  `;
    // Timeline runs over the full available retention regardless of the days
    // window, because its job is dating when the reply-artefact class first
    // appeared on this property.
    const timelineSQL = `
    SELECT
      FORMAT_DATE('%Y-%m', data_date) AS month,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks
    FROM \`${ds}.searchdata_url_impression\`
    WHERE
      search_type = 'WEB'
      AND is_anonymized_query = FALSE
      AND query IS NOT NULL
      AND REGEXP_CONTAINS(LOWER(query), r'${ARTEFACT_SRC}')
    GROUP BY month
    ORDER BY month
    LIMIT 200
  `;
    const [main, iceberg, retention, timeline] = await Promise.all([
        (0, query_js_1.runQuery)(mainSQL, 2000),
        (0, query_js_1.runQuery)(icebergSQL, 10),
        (0, query_js_1.runQuery)(retentionSQL, 10),
        includeTimeline ? (0, query_js_1.runQuery)(timelineSQL, 200) : Promise.resolve(null),
    ]);
    const buckets = {
        artefact: [],
        pivot: [],
        conversational: [],
        tracker_probe: [],
        agent_harness: [],
        pasted_string: [],
        long_uncategorised: [],
    };
    let excludedOrdinary = 0;
    for (const row of main.rows) {
        const query = String(row.query);
        const bucket = classifyQuery(query);
        if (!bucket) {
            excludedOrdinary++;
            continue;
        }
        const impressions = Number(row.impressions) || 0;
        if (impressions < minImpressions)
            continue;
        const clicks = Number(row.clicks) || 0;
        const topPages = Array.isArray(row.top_pages)
            ? row.top_pages.map((p) => ({
                page: String(p.page),
                impressions: Number(p.impressions) || 0,
            }))
            : [];
        buckets[bucket].push({
            query,
            impressions,
            clicks,
            ctr: impressions ? Math.round((clicks / impressions) * 10000) / 100 : 0,
            position: Number(row.position) || 0,
            top_pages: topPages,
        });
    }
    const summary = {};
    for (const b of BUCKET_ORDER) {
        buckets[b].sort((x, y) => y.impressions - x.impressions || x.query.localeCompare(y.query));
        summary[b] = {
            queries: buckets[b].length,
            impressions: buckets[b].reduce((s, r) => s + r.impressions, 0),
            clicks: buckets[b].reduce((s, r) => s + r.clicks, 0),
        };
    }
    const ice = iceberg.rows[0] || {};
    const anonImpr = Number(ice.anonymised_impressions) || 0;
    const visImpr = Number(ice.visible_impressions) || 0;
    return {
        window: {
            start: ice.window_start ?? null,
            end: ice.window_end ?? null,
            days_requested: days,
            note: "Window anchors to the export's latest data date, which runs roughly two days behind today.",
        },
        export_retention: retention.rows[0] || null,
        summary,
        total_conversation_queries: Object.values(summary).reduce((s, b) => s + b.queries, 0),
        total_conversation_impressions: Object.values(summary).reduce((s, b) => s + b.impressions, 0),
        excluded_ordinary_matches: excludedOrdinary,
        anonymised_visibility: {
            anonymised_impressions: anonImpr,
            visible_impressions: visImpr,
            anonymised_share_pct: anonImpr + visImpr
                ? Math.round((anonImpr / (anonImpr + visImpr)) * 1000) / 10
                : null,
            anonymised_clicks: Number(ice.anonymised_clicks) || 0,
            visible_clicks: Number(ice.visible_clicks) || 0,
            distinct_visible_queries: Number(ice.distinct_visible_queries) || 0,
            meaning: "Impressions whose query Google anonymised as too rare. Conversations are almost by definition rare strings, so most of the conversation iceberg sits in this pool; the buckets above are its visible tip.",
        },
        buckets: Object.fromEntries(BUCKET_ORDER.map((b) => [
            b,
            {
                meaning: BUCKET_MEANING[b],
                total_matched: buckets[b].length,
                rows: buckets[b].slice(0, maxRowsPerBucket),
            },
        ])),
        artefact_timeline_monthly: timeline ? timeline.rows : undefined,
        what_this_cannot_see: CANNOT_SEE,
    };
}
