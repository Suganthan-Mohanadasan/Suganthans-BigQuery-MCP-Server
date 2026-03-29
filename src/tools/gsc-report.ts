import { gscSiteSnapshot } from "./gsc-site-snapshot.js";
import { gscQuickWins } from "./gsc-quick-wins.js";
import { gscTrafficDrops } from "./gsc-traffic-drops.js";
import { gscContentDecay } from "./gsc-content-decay.js";
import { gscAlerts } from "./gsc-alerts.js";
import { gscContentRecommendations } from "./gsc-content-recommendations.js";

const ALL_SECTIONS = [
  "snapshot",
  "alerts",
  "quick_wins",
  "traffic_drops",
  "content_decay",
  "recommendations",
];

interface ReportResult {
  markdown: string;
  sectionsIncluded: string[];
  summary: string;
}

export async function gscReport(
  days: number = 28,
  includeSections?: string[],
  dataset?: string
): Promise<ReportResult> {
  const sections = includeSections && includeSections.length > 0
    ? includeSections.filter((s) => ALL_SECTIONS.includes(s))
    : ALL_SECTIONS;

  const date = new Date().toISOString().split("T")[0];

  // Run all requested sections in parallel
  const promises: Record<string, Promise<unknown>> = {};
  if (sections.includes("snapshot")) promises.snapshot = gscSiteSnapshot(days, dataset);
  if (sections.includes("alerts")) promises.alerts = gscAlerts(7, 20, 50, 30, dataset);
  if (sections.includes("quick_wins")) promises.quick_wins = gscQuickWins(days, 50, 15, dataset);
  if (sections.includes("traffic_drops")) promises.traffic_drops = gscTrafficDrops(days, dataset);
  if (sections.includes("content_decay")) promises.content_decay = gscContentDecay(dataset);
  if (sections.includes("recommendations")) promises.recommendations = gscContentRecommendations(days, 10, dataset);

  const results: Record<string, unknown> = {};
  const keys = Object.keys(promises);
  const settled = await Promise.allSettled(Object.values(promises));
  keys.forEach((key, i) => {
    if (settled[i].status === "fulfilled") {
      results[key] = (settled[i] as PromiseFulfilledResult<unknown>).value;
    }
    // Rejected sections are silently omitted from the report
  });

  const lines: string[] = [];
  lines.push(`# GSC Performance Report (BigQuery)`);
  lines.push(`**Date:** ${date}`);
  lines.push(`**Period:** ${days} days`);
  lines.push(`**Source:** BigQuery bulk export data`);
  lines.push("");

  // Snapshot
  if (results.snapshot) {
    const s = results.snapshot as { rows: Record<string, unknown>[] };
    if (s.rows.length > 0) {
      const d = s.rows[0];
      lines.push(`## Site Snapshot`);
      lines.push("");
      lines.push(`| Metric | Current | Prior | Change |`);
      lines.push(`|--------|---------|-------|--------|`);
      lines.push(`| Clicks | ${d.current_clicks} | ${d.prior_clicks} | ${Number(d.click_change_pct) > 0 ? "+" : ""}${d.click_change_pct}% |`);
      lines.push(`| Impressions | ${d.current_impressions} | ${d.prior_impressions} | ${Number(d.impression_change_pct) > 0 ? "+" : ""}${d.impression_change_pct}% |`);
      lines.push(`| CTR | ${d.current_ctr}% | ${d.prior_ctr}% | ${Number(d.ctr_change) > 0 ? "+" : ""}${d.ctr_change} |`);
      lines.push(`| Position | ${d.current_position} | ${d.prior_position} | ${Number(d.position_change) > 0 ? "+" : ""}${d.position_change} |`);
      lines.push(`| Unique Pages | ${d.current_pages} | | |`);
      lines.push(`| Unique Queries | ${d.current_queries} | | |`);
      lines.push("");
    }
  }

  // Alerts
  if (results.alerts) {
    const a = results.alerts as { alerts: Record<string, unknown>[]; disappeared: Record<string, unknown>[]; summary: { total: number; critical: number; warning: number } };
    lines.push(`## Alerts (${a.summary.total} total: ${a.summary.critical} critical, ${a.summary.warning} warning)`);
    lines.push("");
    if (a.summary.total === 0) {
      lines.push("No alerts triggered. Everything looks healthy.");
    } else {
      for (const alert of a.alerts.slice(0, 20)) {
        lines.push(`- **[${String(alert.severity).toUpperCase()}]** ${alert.alert_type}: ${alert.query} on ${alert.url} (position ${alert.prev_position} → ${alert.curr_position})`);
      }
      for (const d of a.disappeared.slice(0, 10)) {
        lines.push(`- **[${String(d.severity).toUpperCase()}]** DISAPPEARED: ${d.query} on ${d.url} (had ${d.prior_clicks} clicks)`);
      }
    }
    lines.push("");
  }

  // Quick Wins
  if (results.quick_wins) {
    const w = results.quick_wins as { rows: Record<string, unknown>[]; totalRows: number };
    lines.push(`## Quick Wins (${w.totalRows} opportunities)`);
    lines.push("");
    if (w.rows.length > 0) {
      lines.push(`| Keyword | Position | Impressions | CTR | Opportunity |`);
      lines.push(`|---------|----------|-------------|-----|-------------|`);
      for (const r of w.rows.slice(0, 15)) {
        lines.push(`| ${r.query} | ${r.avg_position} | ${r.impressions} | ${r.ctr_pct}% | +${r.opportunity} clicks |`);
      }
    }
    lines.push("");
  }

  // Traffic Drops
  if (results.traffic_drops) {
    const t = results.traffic_drops as { rows: Record<string, unknown>[]; totalRows: number };
    lines.push(`## Traffic Drops (${t.totalRows} pages declining)`);
    lines.push("");
    if (t.rows.length > 0) {
      lines.push(`| Page | Current | Prior | Change | Diagnosis |`);
      lines.push(`|------|---------|-------|--------|-----------|`);
      for (const r of t.rows.slice(0, 15)) {
        lines.push(`| ${r.url} | ${r.curr_clicks} | ${r.prev_clicks} | ${r.click_change} | ${r.diagnosis} |`);
      }
    }
    lines.push("");
  }

  // Content Decay
  if (results.content_decay) {
    const c = results.content_decay as { rows: Record<string, unknown>[]; totalRows: number };
    lines.push(`## Content Decay (${c.totalRows} pages with 3-month decline)`);
    lines.push("");
    if (c.rows.length > 0) {
      lines.push(`| Page | 3 Months Ago | 2 Months Ago | Last Month | Decline |`);
      lines.push(`|------|-------------|-------------|------------|---------|`);
      for (const r of c.rows.slice(0, 15)) {
        lines.push(`| ${r.url} | ${r.clicks_3_months_ago} | ${r.clicks_2_months_ago} | ${r.clicks_last_month} | ${r.total_decline_pct}% |`);
      }
    }
    lines.push("");
  }

  // Recommendations
  if (results.recommendations) {
    const recs = results.recommendations as { recommendations: Array<{ priority: number; action: string; target: string; estimatedOpportunity: number; reasoning: string }>; summary: { totalOpportunity: number } };
    lines.push(`## Recommendations (${recs.recommendations.length} actions, ~${recs.summary.totalOpportunity} potential clicks)`);
    lines.push("");
    for (const rec of recs.recommendations) {
      lines.push(`### ${rec.priority}. [${rec.action.toUpperCase()}] ${rec.target}`);
      lines.push(rec.reasoning);
      lines.push("");
    }
  }

  lines.push("---");
  lines.push(`*Generated by BigQuery MCP Server*`);

  const markdown = lines.join("\n");

  const summaryParts: string[] = [];
  if (results.alerts) {
    const a = results.alerts as { summary: { total: number } };
    summaryParts.push(`${a.summary.total} alerts`);
  }
  if (results.quick_wins) {
    const w = results.quick_wins as { totalRows: number };
    summaryParts.push(`${w.totalRows} quick wins`);
  }
  if (results.recommendations) {
    const r = results.recommendations as { recommendations: unknown[] };
    summaryParts.push(`${r.recommendations.length} recommendations`);
  }

  return {
    markdown,
    sectionsIncluded: sections,
    summary: `Report generated with ${sections.length} sections. ${summaryParts.join(". ")}.`,
  };
}
