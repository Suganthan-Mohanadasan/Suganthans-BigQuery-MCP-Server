import { gscQuickWins } from "./gsc-quick-wins.js";
import { gscContentGaps } from "./gsc-content-gaps.js";
import { gscCannibalisation } from "./gsc-cannibalisation.js";

interface Recommendation {
  priority: number;
  action: "update" | "create" | "consolidate";
  target: string;
  estimatedOpportunity: number;
  reasoning: string;
  relatedUrls?: string[];
}

interface RecommendationResult {
  recommendations: Recommendation[];
  summary: {
    update: number;
    create: number;
    consolidate: number;
    totalOpportunity: number;
  };
}

export async function gscContentRecommendations(
  days: number = 28,
  maxRecommendations: number = 10,
  dataset?: string
): Promise<RecommendationResult> {
  const [winsResult, gapsResult, cannibResult] = await Promise.allSettled([
    gscQuickWins(days, 50, 15, dataset),
    gscContentGaps(90, 30, 20, dataset),
    gscCannibalisation(days, 30, dataset),
  ]);

  const emptyResult = { rows: [], totalRows: 0, bytesProcessed: "0 bytes" };
  const wins = winsResult.status === "fulfilled" ? winsResult.value : emptyResult;
  const gaps = gapsResult.status === "fulfilled" ? gapsResult.value : emptyResult;
  const cannib = cannibResult.status === "fulfilled" ? cannibResult.value : emptyResult;

  const recs: Recommendation[] = [];

  // Update recommendations from quick wins
  for (const win of wins.rows.slice(0, 20)) {
    const w = win as Record<string, unknown>;
    recs.push({
      priority: 0,
      action: "update",
      target: String(w.query || ""),
      estimatedOpportunity: Number(w.opportunity || 0),
      reasoning: `Ranking at position ${w.avg_position} with ${w.impressions} impressions. ` +
        `Current CTR: ${w.ctr_pct}%. Moving to page one could gain ~${w.opportunity} extra clicks. ` +
        `Optimise content, internal links, and on-page SEO.`,
    });
  }

  // Create recommendations from content gaps
  for (const gap of gaps.rows.slice(0, 20)) {
    const g = gap as Record<string, unknown>;
    const estimatedClicks = Number(g.estimated_clicks_at_pos5 || 0);
    recs.push({
      priority: 0,
      action: "create",
      target: String(g.query || ""),
      estimatedOpportunity: estimatedClicks,
      reasoning: `${g.impressions} impressions but ranking at position ${g.avg_position}. ` +
        `No page properly targets this query. Creating dedicated content could capture ` +
        `~${estimatedClicks} clicks/month.`,
    });
  }

  // Consolidate recommendations from cannibalisation
  const queryGroups = new Map<string, Record<string, unknown>[]>();
  for (const row of cannib.rows) {
    const r = row as Record<string, unknown>;
    const query = String(r.query || "");
    if (!queryGroups.has(query)) queryGroups.set(query, []);
    queryGroups.get(query)!.push(r);
  }

  for (const [query, pages] of queryGroups) {
    if (pages.length < 2) continue;
    const totalImpressions = pages.reduce((sum, p) => sum + Number(p.impressions || 0), 0);
    const estimatedGain = Math.round(totalImpressions * 0.05);
    const bestPage = pages[0]; // already sorted by position
    const otherUrls = pages.slice(1).map((p) => String(p.url || ""));

    recs.push({
      priority: 0,
      action: "consolidate",
      target: query,
      estimatedOpportunity: estimatedGain,
      reasoning: `${pages.length} pages compete for "${query}" (${totalImpressions} impressions). ` +
        `Best page ranks at position ${bestPage.avg_position}. Consolidating to one authoritative page ` +
        `and redirecting others could capture ~${estimatedGain} additional clicks.`,
      relatedUrls: [String(bestPage.url || ""), ...otherUrls],
    });
  }

  recs.sort((a, b) => b.estimatedOpportunity - a.estimatedOpportunity);

  const final = recs.slice(0, maxRecommendations).map((rec, i) => ({
    ...rec,
    priority: i + 1,
  }));

  return {
    recommendations: final,
    summary: {
      update: final.filter((r) => r.action === "update").length,
      create: final.filter((r) => r.action === "create").length,
      consolidate: final.filter((r) => r.action === "consolidate").length,
      totalOpportunity: final.reduce((sum, r) => sum + r.estimatedOpportunity, 0),
    },
  };
}
