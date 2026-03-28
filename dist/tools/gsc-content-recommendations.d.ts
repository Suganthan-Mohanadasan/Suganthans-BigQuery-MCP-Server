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
export declare function gscContentRecommendations(days?: number, maxRecommendations?: number, dataset?: string): Promise<RecommendationResult>;
export {};
