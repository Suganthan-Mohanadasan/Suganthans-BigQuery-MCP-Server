interface ReportResult {
    markdown: string;
    sectionsIncluded: string[];
    summary: string;
}
export declare function gscReport(days?: number, includeSections?: string[], dataset?: string): Promise<ReportResult>;
export {};
