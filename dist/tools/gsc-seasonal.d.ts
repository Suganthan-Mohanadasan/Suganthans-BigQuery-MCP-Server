/**
 * Monatsvergleich Jahr ueber Jahr.
 *
 * Der erste und der letzte Monat des Exports sind fast immer unvollstaendig. Sie
 * als ganze Monate neben vollen Monaten zu zeigen erzeugt einen scheinbaren
 * Einbruch, wo nur Daten fehlen - gemessen: April mit 17 Exporttagen stand mit
 * 95.795 Klicks neben einem vollen Juni mit 599.284. Solche Monate werden
 * deshalb standardmaessig ausgeschlossen und in droppedPartialMonths benannt,
 * nie stillschweigend weggelassen.
 */
export declare function gscSeasonal(includePartialMonths?: boolean, dataset?: string): Promise<{
    rows: Record<string, unknown>[];
    totalRows: number;
    bytesProcessed: string;
    droppedPartialMonths: string[];
    note: string;
}>;
