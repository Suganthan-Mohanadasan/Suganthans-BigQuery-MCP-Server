import { runQuery } from "./query.js";
import { getConfig, validateIdentifier } from "../client.js";

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
export async function gscSeasonal(
  includePartialMonths: boolean = false,
  dataset?: string
): Promise<{
  rows: Record<string, unknown>[];
  totalRows: number;
  bytesProcessed: string;
  droppedPartialMonths: string[];
  note: string;
}> {
  const config = getConfig();
  const ds = dataset || config.defaultDataset || "searchconsole";
  validateIdentifier(ds, "dataset");

  const sql = `
    WITH monthly AS (
      SELECT
        EXTRACT(YEAR FROM data_date) AS year,
        EXTRACT(MONTH FROM data_date) AS month,
        FORMAT_DATE('%b', data_date) AS month_name,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr_pct,
        ROUND(SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1, 1) AS avg_position,
        COUNT(DISTINCT data_date) AS days_with_data,
        EXTRACT(DAY FROM LAST_DAY(DATE_TRUNC(data_date, MONTH))) AS days_in_month
      FROM \`${ds}.searchdata_url_impression\`
      WHERE search_type = 'WEB'
      GROUP BY 1, 2, 3, days_in_month
    ),
    with_yoy AS (
      SELECT
        m.*,
        LAG(clicks) OVER (PARTITION BY month ORDER BY year) AS prev_year_clicks,
        ROUND(SAFE_DIVIDE(
          clicks - LAG(clicks) OVER (PARTITION BY month ORDER BY year),
          LAG(clicks) OVER (PARTITION BY month ORDER BY year)
        ) * 100, 1) AS yoy_change_pct
      FROM monthly m
    )
    SELECT
      *,
      days_with_data = days_in_month AS is_complete_month
    FROM with_yoy
    ORDER BY year DESC, month DESC
  `;

  const result = await runQuery(sql, 100);

  const dropped: string[] = [];
  const rows = result.rows.filter((row) => {
    if (row.is_complete_month) return true;
    dropped.push(
      `${row.year}-${String(row.month).padStart(2, "0")} (${row.days_with_data} von ${row.days_in_month} Tagen)`
    );
    return includePartialMonths;
  });

  return {
    ...result,
    rows,
    totalRows: rows.length,
    droppedPartialMonths: dropped,
    note: dropped.length
      ? includePartialMonths
        ? `Unvollstaendige Monate sind enthalten und nicht mit vollen Monaten vergleichbar: ${dropped.join(", ")}.`
        : `Unvollstaendige Monate ausgeschlossen: ${dropped.join(", ")}. Mit include_partial_months lassen sie sich einblenden.`
      : "Alle zurueckgegebenen Monate sind vollstaendig.",
  };
}
