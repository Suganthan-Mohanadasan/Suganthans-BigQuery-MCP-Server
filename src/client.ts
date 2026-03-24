import { BigQuery } from "@google-cloud/bigquery";

let cachedClient: BigQuery | null = null;

export function getConfig() {
  const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const projectId = process.env.BIGQUERY_PROJECT_ID;
  const defaultDataset = process.env.BIGQUERY_DEFAULT_DATASET;

  if (!projectId) {
    throw new Error(
      "BIGQUERY_PROJECT_ID environment variable is required. Set it to your Google Cloud project ID."
    );
  }

  return { keyFile, projectId, defaultDataset };
}

export function getBigQueryClient(): BigQuery {
  if (cachedClient) return cachedClient;

  const { keyFile, projectId } = getConfig();

  const options: { projectId: string; keyFilename?: string } = { projectId };
  if (keyFile) {
    options.keyFilename = keyFile;
  }

  cachedClient = new BigQuery(options);
  return cachedClient;
}
