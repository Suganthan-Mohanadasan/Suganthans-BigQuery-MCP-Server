import { getBigQueryClient, getConfig, validateIdentifier } from "../client.js";

interface DatasetInfo {
  id: string;
  location: string;
  created: string;
}

export async function listDatasets(projectId?: string): Promise<DatasetInfo[]> {
  if (projectId) {
    validateIdentifier(projectId, "project_id");
  }

  const client = getBigQueryClient();
  const config = getConfig();
  const targetProject = projectId || config.projectId;

  const [datasets] = await client.getDatasets({ projectId: targetProject });

  return datasets.map((ds) => ({
    id: ds.id || "unknown",
    location: ds.metadata?.location || "unknown",
    created: ds.metadata?.creationTime
      ? new Date(parseInt(ds.metadata.creationTime)).toISOString()
      : "unknown",
  }));
}
