import client from './api/client';
import {
  DatabricksSqlSettings,
  DatabricksSqlSettingsInput,
  DatabricksSqlSettingsTestResult,
  DatabricksSqlSettingsUpdate
} from '../types/data';

interface DatabricksSettingsResponse {
  id: string;
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  ingestionBatchRows?: number | null;
  ingestionMethod?: 'sql' | 'spark';
  warehouseName?: string | null;
  isActive: boolean;
  hasAccessToken: boolean;
  createdAt?: string;
  updatedAt?: string;
}

interface DatabricksSettingsTestPayload {
  workspace_host: string;
  http_path: string;
  access_token: string;
  catalog?: string | null;
  schema_name?: string | null;
  constructed_schema?: string | null;
  ingestion_batch_rows?: number | null;
  ingestion_method?: 'sql' | 'spark';
}

interface DatabricksSettingsTestResponse {
  success: boolean;
  message: string;
  durationMs?: number | null;
}

const mapDatabricksSettings = (
  payload: DatabricksSettingsResponse
): DatabricksSqlSettings => ({
  id: payload.id,
  displayName: payload.displayName,
  workspaceHost: payload.workspaceHost,
  httpPath: payload.httpPath,
  catalog: payload.catalog ?? null,
  schemaName: payload.schemaName ?? null,
  constructedSchema: payload.constructedSchema ?? null,
  ingestionBatchRows: payload.ingestionBatchRows ?? null,
  ingestionMethod: payload.ingestionMethod ?? 'sql',
  warehouseName: payload.warehouseName ?? null,
  isActive: payload.isActive,
  hasAccessToken: payload.hasAccessToken,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchDatabricksSettings = async (): Promise<DatabricksSqlSettings | null> => {
  const response = await client.get<DatabricksSettingsResponse | null>('/databricks/settings');
  return response.data ? mapDatabricksSettings(response.data) : null;
};

export const createDatabricksSettings = async (
  input: DatabricksSqlSettingsInput
): Promise<DatabricksSqlSettings> => {
  const response = await client.post<DatabricksSettingsResponse>('/databricks/settings', {
    display_name: input.displayName,
    workspace_host: input.workspaceHost,
    http_path: input.httpPath,
    access_token: input.accessToken,
    catalog: input.catalog ?? null,
    schema_name: input.schemaName ?? null,
    constructed_schema: input.constructedSchema ?? null,
    ingestion_batch_rows: input.ingestionBatchRows ?? null,
    ingestion_method: input.ingestionMethod ?? 'sql',
    warehouse_name: input.warehouseName ?? null
  });
  return mapDatabricksSettings(response.data);
};

export const updateDatabricksSettings = async (
  id: string,
  input: DatabricksSqlSettingsUpdate
): Promise<DatabricksSqlSettings> => {
  const response = await client.put<DatabricksSettingsResponse>(`/databricks/settings/${id}`, {
    ...(input.displayName !== undefined ? { display_name: input.displayName } : {}),
    ...(input.workspaceHost !== undefined ? { workspace_host: input.workspaceHost } : {}),
    ...(input.httpPath !== undefined ? { http_path: input.httpPath } : {}),
    ...(input.catalog !== undefined ? { catalog: input.catalog } : {}),
    ...(input.schemaName !== undefined ? { schema_name: input.schemaName } : {}),
    ...(input.constructedSchema !== undefined
      ? { constructed_schema: input.constructedSchema }
      : {}),
    ...(input.ingestionBatchRows !== undefined ? { ingestion_batch_rows: input.ingestionBatchRows } : {}),
    ...(input.ingestionMethod !== undefined ? { ingestion_method: input.ingestionMethod } : {}),
    ...(input.warehouseName !== undefined ? { warehouse_name: input.warehouseName } : {}),
    ...(input.accessToken !== undefined ? { access_token: input.accessToken } : {}),
    ...(input.isActive !== undefined ? { is_active: input.isActive } : {})
  });
  return mapDatabricksSettings(response.data);
};

export const testDatabricksSettings = async (
  input: DatabricksSqlSettingsInput
): Promise<DatabricksSqlSettingsTestResult> => {
  const payload: DatabricksSettingsTestPayload = {
    workspace_host: input.workspaceHost,
    http_path: input.httpPath,
    access_token: input.accessToken,
    catalog: input.catalog ?? null,
    schema_name: input.schemaName ?? null,
    constructed_schema: input.constructedSchema ?? null,
    ingestion_batch_rows: input.ingestionBatchRows ?? null,
    ingestion_method: input.ingestionMethod ?? 'sql'
  };
  const response = await client.post<DatabricksSettingsTestResponse>(
    '/databricks/settings/test',
    payload
  );

  return {
    success: response.data.success,
    message: response.data.message,
    durationMs: response.data.durationMs ?? null
  };
};
