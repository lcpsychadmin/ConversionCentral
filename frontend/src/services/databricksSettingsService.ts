import client from './api/client';
import {
  DatabricksSqlSettings,
  DatabricksSqlSettingsInput,
  DatabricksSqlSettingsTestResult,
  DatabricksSqlSettingsUpdate
} from '../types/data';

interface DatabricksSettingsResponse {
  id: string;
  display_name: string;
  workspace_host: string;
  http_path: string;
  catalog?: string | null;
  schema_name?: string | null;
  warehouse_name?: string | null;
  is_active: boolean;
  has_access_token: boolean;
  created_at?: string;
  updated_at?: string;
}

interface DatabricksSettingsTestPayload {
  workspace_host: string;
  http_path: string;
  access_token: string;
  catalog?: string | null;
  schema_name?: string | null;
}

interface DatabricksSettingsTestResponse {
  success: boolean;
  message: string;
  duration_ms?: number | null;
}

const mapDatabricksSettings = (
  payload: DatabricksSettingsResponse
): DatabricksSqlSettings => ({
  id: payload.id,
  displayName: payload.display_name,
  workspaceHost: payload.workspace_host,
  httpPath: payload.http_path,
  catalog: payload.catalog ?? null,
  schemaName: payload.schema_name ?? null,
  warehouseName: payload.warehouse_name ?? null,
  isActive: payload.is_active,
  hasAccessToken: payload.has_access_token,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
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
    schema_name: input.schemaName ?? null
  };
  const response = await client.post<DatabricksSettingsTestResponse>(
    '/databricks/settings/test',
    payload
  );

  return {
    success: response.data.success,
    message: response.data.message,
    durationMs: response.data.duration_ms ?? null
  };
};
