import client from './api/client';
import {
  DatabricksSqlSettings,
  DatabricksSqlSettingsInput,
  DatabricksSqlSettingsTestResult,
  DatabricksSqlSettingsUpdate,
  DatabricksClusterPolicy
} from '../types/data';

interface DatabricksSettingsResponse {
  id: string;
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  dataQualitySchema?: string | null;
  dataQualityStorageFormat: 'delta' | 'hudi';
  dataQualityAutoManageTables: boolean;
  profilingPolicyId?: string | null;
  profilingNotebookPath?: string | null;
  ingestionBatchRows?: number | null;
  ingestionMethod?: 'sql' | 'spark';
  sparkCompute?: 'classic' | 'serverless' | null;
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
  data_quality_schema?: string | null;
  data_quality_storage_format?: 'delta' | 'hudi';
  data_quality_auto_manage_tables?: boolean;
  profiling_policy_id?: string | null;
  profiling_notebook_path?: string | null;
  ingestion_batch_rows?: number | null;
  ingestion_method?: 'sql' | 'spark';
  spark_compute?: 'classic' | 'serverless' | null;
}

interface DatabricksSettingsTestResponse {
  success: boolean;
  message: string;
  durationMs?: number | null;
}

interface DatabricksClusterPolicyResponse {
  id: string;
  settingId: string;
  policyId: string;
  name: string;
  description?: string | null;
  definition?: Record<string, unknown> | null;
  isActive: boolean;
  syncedAt?: string | null;
  createdAt?: string;
  updatedAt?: string;
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
  dataQualitySchema: payload.dataQualitySchema ?? null,
  dataQualityStorageFormat: payload.dataQualityStorageFormat,
  dataQualityAutoManageTables: payload.dataQualityAutoManageTables,
  profilingPolicyId: payload.profilingPolicyId ?? null,
  profilingNotebookPath: payload.profilingNotebookPath ?? null,
  ingestionBatchRows: payload.ingestionBatchRows ?? null,
  ingestionMethod: payload.ingestionMethod ?? 'sql',
  sparkCompute: payload.sparkCompute ?? null,
  warehouseName: payload.warehouseName ?? null,
  isActive: payload.isActive,
  hasAccessToken: payload.hasAccessToken,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

const mapClusterPolicy = (
  payload: DatabricksClusterPolicyResponse
): DatabricksClusterPolicy => ({
  id: payload.id,
  settingId: payload.settingId,
  policyId: payload.policyId,
  name: payload.name,
  description: payload.description ?? null,
  definition: payload.definition ?? null,
  isActive: payload.isActive,
  syncedAt: payload.syncedAt ?? null,
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
    data_quality_schema: input.dataQualitySchema ?? null,
    data_quality_storage_format: input.dataQualityStorageFormat,
    data_quality_auto_manage_tables: input.dataQualityAutoManageTables,
    profiling_policy_id: input.profilingPolicyId ?? null,
    profiling_notebook_path: input.profilingNotebookPath ?? null,
    ingestion_batch_rows: input.ingestionBatchRows ?? null,
    ingestion_method: input.ingestionMethod ?? 'sql',
    spark_compute: input.sparkCompute ?? 'classic',
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
    ...(input.dataQualitySchema !== undefined ? { data_quality_schema: input.dataQualitySchema } : {}),
    ...(input.dataQualityStorageFormat !== undefined
      ? { data_quality_storage_format: input.dataQualityStorageFormat }
      : {}),
    ...(input.dataQualityAutoManageTables !== undefined
      ? { data_quality_auto_manage_tables: input.dataQualityAutoManageTables }
      : {}),
    ...(input.profilingPolicyId !== undefined ? { profiling_policy_id: input.profilingPolicyId } : {}),
    ...(input.profilingNotebookPath !== undefined
      ? { profiling_notebook_path: input.profilingNotebookPath }
      : {}),
    ...(input.ingestionBatchRows !== undefined ? { ingestion_batch_rows: input.ingestionBatchRows } : {}),
    ...(input.ingestionMethod !== undefined ? { ingestion_method: input.ingestionMethod } : {}),
    ...(input.sparkCompute !== undefined ? { spark_compute: input.sparkCompute } : {}),
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
    data_quality_schema: input.dataQualitySchema ?? null,
    data_quality_storage_format: input.dataQualityStorageFormat,
    data_quality_auto_manage_tables: input.dataQualityAutoManageTables,
    profiling_policy_id: input.profilingPolicyId ?? null,
    profiling_notebook_path: input.profilingNotebookPath ?? null,
    ingestion_batch_rows: input.ingestionBatchRows ?? null,
    ingestion_method: input.ingestionMethod ?? 'sql',
    spark_compute: input.sparkCompute ?? 'classic'
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

export const fetchDatabricksClusterPolicies = async (): Promise<DatabricksClusterPolicy[]> => {
  const response = await client.get<DatabricksClusterPolicyResponse[]>(
    '/databricks/settings/policies'
  );
  return response.data.map(mapClusterPolicy);
};

export const syncDatabricksClusterPolicies = async (): Promise<DatabricksClusterPolicy[]> => {
  const response = await client.post<DatabricksClusterPolicyResponse[]>(
    '/databricks/settings/policies/sync'
  );
  return response.data.map(mapClusterPolicy);
};

export const deleteDatabricksSettings = async (id: string): Promise<void> => {
  await client.delete(`/databricks/settings/${id}`);
};
