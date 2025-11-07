import client from './api/client';
import {
  SapHanaSettings,
  SapHanaSettingsInput,
  SapHanaSettingsTestResult,
  SapHanaSettingsUpdate
} from '../types/data';

interface SapHanaSettingsResponse {
  id: string;
  displayName: string;
  host: string;
  port: number;
  databaseName: string;
  username: string;
  schemaName?: string | null;
  tenant?: string | null;
  useSsl: boolean;
  ingestionBatchRows?: number | null;
  isActive: boolean;
  hasPassword: boolean;
  createdAt?: string;
  updatedAt?: string;
}

interface SapHanaSettingsTestResponse {
  success: boolean;
  message: string;
  durationMs?: number | null;
}

const mapSapHanaSettings = (payload: SapHanaSettingsResponse): SapHanaSettings => ({
  id: payload.id,
  displayName: payload.displayName,
  host: payload.host,
  port: payload.port,
  databaseName: payload.databaseName,
  username: payload.username,
  schemaName: payload.schemaName ?? null,
  tenant: payload.tenant ?? null,
  useSsl: payload.useSsl,
  ingestionBatchRows: payload.ingestionBatchRows ?? null,
  isActive: payload.isActive,
  hasPassword: payload.hasPassword,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchSapHanaSettings = async (): Promise<SapHanaSettings | null> => {
  const response = await client.get<SapHanaSettingsResponse | null>('/sap-hana/settings');
  return response.data ? mapSapHanaSettings(response.data) : null;
};

export const createSapHanaSettings = async (
  input: SapHanaSettingsInput
): Promise<SapHanaSettings> => {
  const response = await client.post<SapHanaSettingsResponse>('/sap-hana/settings', {
    display_name: input.displayName ?? 'SAP HANA Warehouse',
    host: input.host,
    port: input.port,
    database_name: input.databaseName,
    username: input.username,
    password: input.password,
    schema_name: input.schemaName ?? null,
    tenant: input.tenant ?? null,
    use_ssl: input.useSsl,
    ingestion_batch_rows: input.ingestionBatchRows ?? null
  });
  return mapSapHanaSettings(response.data);
};

export const updateSapHanaSettings = async (
  id: string,
  input: SapHanaSettingsUpdate
): Promise<SapHanaSettings> => {
  const response = await client.put<SapHanaSettingsResponse>(`/sap-hana/settings/${id}`, {
    ...(input.displayName !== undefined ? { display_name: input.displayName } : {}),
    ...(input.host !== undefined ? { host: input.host } : {}),
    ...(input.port !== undefined ? { port: input.port } : {}),
    ...(input.databaseName !== undefined ? { database_name: input.databaseName } : {}),
    ...(input.username !== undefined ? { username: input.username } : {}),
    ...(input.password !== undefined ? { password: input.password } : {}),
    ...(input.schemaName !== undefined ? { schema_name: input.schemaName } : {}),
    ...(input.tenant !== undefined ? { tenant: input.tenant } : {}),
    ...(input.useSsl !== undefined ? { use_ssl: input.useSsl } : {}),
    ...(input.ingestionBatchRows !== undefined
      ? { ingestion_batch_rows: input.ingestionBatchRows }
      : {}),
    ...(input.isActive !== undefined ? { is_active: input.isActive } : {})
  });
  return mapSapHanaSettings(response.data);
};

export const testSapHanaSettings = async (
  input: SapHanaSettingsInput
): Promise<SapHanaSettingsTestResult> => {
  const response = await client.post<SapHanaSettingsTestResponse>('/sap-hana/settings/test', {
    host: input.host,
    port: input.port,
    database_name: input.databaseName,
    username: input.username,
    password: input.password,
    schema_name: input.schemaName ?? null,
    tenant: input.tenant ?? null,
    use_ssl: input.useSsl,
    ingestion_batch_rows: input.ingestionBatchRows ?? null
  });

  return {
    success: response.data.success,
    message: response.data.message,
    durationMs: response.data.durationMs ?? null
  };
};
