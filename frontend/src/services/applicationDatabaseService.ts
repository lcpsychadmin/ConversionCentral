import client from './api/client';
import {
  ApplicationDatabaseApplyInput,
  ApplicationDatabaseSetting,
  ApplicationDatabaseStatus,
  ApplicationDatabaseTestResult
} from '../types/data';

export const APPLICATION_DATABASE_STATUS_QUERY_KEY = ['application-database', 'status'] as const;

interface ApplicationDatabaseSettingResponse {
  id: string;
  engine: 'default_postgres' | 'custom_postgres' | 'sqlserver';
  connection_display?: string | null;
  applied_at: string;
  display_name?: string | null;
  created_at?: string;
  updated_at?: string;
}

interface ApplicationDatabaseStatusResponse {
  configured: boolean;
  setting: ApplicationDatabaseSettingResponse | null;
  admin_email?: string | null;
}

interface ApplicationDatabaseTestResponse {
  success: boolean;
  message: string;
  latency_ms?: number | null;
}

const mapSetting = (payload: ApplicationDatabaseSettingResponse): ApplicationDatabaseSetting => ({
  id: payload.id,
  engine: payload.engine,
  connectionDisplay: payload.connection_display ?? null,
  appliedAt: payload.applied_at,
  displayName: payload.display_name ?? null,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

const mapStatus = (payload: ApplicationDatabaseStatusResponse): ApplicationDatabaseStatus => ({
  configured: payload.configured,
  setting: payload.setting ? mapSetting(payload.setting) : null,
  adminEmail: payload.admin_email ?? null
});

const normalizeConnectionPayload = (
  input: ApplicationDatabaseApplyInput['connection']
): Record<string, unknown> | undefined => {
  if (!input) {
    return undefined;
  }

  const payload: Record<string, unknown> = {};
  if (input.host) {
    payload.host = input.host.trim();
  }
  if (input.port !== undefined && input.port !== null) {
    payload.port = input.port;
  }
  if (input.database) {
    payload.database = input.database.trim();
  }
  if (input.username) {
    payload.username = input.username.trim();
  }
  if (input.password) {
    payload.password = input.password;
  }
  if (input.options) {
    payload.options = input.options;
  }
  if (input.useSsl !== undefined) {
    payload.use_ssl = input.useSsl;
  }

  return Object.keys(payload).length > 0 ? payload : undefined;
};

const buildApplyPayload = (input: ApplicationDatabaseApplyInput) => {
  const payload: Record<string, unknown> = {
    engine: input.engine
  };

  if (input.displayName !== undefined) {
    payload.display_name = input.displayName;
  }

  const connectionPayload = normalizeConnectionPayload(input.connection ?? null);
  if (connectionPayload) {
    payload.connection = connectionPayload;
  }

  return payload;
};

export const fetchApplicationDatabaseStatus = async (): Promise<ApplicationDatabaseStatus> => {
  const response = await client.get<ApplicationDatabaseStatusResponse>('/application-database/status');
  return mapStatus(response.data);
};

export const testApplicationDatabase = async (
  input: ApplicationDatabaseApplyInput
): Promise<ApplicationDatabaseTestResult> => {
  const payload = buildApplyPayload(input);
  const response = await client.post<ApplicationDatabaseTestResponse>('/application-database/test', payload);
  return {
    success: response.data.success,
    message: response.data.message,
    latencyMs: response.data.latency_ms ?? null
  };
};

export const applyApplicationDatabaseSetting = async (
  input: ApplicationDatabaseApplyInput
): Promise<ApplicationDatabaseSetting> => {
  const payload = buildApplyPayload(input);
  const response = await client.post<ApplicationDatabaseSettingResponse>(
    '/application-database/apply',
    payload
  );
  return mapSetting(response.data);
};
