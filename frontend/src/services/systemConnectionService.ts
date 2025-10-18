import client from './api/client';
import {
  SystemConnection,
  SystemConnectionAuthMethod,
  SystemConnectionInput,
  SystemConnectionType,
  SystemConnectionUpdateInput
} from '../types/data';

export interface SystemConnectionResponse {
  id: string;
  system_id: string;
  connection_type: SystemConnectionType;
  connection_string: string;
  auth_method: SystemConnectionAuthMethod;
  active: boolean;
  notes?: string | null;
  created_at?: string;
  updated_at?: string;
}

export interface SystemConnectionTestResponse {
  success: boolean;
  message: string;
  duration_ms?: number | null;
  connection_summary?: string | null;
}

export interface SystemConnectionTestPayload {
  connectionType: SystemConnectionType;
  connectionString: string;
}

export interface SystemConnectionTestResult {
  success: boolean;
  message: string;
  durationMs?: number;
  connectionSummary?: string;
}

export const mapSystemConnection = (payload: SystemConnectionResponse): SystemConnection => ({
  id: payload.id,
  systemId: payload.system_id,
  connectionType: payload.connection_type,
  connectionString: payload.connection_string,
  authMethod: payload.auth_method,
  active: payload.active,
  notes: payload.notes ?? null,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

export const fetchSystemConnections = async (): Promise<SystemConnection[]> => {
  const response = await client.get<SystemConnectionResponse[]>('/system-connections');
  return response.data.map(mapSystemConnection);
};

export const createSystemConnection = async (
  input: SystemConnectionInput
): Promise<SystemConnection> => {
  const response = await client.post<SystemConnectionResponse>('/system-connections', {
    system_id: input.systemId,
    connection_type: input.connectionType,
    connection_string: input.connectionString,
    auth_method: input.authMethod,
    active: input.active ?? true,
    notes: input.notes ?? null
  });
  return mapSystemConnection(response.data);
};

export const updateSystemConnection = async (
  id: string,
  input: SystemConnectionUpdateInput
): Promise<SystemConnection> => {
  const response = await client.put<SystemConnectionResponse>(`/system-connections/${id}`, {
    ...(input.systemId !== undefined ? { system_id: input.systemId } : {}),
    ...(input.connectionType !== undefined ? { connection_type: input.connectionType } : {}),
    ...(input.connectionString !== undefined ? { connection_string: input.connectionString } : {}),
    ...(input.authMethod !== undefined ? { auth_method: input.authMethod } : {}),
    ...(input.active !== undefined ? { active: input.active } : {}),
    ...(input.notes !== undefined ? { notes: input.notes } : {})
  });
  return mapSystemConnection(response.data);
};

export const deleteSystemConnection = async (id: string): Promise<void> => {
  await client.delete(`/system-connections/${id}`);
};

export const testSystemConnection = async (
  payload: SystemConnectionTestPayload
): Promise<SystemConnectionTestResult> => {
  const response = await client.post<SystemConnectionTestResponse>(
    '/system-connections/test',
    {
      connection_type: payload.connectionType,
      connection_string: payload.connectionString
    }
  );

  if (!response.data.success) {
    const error = new Error(response.data.message || 'Connection test failed.');
    (error as Error & { connectionSummary?: string }).connectionSummary =
      response.data.connection_summary ?? undefined;
    throw error;
  }

  return {
    success: response.data.success,
    message: response.data.message,
    durationMs: response.data.duration_ms ?? undefined,
    connectionSummary: response.data.connection_summary ?? undefined
  };
};
