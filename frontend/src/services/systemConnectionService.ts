import client from './api/client';
import {
  ConnectionCatalogSelectionInput,
  ConnectionCatalogTable,
  ConnectionTablePreview,
  SystemConnection,
  SystemConnectionAuthMethod,
  SystemConnectionInput,
  SystemConnectionType,
  SystemConnectionUpdateInput
} from '../types/data';

export interface SystemConnectionResponse {
  id: string;
  displayName?: string | null;
  display_name?: string | null;
  systemId?: string | null;
  system_id?: string | null;
  connectionType: SystemConnectionType;
  connectionString: string;
  authMethod: SystemConnectionAuthMethod;
  active: boolean;
  notes?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SystemConnectionTestResponse {
  success: boolean;
  message: string;
  durationMs?: number | null;
  connectionSummary?: string | null;
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

interface ConnectionCatalogTableResponse {
  schemaName?: string | null;
  schema_name?: string | null;
  tableName?: string | null;
  table_name?: string | null;
  tableType?: string | null;
  table_type?: string | null;
  columnCount?: number | null;
  column_count?: number | null;
  estimatedRows?: number | null;
  estimated_rows?: number | null;
  selected: boolean;
  available: boolean;
  selectionId?: string | null;
  selection_id?: string | null;
}

interface ConnectionTablePreviewResponse {
  columns: string[];
  rows: Record<string, unknown>[];
}

const mapConnectionCatalogTable = (
  payload: ConnectionCatalogTableResponse
): ConnectionCatalogTable => ({
  schemaName: payload.schemaName ?? payload.schema_name ?? '',
  tableName: payload.tableName ?? payload.table_name ?? '',
  tableType: payload.tableType ?? payload.table_type ?? null,
  columnCount: payload.columnCount ?? payload.column_count ?? null,
  estimatedRows: payload.estimatedRows ?? payload.estimated_rows ?? null,
  selected: payload.selected,
  available: payload.available,
  selectionId: payload.selectionId ?? payload.selection_id ?? null
});

export const mapSystemConnection = (payload: SystemConnectionResponse): SystemConnection => ({
  id: payload.id,
  name: payload.displayName ?? payload.display_name ?? 'Connection',
  systemId: payload.systemId ?? payload.system_id ?? null,
  connectionType: payload.connectionType,
  connectionString: payload.connectionString,
  authMethod: payload.authMethod,
  active: payload.active,
  notes: payload.notes ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchSystemConnections = async (): Promise<SystemConnection[]> => {
  const response = await client.get<SystemConnectionResponse[]>('/system-connections');
  return response.data.map(mapSystemConnection);
};

export const createSystemConnection = async (
  input: SystemConnectionInput
): Promise<SystemConnection> => {
  const payload: Record<string, unknown> = {
    display_name: input.name ?? 'Connection',
    connection_type: input.connectionType,
    auth_method: input.authMethod,
    active: input.active ?? true,
    notes: input.notes ?? null
  };

  if (input.systemId !== undefined) {
    payload.system_id = input.systemId;
  }

  if (input.connectionString !== undefined) {
    payload.connection_string = input.connectionString;
  }

  const response = await client.post<SystemConnectionResponse>('/system-connections', payload);
  return mapSystemConnection(response.data);
};

export const updateSystemConnection = async (
  id: string,
  input: SystemConnectionUpdateInput
): Promise<SystemConnection> => {
  const response = await client.put<SystemConnectionResponse>(`/system-connections/${id}`, {
    ...(input.name !== undefined ? { display_name: input.name } : {}),
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
      response.data.connectionSummary ?? undefined;
    throw error;
  }

  return {
    success: response.data.success,
    message: response.data.message,
    durationMs: response.data.durationMs ?? undefined,
    connectionSummary: response.data.connectionSummary ?? undefined
  };
};

export const fetchSystemConnectionCatalog = async (
  id: string
): Promise<ConnectionCatalogTable[]> => {
  const response = await client.get<ConnectionCatalogTableResponse[]>(
    `/system-connections/${id}/catalog`
  );
  return response.data.map(mapConnectionCatalogTable);
};

export const updateSystemConnectionCatalogSelection = async (
  id: string,
  selected: ConnectionCatalogSelectionInput[]
): Promise<void> => {
  await client.put(`/system-connections/${id}/catalog/selection`, {
    selected_tables: selected.map((table) => ({
      schema_name: table.schemaName,
      table_name: table.tableName,
      table_type: table.tableType ?? null,
      column_count: table.columnCount ?? null,
      estimated_rows: table.estimatedRows ?? null
    }))
  });
};

export const fetchConnectionTablePreview = async (
  id: string,
  schemaName: string | null,
  tableName: string,
  limit = 100
): Promise<ConnectionTablePreview> => {
  const response = await client.get<ConnectionTablePreviewResponse>(
    `/system-connections/${id}/catalog/preview`,
    {
      params: {
        schema_name: schemaName ?? undefined,
        table_name: tableName,
        limit
      }
    }
  );

  const { columns, rows } = response.data;
  const remapKey = (key: string): string => key.replace(/_([a-z])/g, (_, letter: string) => letter.toUpperCase());
  const normalizedRows = (rows ?? []).map((row) => {
    const source = row as Record<string, unknown>;
    const normalized: Record<string, unknown> = {};
    for (const column of columns) {
      if (Object.prototype.hasOwnProperty.call(source, column)) {
        normalized[column] = source[column];
        continue;
      }

      const camelKey = remapKey(column);
      if (Object.prototype.hasOwnProperty.call(source, camelKey)) {
        normalized[column] = source[camelKey];
        continue;
      }

      normalized[column] = undefined;
    }
    return normalized;
  });
  return {
    columns,
    rows: normalizedRows
  };
};
