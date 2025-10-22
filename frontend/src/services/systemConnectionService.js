import client from './api/client';
const mapConnectionCatalogTable = (payload) => ({
    schemaName: payload.schemaName ?? payload.schema_name ?? '',
    tableName: payload.tableName ?? payload.table_name ?? '',
    tableType: payload.tableType ?? payload.table_type ?? null,
    columnCount: payload.columnCount ?? payload.column_count ?? null,
    estimatedRows: payload.estimatedRows ?? payload.estimated_rows ?? null,
    selected: payload.selected,
    available: payload.available,
    selectionId: payload.selectionId ?? payload.selection_id ?? null
});
export const mapSystemConnection = (payload) => ({
    id: payload.id,
    systemId: payload.systemId,
    connectionType: payload.connectionType,
    connectionString: payload.connectionString,
    authMethod: payload.authMethod,
    active: payload.active,
    ingestionEnabled: payload.ingestionEnabled,
    notes: payload.notes ?? null,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const fetchSystemConnections = async () => {
    const response = await client.get('/system-connections');
    return response.data.map(mapSystemConnection);
};
export const createSystemConnection = async (input) => {
    const response = await client.post('/system-connections', {
        system_id: input.systemId,
        connection_type: input.connectionType,
        connection_string: input.connectionString,
        auth_method: input.authMethod,
        active: input.active ?? true,
        ingestion_enabled: input.ingestionEnabled ?? true,
        notes: input.notes ?? null
    });
    return mapSystemConnection(response.data);
};
export const updateSystemConnection = async (id, input) => {
    const response = await client.put(`/system-connections/${id}`, {
        ...(input.systemId !== undefined ? { system_id: input.systemId } : {}),
        ...(input.connectionType !== undefined ? { connection_type: input.connectionType } : {}),
        ...(input.connectionString !== undefined ? { connection_string: input.connectionString } : {}),
        ...(input.authMethod !== undefined ? { auth_method: input.authMethod } : {}),
        ...(input.active !== undefined ? { active: input.active } : {}),
        ...(input.ingestionEnabled !== undefined ? { ingestion_enabled: input.ingestionEnabled } : {}),
        ...(input.notes !== undefined ? { notes: input.notes } : {})
    });
    return mapSystemConnection(response.data);
};
export const deleteSystemConnection = async (id) => {
    await client.delete(`/system-connections/${id}`);
};
export const testSystemConnection = async (payload) => {
    const response = await client.post('/system-connections/test', {
        connection_type: payload.connectionType,
        connection_string: payload.connectionString
    });
    if (!response.data.success) {
        const error = new Error(response.data.message || 'Connection test failed.');
        error.connectionSummary =
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
export const fetchSystemConnectionCatalog = async (id) => {
    const response = await client.get(`/system-connections/${id}/catalog`);
    return response.data.map(mapConnectionCatalogTable);
};
export const updateSystemConnectionCatalogSelection = async (id, selected) => {
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
export const fetchConnectionTablePreview = async (id, schemaName, tableName, limit = 100) => {
    const response = await client.get(`/system-connections/${id}/catalog/preview`, {
        params: {
            schema_name: schemaName ?? undefined,
            table_name: tableName,
            limit
        }
    });
    const { columns, rows } = response.data;
    const remapKey = (key) => key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
    const normalizedRows = (rows ?? []).map((row) => {
        const source = row;
        const normalized = {};
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
