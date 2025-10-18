import client from './api/client';
export const mapSystemConnection = (payload) => ({
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
