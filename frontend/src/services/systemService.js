import client from './api/client';
export const mapSystem = (payload) => ({
    id: payload.id,
    name: payload.name,
    physicalName: payload.physicalName,
    description: payload.description ?? null,
    systemType: payload.systemType ?? null,
    status: payload.status,
    securityClassification: payload.securityClassification ?? null,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const fetchSystems = async () => {
    const response = await client.get('/systems');
    return response.data.map(mapSystem);
};
export const createSystem = async (input) => {
    const response = await client.post('/systems', {
        name: input.name,
        physical_name: input.physicalName,
        description: input.description ?? null,
        system_type: input.systemType ?? null,
        status: input.status ?? 'active',
        security_classification: input.securityClassification ?? null
    });
    return mapSystem(response.data);
};
export const updateSystem = async (id, input) => {
    const response = await client.put(`/systems/${id}`, {
        ...(input.name !== undefined ? { name: input.name } : {}),
        ...(input.physicalName !== undefined ? { physical_name: input.physicalName } : {}),
        ...(input.description !== undefined ? { description: input.description } : {}),
        ...(input.systemType !== undefined ? { system_type: input.systemType } : {}),
        ...(input.status !== undefined ? { status: input.status } : {}),
        ...(input.securityClassification !== undefined
            ? { security_classification: input.securityClassification }
            : {})
    });
    return mapSystem(response.data);
};
export const deleteSystem = async (id) => {
    await client.delete(`/systems/${id}`);
};
