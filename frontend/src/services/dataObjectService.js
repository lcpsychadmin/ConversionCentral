import client from './api/client';
import { mapSystem } from './systemService';
const mapDataObject = (payload) => ({
    id: payload.id,
    name: payload.name,
    description: payload.description ?? null,
    status: payload.status,
    processAreaId: payload.process_area_id,
    systems: (payload.systems ?? []).map(mapSystem),
    createdAt: payload.created_at,
    updatedAt: payload.updated_at
});
export const fetchDataObjects = async () => {
    const response = await client.get('/data-objects');
    return response.data.map(mapDataObject);
};
export const createDataObject = async (input) => {
    const response = await client.post('/data-objects', {
        name: input.name,
        description: input.description ?? null,
        status: input.status ?? 'draft',
        process_area_id: input.processAreaId ?? null,
        system_ids: input.systemIds ?? []
    });
    return mapDataObject(response.data);
};
export const updateDataObject = async (id, input) => {
    const response = await client.put(`/data-objects/${id}`, {
        ...(input.name !== undefined ? { name: input.name } : {}),
        ...(input.description !== undefined ? { description: input.description } : {}),
        ...(input.status !== undefined ? { status: input.status } : {}),
        ...(input.processAreaId !== undefined ? { process_area_id: input.processAreaId } : {}),
        ...(input.systemIds !== undefined ? { system_ids: input.systemIds } : {})
    });
    return mapDataObject(response.data);
};
export const deleteDataObject = async (id) => {
    await client.delete(`/data-objects/${id}`);
};
