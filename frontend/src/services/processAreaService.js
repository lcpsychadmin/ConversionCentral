import client from './api/client';
const mapProcessArea = (payload) => ({
    id: payload.id,
    name: payload.name,
    description: payload.description ?? null,
    status: payload.status,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const fetchProcessAreas = async () => {
    const response = await client.get('/process-areas');
    return response.data.map(mapProcessArea);
};
export const createProcessArea = async (input) => {
    const response = await client.post('/process-areas', {
        name: input.name,
        description: input.description ?? null,
        status: input.status ?? 'draft'
    });
    return mapProcessArea(response.data);
};
export const updateProcessArea = async (id, input) => {
    const response = await client.put(`/process-areas/${id}`, {
        ...(input.name !== undefined ? { name: input.name } : {}),
        ...(input.description !== undefined ? { description: input.description } : {}),
        ...(input.status !== undefined ? { status: input.status } : {})
    });
    return mapProcessArea(response.data);
};
export const deleteProcessArea = async (id) => {
    await client.delete(`/process-areas/${id}`);
};
