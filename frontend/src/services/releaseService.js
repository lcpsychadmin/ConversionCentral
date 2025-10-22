import client from './api/client';
const mapRelease = (payload) => ({
    id: payload.id,
    projectId: payload.projectId,
    name: payload.name,
    description: payload.description ?? null,
    status: payload.status,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt,
    projectName: payload.project?.name ?? null
});
export const fetchReleases = async () => {
    const response = await client.get('/releases');
    return response.data.map(mapRelease);
};
export const createRelease = async (input) => {
    const response = await client.post('/releases', {
        project_id: input.projectId,
        name: input.name,
        description: input.description ?? null,
        status: input.status
    });
    return mapRelease(response.data);
};
export const updateRelease = async (id, input) => {
    const payload = Object.fromEntries(Object.entries({
        project_id: input.projectId,
        name: input.name,
        description: input.description ?? null,
        status: input.status
    }).filter(([, value]) => value !== undefined));
    const response = await client.put(`/releases/${id}`, payload);
    return mapRelease(response.data);
};
export const deleteRelease = async (id) => {
    await client.delete(`/releases/${id}`);
};
