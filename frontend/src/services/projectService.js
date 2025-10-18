import client from './api/client';
const mapProject = (payload) => ({
    id: payload.id,
    name: payload.name,
    description: payload.description ?? null,
    status: payload.status,
    createdAt: payload.created_at,
    updatedAt: payload.updated_at
});
export const fetchProjects = async () => {
    const response = await client.get('/projects');
    return response.data.map(mapProject);
};
export const createProject = async (input) => {
    const response = await client.post('/projects', {
        name: input.name,
        description: input.description ?? null,
        status: input.status
    });
    return mapProject(response.data);
};
export const updateProject = async (id, input) => {
    const payload = Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
    const response = await client.put(`/projects/${id}`, payload);
    return mapProject(response.data);
};
export const deleteProject = async (id) => {
    await client.delete(`/projects/${id}`);
};
