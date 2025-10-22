import client from './api/client';
import { Project, ProjectInput } from '../types/data';

interface ProjectResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
}

const mapProject = (payload: ProjectResponse): Project => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchProjects = async (): Promise<Project[]> => {
  const response = await client.get<ProjectResponse[]>('/projects');
  return response.data.map(mapProject);
};

export const createProject = async (input: ProjectInput): Promise<Project> => {
  const response = await client.post<ProjectResponse>('/projects', {
    name: input.name,
    description: input.description ?? null,
    status: input.status
  });
  return mapProject(response.data);
};

export const updateProject = async (
  id: string,
  input: Partial<ProjectInput>
): Promise<Project> => {
  const payload = Object.fromEntries(
    Object.entries(input).filter(([, value]) => value !== undefined)
  );

  const response = await client.put<ProjectResponse>(`/projects/${id}`, payload);
  return mapProject(response.data);
};

export const deleteProject = async (id: string): Promise<void> => {
  await client.delete(`/projects/${id}`);
};
