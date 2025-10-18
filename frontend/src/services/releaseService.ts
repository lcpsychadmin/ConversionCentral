import client from './api/client';
import { Release, ReleaseInput } from '../types/data';

interface ReleaseResponse {
  id: string;
  project_id: string;
  name: string;
  description?: string | null;
  status: string;
  created_at?: string;
  updated_at?: string;
  project?: {
    id: string;
    name: string;
  } | null;
}

const mapRelease = (payload: ReleaseResponse): Release => ({
  id: payload.id,
  projectId: payload.project_id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at,
  projectName: payload.project?.name ?? null
});

export const fetchReleases = async (): Promise<Release[]> => {
  const response = await client.get<ReleaseResponse[]>('/releases');
  return response.data.map(mapRelease);
};

export const createRelease = async (input: ReleaseInput): Promise<Release> => {
  const response = await client.post<ReleaseResponse>('/releases', {
    project_id: input.projectId,
    name: input.name,
    description: input.description ?? null,
    status: input.status
  });
  return mapRelease(response.data);
};

export const updateRelease = async (
  id: string,
  input: Partial<ReleaseInput>
): Promise<Release> => {
  const payload = Object.fromEntries(
    Object.entries({
      project_id: input.projectId,
      name: input.name,
      description: input.description ?? null,
      status: input.status
    }).filter(([, value]) => value !== undefined)
  );

  const response = await client.put<ReleaseResponse>(`/releases/${id}`, payload);
  return mapRelease(response.data);
};

export const deleteRelease = async (id: string): Promise<void> => {
  await client.delete(`/releases/${id}`);
};
