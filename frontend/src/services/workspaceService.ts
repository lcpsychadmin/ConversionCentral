import client from './api/client';
import type { Workspace } from '@cc-types/data';

interface WorkspaceResponse {
  id: string;
  name: string;
  description?: string | null;
  isDefault: boolean;
  isActive: boolean;
  createdAt?: string;
  updatedAt?: string;
}

const mapWorkspace = (payload: WorkspaceResponse): Workspace => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  isDefault: payload.isDefault,
  isActive: payload.isActive,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchWorkspaces = async (): Promise<Workspace[]> => {
  const response = await client.get<WorkspaceResponse[]>('/workspaces');
  return response.data.map(mapWorkspace);
};

export interface WorkspaceInput {
  name: string;
  description?: string | null;
  isDefault?: boolean;
  isActive?: boolean;
}

export interface WorkspaceUpdateInput {
  name?: string;
  description?: string | null;
  isDefault?: boolean;
  isActive?: boolean;
}

export const createWorkspace = async (input: WorkspaceInput): Promise<Workspace> => {
  const response = await client.post<WorkspaceResponse>('/workspaces', {
    name: input.name,
    description: input.description ?? null,
    is_active: input.isActive ?? true,
    is_default: input.isDefault ?? false
  });
  return mapWorkspace(response.data);
};

export const updateWorkspace = async (
  workspaceId: string,
  input: WorkspaceUpdateInput
): Promise<Workspace> => {
  const response = await client.patch<WorkspaceResponse>(`/workspaces/${workspaceId}`, {
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.description !== undefined ? { description: input.description } : {}),
    ...(input.isActive !== undefined ? { is_active: input.isActive } : {}),
    ...(input.isDefault !== undefined ? { is_default: input.isDefault } : {})
  });
  return mapWorkspace(response.data);
};

export const deleteWorkspace = async (workspaceId: string): Promise<void> => {
  await client.delete(`/workspaces/${workspaceId}`);
};
