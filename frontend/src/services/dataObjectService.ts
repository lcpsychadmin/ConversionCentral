import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import { DataObject } from '../types/data';
import { mapSystem, SystemResponse } from './systemService';

interface DataObjectResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  processAreaId: string | null;
  workspaceId?: string | null;
  workspace?: {
    id: string;
    name: string;
  } | null;
  createdAt?: string;
  updatedAt?: string;
  systems?: SystemResponse[];
}

const mapDataObject = (payload: DataObjectResponse): DataObject => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  processAreaId: payload.processAreaId,
  workspaceId: payload.workspaceId ?? payload.workspace?.id ?? null,
  workspaceName: payload.workspace?.name ?? null,
  systems: (payload.systems ?? []).map(mapSystem),
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export interface DataObjectFilters {
  workspaceId?: string | null;
  processAreaId?: string | null;
}

export const fetchDataObjects = async (filters: DataObjectFilters = {}): Promise<DataObject[]> => {
  const params: Record<string, string> = {};
  if (filters.workspaceId) {
    params.workspace_id = filters.workspaceId;
  }
  if (filters.processAreaId) {
    params.process_area_id = filters.processAreaId;
  }
  const response = await client.get<DataObjectResponse[] | PaginatedResponse<DataObjectResponse>>('/data-objects', {
    params: Object.keys(params).length ? params : undefined
  });
  const objects = ensureArrayResponse(response.data);
  return objects.map(mapDataObject);
};

export interface DataObjectInput {
  name: string;
  description?: string | null;
  status?: string;
  processAreaId?: string | null;
  workspaceId?: string | null;
  systemIds?: string[];
}

export const createDataObject = async (input: DataObjectInput): Promise<DataObject> => {
  const response = await client.post<DataObjectResponse>('/data-objects', {
    name: input.name,
    description: input.description ?? null,
    status: input.status ?? 'draft',
    process_area_id: input.processAreaId ?? null,
    workspace_id: input.workspaceId ?? null,
    system_ids: input.systemIds ?? []
  });
  return mapDataObject(response.data);
};

export const updateDataObject = async (
  id: string,
  input: Partial<DataObjectInput>
): Promise<DataObject> => {
  const response = await client.put<DataObjectResponse>(`/data-objects/${id}`, {
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.description !== undefined ? { description: input.description } : {}),
    ...(input.status !== undefined ? { status: input.status } : {}),
    ...(input.processAreaId !== undefined ? { process_area_id: input.processAreaId } : {}),
    ...(input.workspaceId !== undefined ? { workspace_id: input.workspaceId } : {}),
    ...(input.systemIds !== undefined ? { system_ids: input.systemIds } : {})
  });
  return mapDataObject(response.data);
};

export const deleteDataObject = async (id: string): Promise<void> => {
  await client.delete(`/data-objects/${id}`);
};
