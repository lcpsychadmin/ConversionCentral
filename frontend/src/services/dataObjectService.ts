import client from './api/client';
import { DataObject } from '../types/data';
import { mapSystem, SystemResponse } from './systemService';

interface DataObjectResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  processAreaId: string | null;
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
  systems: (payload.systems ?? []).map(mapSystem),
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchDataObjects = async (): Promise<DataObject[]> => {
  const response = await client.get<DataObjectResponse[]>('/data-objects');
  return response.data.map(mapDataObject);
};

export interface DataObjectInput {
  name: string;
  description?: string | null;
  status?: string;
  processAreaId?: string | null;
  systemIds?: string[];
}

export const createDataObject = async (input: DataObjectInput): Promise<DataObject> => {
  const response = await client.post<DataObjectResponse>('/data-objects', {
    name: input.name,
    description: input.description ?? null,
    status: input.status ?? 'draft',
    process_area_id: input.processAreaId ?? null,
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
    ...(input.systemIds !== undefined ? { system_ids: input.systemIds } : {})
  });
  return mapDataObject(response.data);
};

export const deleteDataObject = async (id: string): Promise<void> => {
  await client.delete(`/data-objects/${id}`);
};
