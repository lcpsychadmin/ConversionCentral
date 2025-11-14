import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import { ProcessArea } from '../types/data';

interface ProcessAreaResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
}

const mapProcessArea = (payload: ProcessAreaResponse): ProcessArea => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export interface ProcessAreaInput {
  name: string;
  description?: string | null;
  status?: string;
}

export const fetchProcessAreas = async (): Promise<ProcessArea[]> => {
  const response = await client.get<ProcessAreaResponse[] | PaginatedResponse<ProcessAreaResponse>>('/process-areas');
  const processAreas = ensureArrayResponse(response.data);
  return processAreas.map(mapProcessArea);
};

export const createProcessArea = async (input: ProcessAreaInput): Promise<ProcessArea> => {
  const response = await client.post<ProcessAreaResponse>('/process-areas', {
    name: input.name,
    description: input.description ?? null,
    status: input.status ?? 'draft'
  });
  return mapProcessArea(response.data);
};

export const updateProcessArea = async (
  id: string,
  input: Partial<ProcessAreaInput>
): Promise<ProcessArea> => {
  const response = await client.put<ProcessAreaResponse>(`/process-areas/${id}`, {
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.description !== undefined ? { description: input.description } : {}),
    ...(input.status !== undefined ? { status: input.status } : {})
  });
  return mapProcessArea(response.data);
};

export const deleteProcessArea = async (id: string): Promise<void> => {
  await client.delete(`/process-areas/${id}`);
};
