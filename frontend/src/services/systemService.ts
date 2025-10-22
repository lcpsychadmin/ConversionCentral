import client from './api/client';
import { System } from '../types/data';

export interface SystemResponse {
  id: string;
  name: string;
  physicalName: string;
  description?: string | null;
  systemType?: string | null;
  status: string;
  securityClassification?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export const mapSystem = (payload: SystemResponse): System => ({
  id: payload.id,
  name: payload.name,
  physicalName: payload.physicalName,
  description: payload.description ?? null,
  systemType: payload.systemType ?? null,
  status: payload.status,
  securityClassification: payload.securityClassification ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export interface SystemInput {
  name: string;
  physicalName: string;
  description?: string | null;
  systemType?: string | null;
  status?: string;
  securityClassification?: string | null;
}

export const fetchSystems = async (): Promise<System[]> => {
  const response = await client.get<SystemResponse[]>('/systems');
  return response.data.map(mapSystem);
};

export const createSystem = async (input: SystemInput): Promise<System> => {
  const response = await client.post<SystemResponse>('/systems', {
    name: input.name,
    physical_name: input.physicalName,
    description: input.description ?? null,
    system_type: input.systemType ?? null,
    status: input.status ?? 'active',
    security_classification: input.securityClassification ?? null
  });
  return mapSystem(response.data);
};

export const updateSystem = async (
  id: string,
  input: Partial<SystemInput>
): Promise<System> => {
  const response = await client.put<SystemResponse>(`/systems/${id}`, {
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.physicalName !== undefined ? { physical_name: input.physicalName } : {}),
    ...(input.description !== undefined ? { description: input.description } : {}),
    ...(input.systemType !== undefined ? { system_type: input.systemType } : {}),
    ...(input.status !== undefined ? { status: input.status } : {}),
    ...(input.securityClassification !== undefined
      ? { security_classification: input.securityClassification }
      : {})
  });
  return mapSystem(response.data);
};

export const deleteSystem = async (id: string): Promise<void> => {
  await client.delete(`/systems/${id}`);
};
