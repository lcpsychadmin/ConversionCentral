import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import { SecurityClassification } from '../types/data';

interface SecurityClassificationResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SecurityClassificationInput {
  name: string;
  description?: string | null;
  status?: string;
  displayOrder?: number | null;
}

export interface SecurityClassificationUpdateInput {
  name?: string;
  description?: string | null;
  status?: string;
  displayOrder?: number | null;
}

const mapSecurityClassification = (
  payload: SecurityClassificationResponse
): SecurityClassification => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  displayOrder: payload.displayOrder ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchSecurityClassifications = async (): Promise<SecurityClassification[]> => {
  const response = await client.get<
    SecurityClassificationResponse[] | PaginatedResponse<SecurityClassificationResponse>
  >('/security-classifications');
  const items = ensureArrayResponse(response.data);
  return items.map(mapSecurityClassification);
};

export const createSecurityClassification = async (
  input: SecurityClassificationInput
): Promise<SecurityClassification> => {
  const response = await client.post<SecurityClassificationResponse>('/security-classifications', {
    name: input.name,
    description: input.description ?? null,
    status: input.status ?? 'active',
    display_order: input.displayOrder ?? null
  });
  return mapSecurityClassification(response.data);
};

export const updateSecurityClassification = async (
  id: string,
  input: SecurityClassificationUpdateInput
): Promise<SecurityClassification> => {
  const payload: Record<string, unknown> = {};
  if (input.name !== undefined) payload.name = input.name;
  if (input.description !== undefined) payload.description = input.description;
  if (input.status !== undefined) payload.status = input.status;
  if (input.displayOrder !== undefined) payload.display_order = input.displayOrder;

  const response = await client.put<SecurityClassificationResponse>(
    `/security-classifications/${id}`,
    payload
  );
  return mapSecurityClassification(response.data);
};

export const deleteSecurityClassification = async (id: string): Promise<void> => {
  await client.delete(`/security-classifications/${id}`);
};
