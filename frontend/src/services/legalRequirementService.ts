import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import { LegalRequirement } from '../types/data';

interface LegalRequirementResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface LegalRequirementInput {
  name: string;
  description?: string | null;
  status?: string;
  displayOrder?: number | null;
}

export interface LegalRequirementUpdateInput {
  name?: string;
  description?: string | null;
  status?: string;
  displayOrder?: number | null;
}

const mapLegalRequirement = (payload: LegalRequirementResponse): LegalRequirement => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  displayOrder: payload.displayOrder ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchLegalRequirements = async (): Promise<LegalRequirement[]> => {
  const response = await client.get<
    LegalRequirementResponse[] | PaginatedResponse<LegalRequirementResponse>
  >('/legal-requirements');
  const items = ensureArrayResponse(response.data);
  return items.map(mapLegalRequirement);
};

export const createLegalRequirement = async (
  input: LegalRequirementInput
): Promise<LegalRequirement> => {
  const response = await client.post<LegalRequirementResponse>('/legal-requirements', {
    name: input.name,
    description: input.description ?? null,
    status: input.status ?? 'active',
    display_order: input.displayOrder ?? null
  });
  return mapLegalRequirement(response.data);
};

export const updateLegalRequirement = async (
  id: string,
  input: LegalRequirementUpdateInput
): Promise<LegalRequirement> => {
  const payload: Record<string, unknown> = {};
  if (input.name !== undefined) payload.name = input.name;
  if (input.description !== undefined) payload.description = input.description;
  if (input.status !== undefined) payload.status = input.status;
  if (input.displayOrder !== undefined) payload.display_order = input.displayOrder;

  const response = await client.put<LegalRequirementResponse>(`/legal-requirements/${id}`, payload);
  return mapLegalRequirement(response.data);
};

export const deleteLegalRequirement = async (id: string): Promise<void> => {
  await client.delete(`/legal-requirements/${id}`);
};
