import client from './api/client';
import {
  DataDefinitionRelationship,
  DataDefinitionRelationshipInput,
  DataDefinitionRelationshipUpdateInput
} from '../types/data';
import {
  DataDefinitionRelationshipResponse,
  mapDataDefinitionRelationship
} from './dataDefinitionService';

export const fetchDataDefinitionRelationships = async (
  definitionId: string
): Promise<DataDefinitionRelationship[]> => {
  const response = await client.get<DataDefinitionRelationshipResponse[]>(
    `/data-definitions/${definitionId}/relationships`
  );
  return response.data.map(mapDataDefinitionRelationship);
};

export const createDataDefinitionRelationship = async (
  definitionId: string,
  input: DataDefinitionRelationshipInput
): Promise<DataDefinitionRelationship> => {
  const response = await client.post<DataDefinitionRelationshipResponse>(
    `/data-definitions/${definitionId}/relationships`,
    {
      primary_field_id: input.primaryFieldId,
      foreign_field_id: input.foreignFieldId,
      relationship_type: input.relationshipType,
      notes: input.notes ?? null
    }
  );
  return mapDataDefinitionRelationship(response.data);
};

export const updateDataDefinitionRelationship = async (
  definitionId: string,
  relationshipId: string,
  input: DataDefinitionRelationshipUpdateInput
): Promise<DataDefinitionRelationship> => {
  const response = await client.put<DataDefinitionRelationshipResponse>(
    `/data-definitions/${definitionId}/relationships/${relationshipId}`,
    {
      ...(input.primaryFieldId !== undefined ? { primary_field_id: input.primaryFieldId } : {}),
      ...(input.foreignFieldId !== undefined ? { foreign_field_id: input.foreignFieldId } : {}),
      ...(input.relationshipType !== undefined ? { relationship_type: input.relationshipType } : {}),
      ...(input.notes !== undefined ? { notes: input.notes ?? null } : {})
    }
  );
  return mapDataDefinitionRelationship(response.data);
};

export const deleteDataDefinitionRelationship = async (
  definitionId: string,
  relationshipId: string
): Promise<void> => {
  await client.delete(`/data-definitions/${definitionId}/relationships/${relationshipId}`);
};
