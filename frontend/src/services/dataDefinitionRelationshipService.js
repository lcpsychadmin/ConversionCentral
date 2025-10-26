import client from './api/client';
import { mapDataDefinitionRelationship } from './dataDefinitionService';
export const fetchDataDefinitionRelationships = async (definitionId) => {
    const response = await client.get(`/data-definitions/${definitionId}/relationships`);
    return response.data.map(mapDataDefinitionRelationship);
};
export const createDataDefinitionRelationship = async (definitionId, input) => {
    const response = await client.post(`/data-definitions/${definitionId}/relationships`, {
        primary_field_id: input.primaryFieldId,
        foreign_field_id: input.foreignFieldId,
        join_type: input.joinType,
        notes: input.notes ?? null
    });
    return mapDataDefinitionRelationship(response.data);
};
export const updateDataDefinitionRelationship = async (definitionId, relationshipId, input) => {
    const response = await client.put(`/data-definitions/${definitionId}/relationships/${relationshipId}`, {
        ...(input.primaryFieldId !== undefined ? { primary_field_id: input.primaryFieldId } : {}),
        ...(input.foreignFieldId !== undefined ? { foreign_field_id: input.foreignFieldId } : {}),
        ...(input.joinType !== undefined ? { join_type: input.joinType } : {}),
        ...(input.notes !== undefined ? { notes: input.notes ?? null } : {})
    });
    return mapDataDefinitionRelationship(response.data);
};
export const deleteDataDefinitionRelationship = async (definitionId, relationshipId) => {
    await client.delete(`/data-definitions/${definitionId}/relationships/${relationshipId}`);
};
