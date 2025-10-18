import client from './api/client';
import { mapSystem } from './systemService';
import { mapField, mapTable } from './tableService';
export const mapDataDefinitionField = (payload) => ({
    id: payload.id,
    definitionTableId: payload.definition_table_id,
    fieldId: payload.field_id,
    notes: payload.notes ?? null,
    field: mapField(payload.field),
    createdAt: payload.created_at,
    updatedAt: payload.updated_at
});
export const mapDataDefinitionRelationship = (payload) => ({
    id: payload.id,
    dataDefinitionId: payload.data_definition_id,
    primaryTableId: payload.primary_table_id,
    primaryFieldId: payload.primary_field_id,
    foreignTableId: payload.foreign_table_id,
    foreignFieldId: payload.foreign_field_id,
    relationshipType: payload.relationship_type,
    notes: payload.notes ?? null,
    primaryField: mapDataDefinitionField(payload.primary_field),
    foreignField: mapDataDefinitionField(payload.foreign_field),
    createdAt: payload.created_at,
    updatedAt: payload.updated_at
});
const mapDataDefinition = (payload) => ({
    id: payload.id,
    dataObjectId: payload.data_object_id,
    systemId: payload.system_id,
    description: payload.description ?? null,
    system: payload.system ? mapSystem(payload.system) : null,
    tables: (payload.tables ?? []).map((table) => ({
        id: table.id,
        dataDefinitionId: table.data_definition_id,
        tableId: table.table_id,
        alias: table.alias ?? null,
        description: table.description ?? null,
        loadOrder: table.load_order ?? null,
        table: mapTable(table.table),
        fields: (table.fields ?? []).map(mapDataDefinitionField),
        createdAt: table.created_at,
        updatedAt: table.updated_at
    })),
    relationships: (payload.relationships ?? []).map(mapDataDefinitionRelationship),
    createdAt: payload.created_at,
    updatedAt: payload.updated_at
});
const normalizeTableInput = (table) => ({
    table_id: table.tableId,
    alias: table.alias ?? null,
    description: table.description ?? null,
    load_order: table.loadOrder ?? null,
    fields: table.fields.map((field) => ({
        field_id: field.fieldId,
        notes: field.notes ?? null
    }))
});
export const fetchDataDefinition = async (dataObjectId, systemId) => {
    const response = await client.get('/data-definitions', {
        params: {
            data_object_id: dataObjectId,
            system_id: systemId
        }
    });
    if (!response.data.length) {
        return null;
    }
    return mapDataDefinition(response.data[0]);
};
export const createDataDefinition = async (input) => {
    const response = await client.post('/data-definitions', {
        data_object_id: input.dataObjectId,
        system_id: input.systemId,
        description: input.description ?? null,
        tables: input.tables.map(normalizeTableInput)
    });
    return mapDataDefinition(response.data);
};
export const updateDataDefinition = async (id, input) => {
    const response = await client.put(`/data-definitions/${id}`, {
        ...(input.description !== undefined ? { description: input.description ?? null } : {}),
        ...(input.tables !== undefined ? { tables: input.tables.map(normalizeTableInput) } : {})
    });
    return mapDataDefinition(response.data);
};
export const deleteDataDefinition = async (id) => {
    await client.delete(`/data-definitions/${id}`);
};
