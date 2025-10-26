import client from './api/client';
import { mapSystem } from './systemService';
import { mapField, mapTable } from './tableService';
export const mapDataDefinitionField = (payload) => ({
    id: payload.id,
    definitionTableId: payload.definitionTableId,
    fieldId: payload.fieldId,
    notes: payload.notes ?? null,
    field: mapField(payload.field),
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
const mapNullableDataDefinitionField = (payload) => {
    if (!payload) {
        return null;
    }
    return mapDataDefinitionField(payload);
};
export const mapDataDefinitionRelationship = (payload) => ({
    id: payload.id,
    dataDefinitionId: payload.dataDefinitionId,
    primaryTableId: payload.primaryTableId,
    primaryFieldId: payload.primaryFieldId,
    foreignTableId: payload.foreignTableId,
    foreignFieldId: payload.foreignFieldId,
    joinType: payload.joinType,
    notes: payload.notes ?? null,
    primaryField: mapNullableDataDefinitionField(payload.primaryField),
    foreignField: mapNullableDataDefinitionField(payload.foreignField),
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
const mapDataDefinition = (payload) => ({
    id: payload.id,
    dataObjectId: payload.dataObjectId,
    systemId: payload.systemId,
    description: payload.description ?? null,
    system: payload.system ? mapSystem(payload.system) : null,
    tables: (payload.tables ?? []).map((table) => ({
        id: table.id,
        dataDefinitionId: table.dataDefinitionId,
        tableId: table.tableId,
        alias: table.alias ?? null,
        description: table.description ?? null,
        loadOrder: table.loadOrder ?? null,
        isConstruction: table.isConstruction ?? false,
        table: mapTable(table.table),
        fields: (table.fields ?? []).map(mapDataDefinitionField),
        createdAt: table.createdAt,
        updatedAt: table.updatedAt
    })),
    relationships: (payload.relationships ?? []).map(mapDataDefinitionRelationship),
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
const normalizeTableInput = (table) => ({
    table_id: table.tableId,
    alias: table.alias ?? null,
    description: table.description ?? null,
    load_order: table.loadOrder ?? null,
    is_construction: table.isConstruction ?? false,
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
export const fetchAvailableSourceTables = async (dataObjectId) => {
    const response = await client.get(`/data-definitions/data-objects/${dataObjectId}/available-source-tables`);
    return response.data;
};
export const fetchSourceTableColumns = async (dataObjectId, schemaName, tableName) => {
    const response = await client.get(`/data-definitions/source-table-columns/${dataObjectId}`, {
        params: {
            schema_name: schemaName,
            table_name: tableName
        }
    });
    return response.data;
};
