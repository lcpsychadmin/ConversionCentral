import client from './api/client';
import {
  DataDefinition,
  DataDefinitionField,
  DataDefinitionInput,
  DataDefinitionRelationship,
  DataDefinitionRelationshipType,
  DataDefinitionTableInput,
  DataDefinitionUpdateInput
} from '../types/data';
import { mapSystem, SystemResponse } from './systemService';
import { FieldResponse, mapField, mapTable, TableResponse } from './tableService';

export interface DataDefinitionFieldResponse {
  id: string;
  definition_table_id: string;
  field_id: string;
  notes?: string | null;
  field: FieldResponse;
  created_at?: string;
  updated_at?: string;
}

interface DataDefinitionTableResponse {
  id: string;
  data_definition_id: string;
  table_id: string;
  alias?: string | null;
  description?: string | null;
  load_order?: number | null;
  table: TableResponse;
  fields: DataDefinitionFieldResponse[];
  created_at?: string;
  updated_at?: string;
}

export interface DataDefinitionRelationshipResponse {
  id: string;
  data_definition_id: string;
  primary_table_id: string;
  primary_field_id: string;
  foreign_table_id: string;
  foreign_field_id: string;
  relationship_type: DataDefinitionRelationshipType;
  notes?: string | null;
  primary_field: DataDefinitionFieldResponse;
  foreign_field: DataDefinitionFieldResponse;
  created_at?: string;
  updated_at?: string;
}

interface DataDefinitionResponse {
  id: string;
  data_object_id: string;
  system_id: string;
  description?: string | null;
  system?: SystemResponse | null;
  tables: DataDefinitionTableResponse[];
  relationships?: DataDefinitionRelationshipResponse[];
  created_at?: string;
  updated_at?: string;
}

export const mapDataDefinitionField = (
  payload: DataDefinitionFieldResponse
): DataDefinitionField => ({
  id: payload.id,
  definitionTableId: payload.definition_table_id,
  fieldId: payload.field_id,
  notes: payload.notes ?? null,
  field: mapField(payload.field),
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

export const mapDataDefinitionRelationship = (
  payload: DataDefinitionRelationshipResponse
): DataDefinitionRelationship => ({
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

const mapDataDefinition = (payload: DataDefinitionResponse): DataDefinition => ({
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

const normalizeTableInput = (table: DataDefinitionTableInput) => ({
  table_id: table.tableId,
  alias: table.alias ?? null,
  description: table.description ?? null,
  load_order: table.loadOrder ?? null,
  fields: table.fields.map((field) => ({
    field_id: field.fieldId,
    notes: field.notes ?? null
  }))
});

export const fetchDataDefinition = async (
  dataObjectId: string,
  systemId: string
): Promise<DataDefinition | null> => {
  const response = await client.get<DataDefinitionResponse[]>('/data-definitions', {
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

export const createDataDefinition = async (
  input: DataDefinitionInput
): Promise<DataDefinition> => {
  const response = await client.post<DataDefinitionResponse>('/data-definitions', {
    data_object_id: input.dataObjectId,
    system_id: input.systemId,
    description: input.description ?? null,
    tables: input.tables.map(normalizeTableInput)
  });

  return mapDataDefinition(response.data);
};

export const updateDataDefinition = async (
  id: string,
  input: DataDefinitionUpdateInput
): Promise<DataDefinition> => {
  const response = await client.put<DataDefinitionResponse>(`/data-definitions/${id}`, {
    ...(input.description !== undefined ? { description: input.description ?? null } : {}),
    ...(input.tables !== undefined ? { tables: input.tables.map(normalizeTableInput) } : {})
  });

  return mapDataDefinition(response.data);
};

export const deleteDataDefinition = async (id: string): Promise<void> => {
  await client.delete(`/data-definitions/${id}`);
};
