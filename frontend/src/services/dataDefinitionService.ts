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
  definitionTableId: string;
  fieldId: string;
  notes?: string | null;
  field: FieldResponse;
  createdAt?: string;
  updatedAt?: string;
}

interface DataDefinitionTableResponse {
  id: string;
  dataDefinitionId: string;
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  isConstruction?: boolean | null;
  table: TableResponse;
  fields: DataDefinitionFieldResponse[];
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinitionRelationshipResponse {
  id: string;
  dataDefinitionId: string;
  primaryTableId: string;
  primaryFieldId: string;
  foreignTableId: string;
  foreignFieldId: string;
  relationshipType: DataDefinitionRelationshipType;
  notes?: string | null;
  primaryField?: DataDefinitionFieldResponse | null;
  foreignField?: DataDefinitionFieldResponse | null;
  createdAt?: string;
  updatedAt?: string;
}

interface DataDefinitionResponse {
  id: string;
  dataObjectId: string;
  systemId: string;
  description?: string | null;
  system?: SystemResponse | null;
  tables: DataDefinitionTableResponse[];
  relationships?: DataDefinitionRelationshipResponse[];
  createdAt?: string;
  updatedAt?: string;
}

export const mapDataDefinitionField = (
  payload: DataDefinitionFieldResponse
): DataDefinitionField => ({
  id: payload.id,
  definitionTableId: payload.definitionTableId,
  fieldId: payload.fieldId,
  notes: payload.notes ?? null,
  field: mapField(payload.field),
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

const mapNullableDataDefinitionField = (
  payload?: DataDefinitionFieldResponse | null
): DataDefinitionField | null => {
  if (!payload) {
    return null;
  }
  return mapDataDefinitionField(payload);
};

export const mapDataDefinitionRelationship = (
  payload: DataDefinitionRelationshipResponse
): DataDefinitionRelationship => ({
  id: payload.id,
  dataDefinitionId: payload.dataDefinitionId,
  primaryTableId: payload.primaryTableId,
  primaryFieldId: payload.primaryFieldId,
  foreignTableId: payload.foreignTableId,
  foreignFieldId: payload.foreignFieldId,
  relationshipType: payload.relationshipType,
  notes: payload.notes ?? null,
  primaryField: mapNullableDataDefinitionField(payload.primaryField),
  foreignField: mapNullableDataDefinitionField(payload.foreignField),
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

const mapDataDefinition = (payload: DataDefinitionResponse): DataDefinition => ({
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

const normalizeTableInput = (table: DataDefinitionTableInput) => ({
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

export interface AvailableSourceTable {
  schemaName: string;
  tableName: string;
  tableType?: string | null;
  columnCount?: number | null;
  estimatedRows?: number | null;
}

export interface SourceTableColumn {
  name: string;
  typeName: string;
  length: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
  nullable: boolean;
}

export const fetchAvailableSourceTables = async (
  dataObjectId: string
): Promise<AvailableSourceTable[]> => {
  const response = await client.get<AvailableSourceTable[]>(
    `/data-definitions/data-objects/${dataObjectId}/available-source-tables`
  );
  return response.data;
};

export const fetchSourceTableColumns = async (
  dataObjectId: string,
  schemaName: string,
  tableName: string
): Promise<SourceTableColumn[]> => {
  const response = await client.get<SourceTableColumn[]>(
    `/data-definitions/source-table-columns/${dataObjectId}`,
    {
      params: {
        schema_name: schemaName,
        table_name: tableName
      }
    }
  );
  return response.data;
};
