import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import {
  DataDefinition,
  DataDefinitionField,
  DataDefinitionInput,
  DataDefinitionRelationship,
  DataDefinitionJoinType,
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
  displayOrder?: number | null;
  isUnique?: boolean | null;
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
  systemConnectionId?: string | null;
  connectionTableSelectionId?: string | null;
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
  joinType: DataDefinitionJoinType;
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
  displayOrder: payload.displayOrder ?? 0,
  isUnique: payload.isUnique ?? false,
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
  joinType: payload.joinType,
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
    systemConnectionId: table.systemConnectionId ?? null,
    connectionTableSelectionId: table.connectionTableSelectionId ?? null,
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
  system_connection_id: table.systemConnectionId ?? null,
  connection_table_selection_id: table.connectionTableSelectionId ?? null,
  fields: table.fields.map((field, index) => ({
    field_id: field.fieldId,
    notes: field.notes ?? null,
    display_order: field.displayOrder ?? index,
    is_unique: field.isUnique ?? false
  }))
});

export const fetchDataDefinition = async (
  dataObjectId: string,
  systemId: string
): Promise<DataDefinition | null> => {
  const response = await client.get<DataDefinitionResponse[] | PaginatedResponse<DataDefinitionResponse>>(
    '/data-definitions',
    {
      params: {
        data_object_id: dataObjectId,
        system_id: systemId
      }
    }
  );

  const definitions = ensureArrayResponse(response.data);

  if (!definitions.length) {
    return null;
  }

  return mapDataDefinition(definitions[0]);
};

export interface DataDefinitionFilters {
  workspaceId?: string | null;
}

export const fetchAllDataDefinitions = async (
  filters: DataDefinitionFilters = {}
): Promise<DataDefinition[]> => {
  const params: Record<string, string> = {};
  if (filters.workspaceId) {
    params.workspace_id = filters.workspaceId;
  }
  const response = await client.get<DataDefinitionResponse[] | PaginatedResponse<DataDefinitionResponse>>(
    '/data-definitions',
    {
      params: Object.keys(params).length ? params : undefined
    }
  );
  const definitions = ensureArrayResponse(response.data);
  return definitions.map(mapDataDefinition);
};

export const fetchDataDefinitionsByContext = async (
  dataObjectId: string,
  systemId: string
): Promise<DataDefinition[]> => {
  const response = await client.get<DataDefinitionResponse[] | PaginatedResponse<DataDefinitionResponse>>(
    '/data-definitions',
    {
      params: {
        data_object_id: dataObjectId,
        system_id: systemId
      }
    }
  );
  const definitions = ensureArrayResponse(response.data);
  return definitions.map(mapDataDefinition);
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
  catalogName?: string | null;
  schemaName: string;
  tableName: string;
  tableType?: string | null;
  columnCount?: number | null;
  estimatedRows?: number | null;
  selectionId?: string | null;
  systemConnectionId?: string | null;
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
  return response.data.map((item) => ({
    catalogName: item.catalogName ?? null,
    schemaName: item.schemaName,
    tableName: item.tableName,
    tableType: item.tableType ?? null,
    columnCount: item.columnCount ?? null,
    estimatedRows: item.estimatedRows ?? null,
    selectionId: item.selectionId ?? null,
    systemConnectionId: item.systemConnectionId ?? null
  }));
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
