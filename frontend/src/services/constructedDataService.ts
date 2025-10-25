import client from './api/client';

export type ConstructedRowPayload = Record<string, unknown>;

export interface ConstructedTable {
  id: string;
  executionContextId: string;
  name: string;
  description?: string | null;
  purpose?: string | null;
  status: 'draft' | 'pending_approval' | 'approved' | 'rejected';
  createdAt?: string;
  updatedAt?: string;
}

export interface ConstructedField {
  id: string;
  constructedTableId: string;
  name: string;
  dataType: string;
  isNullable: boolean;
  defaultValue?: string | null;
  description?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface ConstructedData {
  id: string;
  constructedTableId: string;
  rowIdentifier?: string | null;
  payload: ConstructedRowPayload;
  createdAt?: string;
  updatedAt?: string;
}

export interface ValidationError {
  rowIndex: number;
  fieldName?: string | null;
  message: string;
  ruleId: string;
}

export interface BatchSaveRequest {
  rows: ConstructedRowPayload[];
  validateOnly?: boolean;
}

export interface BatchSaveResponse {
  success: boolean;
  rowsSaved: number;
  errors: ValidationError[];
}

export interface ConstructedDataValidationRule {
  id: string;
  constructedTableId: string;
  name: string;
  description?: string | null;
  ruleType: 'required' | 'unique' | 'range' | 'pattern' | 'custom' | 'cross_field';
  fieldId?: string | null;
  configuration: Record<string, unknown>;
  errorMessage: string;
  isActive: boolean;
  appliesTo_NewOnly: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinitionTable {
  id: string;
  dataDefinitionId: string;
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  isConstruction: boolean;
  constructedTableId?: string | null;
  constructedTableName?: string | null;
  constructedTableStatus?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinition {
  id: string;
  dataObjectId: string;
  systemId: string;
  description?: string | null;
  tables: DataDefinitionTable[];
  dataObject?: DataObject | null;
  system?: System | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface ProcessArea {
  id: string;
  name: string;
  status: string;
}

export interface DataObject {
  id: string;
  processAreaId: string;
  name: string;
  status: string;
  processArea?: ProcessArea | null;
  systems?: System[];
}

export interface System {
  id: string;
  name: string;
  status: string;
}

/**
 * Fetch all constructed tables for an execution context
 */
export async function fetchConstructedTables(
  executionContextId: string
): Promise<ConstructedTable[]> {
  const response = await client.get<ConstructedTable[]>(
    '/constructed-tables',
    {
      params: { executionContextId }
    }
  );
  return response.data;
}

/**
 * Fetch all fields for a constructed table
 */
export async function fetchConstructedFields(
  constructedTableId: string
): Promise<ConstructedField[]> {
  const response = await client.get<ConstructedField[]>(
    '/constructed-fields',
    {
      params: { constructedTableId }
    }
  );
  return response.data;
}

/**
 * Fetch all data rows for a constructed table
 */
export async function fetchConstructedData(
  constructedTableId: string
): Promise<ConstructedData[]> {
  const response = await client.get<ConstructedData[]>(
    `/constructed-data/${constructedTableId}/by-table`
  );
  return response.data;
}

/**
 * Save constructed data with batch validation
 */
export async function batchSaveConstructedData(
  constructedTableId: string,
  request: BatchSaveRequest
): Promise<BatchSaveResponse> {
  const response = await client.post<BatchSaveResponse>(
    `/constructed-data/${constructedTableId}/batch-save`,
    request
  );
  return response.data;
}

/**
 * Create a single constructed data row
 */
export interface CreateConstructedDataPayload {
  constructedTableId: string;
  payload: ConstructedRowPayload;
  rowIdentifier?: string | null;
}

export async function createConstructedData(
  data: CreateConstructedDataPayload
): Promise<ConstructedData> {
  const response = await client.post<ConstructedData>(
    '/constructed-data',
    {
      constructed_table_id: data.constructedTableId,
      payload: data.payload,
      row_identifier: data.rowIdentifier ?? null,
    }
  );
  return response.data;
}

/**
 * Update a single constructed data row
 */
export async function updateConstructedData(
  id: string,
  data: Partial<ConstructedData>
): Promise<ConstructedData> {
  const response = await client.put<ConstructedData>(
    `/constructed-data/${id}`,
    {
      ...(data.constructedTableId ? { constructed_table_id: data.constructedTableId } : {}),
      ...(data.rowIdentifier !== undefined ? { row_identifier: data.rowIdentifier } : {}),
      ...(data.payload !== undefined ? { payload: data.payload } : {}),
    }
  );
  return response.data;
}

/**
 * Delete a constructed data row
 */
export async function deleteConstructedData(id: string): Promise<void> {
  await client.delete(`/constructed-data/${id}`);
}

/**
 * Fetch all validation rules for a constructed table
 */
export async function fetchValidationRules(
  constructedTableId: string
): Promise<ConstructedDataValidationRule[]> {
  const response = await client.get<ConstructedDataValidationRule[]>(
    '/constructed-data-validation-rules',
    {
      params: { constructed_table_id: constructedTableId }
    }
  );
  return response.data;
}

/**
 * Create a validation rule
 */
export async function createValidationRule(
  data: Omit<ConstructedDataValidationRule, 'id' | 'createdAt' | 'updatedAt'>
): Promise<ConstructedDataValidationRule> {
  const response = await client.post<ConstructedDataValidationRule>(
    '/constructed-data-validation-rules',
    data
  );
  return response.data;
}

/**
 * Update a validation rule
 */
export async function updateValidationRule(
  id: string,
  data: Partial<ConstructedDataValidationRule>
): Promise<ConstructedDataValidationRule> {
  const response = await client.put<ConstructedDataValidationRule>(
    `/constructed-data-validation-rules/${id}`,
    data
  );
  return response.data;
}

/**
 * Delete a validation rule
 */
export async function deleteValidationRule(id: string): Promise<void> {
  await client.delete(`/constructed-data-validation-rules/${id}`);
}

/**
 * Fetch all process areas
 */
export async function fetchProcessAreas(): Promise<ProcessArea[]> {
  const response = await client.get<ProcessArea[]>('/process-areas');
  return response.data;
}

/**
 * Fetch all data objects for a process area
 */
export async function fetchDataObjects(processAreaId: string): Promise<DataObject[]> {
  const response = await client.get<DataObject[]>(
    '/data-objects',
    {
      params: { processAreaId }
    }
  );
  return response.data;
}

/**
 * Fetch all systems
 */
export async function fetchSystems(): Promise<System[]> {
  const response = await client.get<System[]>('/systems');
  return response.data;
}

/**
 * Fetch ALL data definitions with construction tables (no filter)
 * Used to display all available tables upfront
 */
export async function fetchAllConstructionDefinitions(): Promise<DataDefinition[]> {
  try {
    const response = await client.get<DataDefinition[]>('/data-definitions');
    
    // Filter for tables marked as construction
    return response.data
      .map(def => ({
        ...def,
        tables: def.tables?.filter(table => table.isConstruction && !!table.constructedTableId) || []
      }))
      .filter(def => def.tables.length > 0);
  } catch (error) {
    console.error('Error fetching all construction definitions:', error);
    return [];
  }
}

/**
 * Fetch data definitions marked as construction tables
 * Filtered by dataObjectId and systemId
 */
export async function fetchConstructionDefinitions(
  dataObjectId: string,
  systemId: string
): Promise<DataDefinition[]> {
  const response = await client.get<DataDefinition[]>(
    '/data-definitions',
    {
      params: { data_object_id: dataObjectId, system_id: systemId }
    }
  );
  
  // Filter for tables marked as construction
  return response.data.map(def => ({
    ...def,
    tables: def.tables?.filter(table => table.isConstruction && !!table.constructedTableId) || []
  })).filter(def => def.tables.length > 0);
}
