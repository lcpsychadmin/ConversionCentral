import client from './api/client';
import { Field, FieldInput, Table, TableInput, ConnectionTablePreview } from '../types/data';

export interface TableResponse {
  id: string;
  system_id: string;
  name: string;
  physical_name: string;
  schema_name?: string | null;
  description?: string | null;
  table_type?: string | null;
  status: string;
  created_at?: string;
  updated_at?: string;
}

export interface FieldResponse {
  id: string;
  table_id: string;
  name: string;
  description?: string | null;
  application_usage?: string | null;
  business_definition?: string | null;
  enterprise_attribute?: string | null;
  field_type: string;
  field_length?: number | null;
  decimal_places?: number | null;
  system_required: boolean;
  business_process_required: boolean;
  suppressed_field: boolean;
  active: boolean;
  legal_regulatory_implications?: string | null;
  security_classification?: string | null;
  data_validation?: string | null;
  reference_table?: string | null;
  grouping_tab?: string | null;
  created_at?: string;
  updated_at?: string;
}

export const mapTable = (payload: TableResponse): Table => ({
  id: payload.id,
  systemId: payload.system_id,
  name: payload.name,
  physicalName: payload.physical_name,
  schemaName: payload.schema_name ?? null,
  description: payload.description ?? null,
  tableType: payload.table_type ?? null,
  status: payload.status,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

export const mapField = (payload: FieldResponse): Field => ({
  id: payload.id,
  tableId: payload.table_id,
  name: payload.name,
  description: payload.description ?? null,
  applicationUsage: payload.application_usage ?? null,
  businessDefinition: payload.business_definition ?? null,
  enterpriseAttribute: payload.enterprise_attribute ?? null,
  fieldType: payload.field_type,
  fieldLength: payload.field_length ?? null,
  decimalPlaces: payload.decimal_places ?? null,
  systemRequired: payload.system_required,
  businessProcessRequired: payload.business_process_required,
  suppressedField: payload.suppressed_field,
  active: payload.active,
  legalRegulatoryImplications: payload.legal_regulatory_implications ?? null,
  securityClassification: payload.security_classification ?? null,
  dataValidation: payload.data_validation ?? null,
  referenceTable: payload.reference_table ?? null,
  groupingTab: payload.grouping_tab ?? null,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

export const fetchTables = async (): Promise<Table[]> => {
  const response = await client.get<TableResponse[]>('/tables');
  return response.data.map(mapTable);
};

export const fetchFields = async (): Promise<Field[]> => {
  const response = await client.get<FieldResponse[]>('/fields');
  return response.data.map(mapField);
};

export const createTable = async (input: TableInput): Promise<Table> => {
  const response = await client.post<TableResponse>('/tables', {
    system_id: input.systemId,
    name: input.name,
    physical_name: input.physicalName,
    schema_name: input.schemaName ?? null,
    description: input.description ?? null,
    table_type: input.tableType ?? null,
    status: input.status ?? 'active'
  });
  return mapTable(response.data);
};

export interface TableUpdateInput {
  systemId?: string;
  name?: string;
  physicalName?: string;
  schemaName?: string | null;
  description?: string | null;
  tableType?: string | null;
  status?: string;
}

export const updateTable = async (id: string, input: TableUpdateInput): Promise<Table> => {
  const payload: Record<string, unknown> = {};
  if (input.systemId !== undefined) payload.system_id = input.systemId;
  if (input.name !== undefined) payload.name = input.name;
  if (input.physicalName !== undefined) payload.physical_name = input.physicalName;
  if (input.schemaName !== undefined) payload.schema_name = input.schemaName;
  if (input.description !== undefined) payload.description = input.description;
  if (input.tableType !== undefined) payload.table_type = input.tableType;
  if (input.status !== undefined) payload.status = input.status;

  const response = await client.put<TableResponse>(`/tables/${id}`, payload);
  return mapTable(response.data);
};

export const createField = async (input: FieldInput): Promise<Field> => {
  const response = await client.post<FieldResponse>('/fields', {
    table_id: input.tableId,
    name: input.name,
    description: input.description ?? null,
    application_usage: input.applicationUsage ?? null,
    business_definition: input.businessDefinition ?? null,
    enterprise_attribute: input.enterpriseAttribute ?? null,
    field_type: input.fieldType,
    field_length: input.fieldLength ?? null,
    decimal_places: input.decimalPlaces ?? null,
    system_required: input.systemRequired ?? false,
    business_process_required: input.businessProcessRequired ?? false,
    suppressed_field: input.suppressedField ?? false,
    active: input.active ?? true,
    legal_regulatory_implications: input.legalRegulatoryImplications ?? null,
    security_classification: input.securityClassification ?? null,
    data_validation: input.dataValidation ?? null,
    reference_table: input.referenceTable ?? null,
    grouping_tab: input.groupingTab ?? null
  });
  return mapField(response.data);
};

export interface FieldUpdateInput {
  tableId?: string;
  name?: string;
  description?: string | null;
  applicationUsage?: string | null;
  businessDefinition?: string | null;
  enterpriseAttribute?: string | null;
  fieldType?: string;
  fieldLength?: number | null;
  decimalPlaces?: number | null;
  systemRequired?: boolean;
  businessProcessRequired?: boolean;
  suppressedField?: boolean;
  active?: boolean;
  legalRegulatoryImplications?: string | null;
  securityClassification?: string | null;
  dataValidation?: string | null;
  referenceTable?: string | null;
  groupingTab?: string | null;
}

export const updateField = async (id: string, input: FieldUpdateInput): Promise<Field> => {
  const payload: Record<string, unknown> = {};
  if (input.tableId !== undefined) payload.table_id = input.tableId;
  if (input.name !== undefined) payload.name = input.name;
  if (input.description !== undefined) payload.description = input.description;
  if (input.applicationUsage !== undefined) payload.application_usage = input.applicationUsage;
  if (input.businessDefinition !== undefined) payload.business_definition = input.businessDefinition;
  if (input.enterpriseAttribute !== undefined) payload.enterprise_attribute = input.enterpriseAttribute;
  if (input.fieldType !== undefined) payload.field_type = input.fieldType;
  if (input.fieldLength !== undefined) payload.field_length = input.fieldLength;
  if (input.decimalPlaces !== undefined) payload.decimal_places = input.decimalPlaces;
  if (input.systemRequired !== undefined) payload.system_required = input.systemRequired;
  if (input.businessProcessRequired !== undefined) {
    payload.business_process_required = input.businessProcessRequired;
  }
  if (input.suppressedField !== undefined) payload.suppressed_field = input.suppressedField;
  if (input.active !== undefined) payload.active = input.active;
  if (input.legalRegulatoryImplications !== undefined) {
    payload.legal_regulatory_implications = input.legalRegulatoryImplications;
  }
  if (input.securityClassification !== undefined) {
    payload.security_classification = input.securityClassification;
  }
  if (input.dataValidation !== undefined) payload.data_validation = input.dataValidation;
  if (input.referenceTable !== undefined) payload.reference_table = input.referenceTable;
  if (input.groupingTab !== undefined) payload.grouping_tab = input.groupingTab;

  const response = await client.put<FieldResponse>(`/fields/${id}`, payload);
  return mapField(response.data);
};

export const fetchTablePreview = async (
  tableId: string,
  limit = 100
): Promise<ConnectionTablePreview> => {
  const response = await client.get<ConnectionTablePreview>(
    `/tables/${tableId}/preview`,
    {
      params: {
        limit
      }
    }
  );
  return response.data;
};
