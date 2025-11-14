import client from './api/client';
import { ensureArrayResponse, PaginatedResponse } from './api/responseUtils';
import {
  Field,
  FieldInput,
  Table,
  TableInput,
  ConnectionTablePreview,
  LegalRequirement,
  SecurityClassification
} from '../types/data';

export interface TableResponse {
  id: string;
  systemId: string;
  name: string;
  physicalName: string;
  schemaName?: string | null;
  description?: string | null;
  tableType?: string | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface FieldResponse {
  id: string;
  tableId: string;
  name: string;
  description?: string | null;
  applicationUsage?: string | null;
  businessDefinition?: string | null;
  enterpriseAttribute?: string | null;
  fieldType: string;
  fieldLength?: number | null;
  decimalPlaces?: number | null;
  systemRequired: boolean;
  businessProcessRequired: boolean;
  suppressedField: boolean;
  active: boolean;
  legalRegulatoryImplications?: string | null;
  legalRequirementId?: string | null;
  legalRequirement?: LegalRequirementResponse | null;
  securityClassificationId?: string | null;
  securityClassification?: SecurityClassificationResponse | null;
  dataValidation?: string | null;
  referenceTable?: string | null;
  groupingTab?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

interface LegalRequirementResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
  createdAt?: string;
  updatedAt?: string;
}

interface SecurityClassificationResponse {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
  createdAt?: string;
  updatedAt?: string;
}

export const mapTable = (payload: TableResponse): Table => ({
  id: payload.id,
  systemId: payload.systemId,
  name: payload.name,
  physicalName: payload.physicalName,
  schemaName: payload.schemaName ?? null,
  description: payload.description ?? null,
  tableType: payload.tableType ?? null,
  status: payload.status,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});
const mapLegalRequirement = (payload: LegalRequirementResponse): LegalRequirement => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  displayOrder: payload.displayOrder ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

const mapSecurityClassification = (
  payload: SecurityClassificationResponse
): SecurityClassification => ({
  id: payload.id,
  name: payload.name,
  description: payload.description ?? null,
  status: payload.status,
  displayOrder: payload.displayOrder ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const mapField = (payload: FieldResponse): Field => ({
  id: payload.id,
  tableId: payload.tableId,
  name: payload.name,
  description: payload.description ?? null,
  applicationUsage: payload.applicationUsage ?? null,
  businessDefinition: payload.businessDefinition ?? null,
  enterpriseAttribute: payload.enterpriseAttribute ?? null,
  fieldType: payload.fieldType,
  fieldLength: payload.fieldLength ?? null,
  decimalPlaces: payload.decimalPlaces ?? null,
  systemRequired: payload.systemRequired,
  businessProcessRequired: payload.businessProcessRequired,
  suppressedField: payload.suppressedField,
  active: payload.active,
  legalRegulatoryImplications: payload.legalRegulatoryImplications ?? null,
  legalRequirementId: payload.legalRequirementId ?? null,
  legalRequirement: payload.legalRequirement ? mapLegalRequirement(payload.legalRequirement) : null,
  securityClassificationId: payload.securityClassificationId ?? null,
  securityClassification: payload.securityClassification
    ? mapSecurityClassification(payload.securityClassification)
    : null,
  dataValidation: payload.dataValidation ?? null,
  referenceTable: payload.referenceTable ?? null,
  groupingTab: payload.groupingTab ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

export const fetchTables = async (): Promise<Table[]> => {
  const response = await client.get<TableResponse[] | PaginatedResponse<TableResponse>>('/tables');
  const tables = ensureArrayResponse(response.data);
  return tables.map(mapTable);
};

export const fetchFields = async (): Promise<Field[]> => {
  const response = await client.get<FieldResponse[] | PaginatedResponse<FieldResponse>>('/fields');
  const fieldPayloads = ensureArrayResponse(response.data);
  const seen = new Set<string>();
  const results: Field[] = [];
  for (const payload of fieldPayloads) {
    if (seen.has(payload.id)) {
      continue;
    }
    seen.add(payload.id);
    results.push(mapField(payload));
  }
  return results;
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
    legal_requirement_id: input.legalRequirementId ?? null,
    security_classification_id: input.securityClassificationId ?? null,
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
  legalRequirementId?: string | null;
  securityClassificationId?: string | null;
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
  if (input.legalRequirementId !== undefined) {
    payload.legal_requirement_id = input.legalRequirementId;
  }
  if (input.securityClassificationId !== undefined) {
    payload.security_classification_id = input.securityClassificationId;
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
