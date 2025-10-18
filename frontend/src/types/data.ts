export interface DataObject {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  processAreaId: string | null;
  processAreaName?: string | null;
  systems: System[];
  createdAt?: string;
  updatedAt?: string;
}

export interface DataObjectFormValues {
  name: string;
  description?: string | null;
  status: string;
  processAreaId: string;
  systemIds: string[];
}

export interface ProjectSummary {
  projects: number;
  releases: number;
  validationIssues: number;
  pendingApprovals: number;
}

export interface FieldLoad {
  id: string;
  fieldName: string;
  tableName: string;
  releaseName: string;
  loadFlag: boolean;
}

export interface TableLoadOrderItem {
  id: string;
  tableName: string;
  sequence: number;
  notes?: string;
}

export interface ProcessArea {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface ProcessAreaFormValues {
  name: string;
  description?: string | null;
  status: string;
}

export interface ProjectSummaryItem {
  id: string;
  name: string;
  description?: string | null;
  status: string;
}

export interface Project extends ProjectSummaryItem {
  createdAt?: string;
  updatedAt?: string;
}

export interface ProjectFormValues {
  name: string;
  description?: string | null;
  status: string;
}

export interface ProjectInput extends ProjectFormValues {}

export interface Release {
  id: string;
  projectId: string;
  name: string;
  description?: string | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
  projectName?: string | null;
}

export interface ReleaseFormValues {
  projectId: string;
  name: string;
  description?: string | null;
  status: string;
}

export interface ReleaseInput extends ReleaseFormValues {}

export interface System {
  id: string;
  name: string;
  physicalName: string;
  description?: string | null;
  systemType?: string | null;
  status: string;
  securityClassification?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SystemFormValues {
  name: string;
  physicalName: string;
  description?: string | null;
  systemType?: string | null;
  status: string;
  securityClassification?: string | null;
}

export type SystemConnectionType = 'jdbc' | 'odbc' | 'api' | 'file' | 'saprfc' | 'other';

export type SystemConnectionAuthMethod =
  | 'username_password'
  | 'oauth'
  | 'key_vault_reference';

export type RelationalDatabaseType = 'postgresql';

export interface SystemConnection {
  id: string;
  systemId: string;
  connectionType: SystemConnectionType;
  connectionString: string;
  authMethod: SystemConnectionAuthMethod;
  active: boolean;
  notes?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SystemConnectionInput {
  systemId: string;
  connectionType: SystemConnectionType;
  connectionString: string;
  authMethod: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
}

export interface SystemConnectionUpdateInput {
  systemId?: string;
  connectionType?: SystemConnectionType;
  connectionString?: string;
  authMethod?: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
}

export interface SystemConnectionFormValues {
  systemId: string;
  databaseType: RelationalDatabaseType;
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  options?: Record<string, string>;
  notes?: string | null;
  active: boolean;
}

export interface Table {
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

export interface TableInput {
  systemId: string;
  name: string;
  physicalName: string;
  schemaName?: string | null;
  description?: string | null;
  tableType?: string | null;
  status?: string;
}

export interface Field {
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
  securityClassification?: string | null;
  dataValidation?: string | null;
  referenceTable?: string | null;
  groupingTab?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface FieldInput {
  tableId: string;
  name: string;
  description?: string | null;
  applicationUsage?: string | null;
  businessDefinition?: string | null;
  enterpriseAttribute?: string | null;
  fieldType: string;
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

export interface DataDefinitionField {
  id: string;
  definitionTableId: string;
  fieldId: string;
  notes?: string | null;
  field: Field;
  createdAt?: string;
  updatedAt?: string;
}

export type DataDefinitionRelationshipType =
  | 'one_to_one'
  | 'one_to_many'
  | 'many_to_one'
  | 'many_to_many';

export interface DataDefinitionRelationship {
  id: string;
  dataDefinitionId: string;
  primaryTableId: string;
  primaryFieldId: string;
  foreignTableId: string;
  foreignFieldId: string;
  relationshipType: DataDefinitionRelationshipType;
  notes?: string | null;
  primaryField: DataDefinitionField;
  foreignField: DataDefinitionField;
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinitionRelationshipInput {
  primaryFieldId: string;
  foreignFieldId: string;
  relationshipType: DataDefinitionRelationshipType;
  notes?: string | null;
}

export interface DataDefinitionRelationshipUpdateInput {
  primaryFieldId?: string;
  foreignFieldId?: string;
  relationshipType?: DataDefinitionRelationshipType;
  notes?: string | null;
}

export interface DataDefinitionTable {
  id: string;
  dataDefinitionId: string;
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  table: Table;
  fields: DataDefinitionField[];
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinition {
  id: string;
  dataObjectId: string;
  systemId: string;
  description?: string | null;
  system?: System | null;
  tables: DataDefinitionTable[];
  relationships: DataDefinitionRelationship[];
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinitionFieldInput {
  fieldId: string;
  notes?: string | null;
}

export interface DataDefinitionTableInput {
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  fields: DataDefinitionFieldInput[];
}

export interface DataDefinitionInput {
  dataObjectId: string;
  systemId: string;
  description?: string | null;
  tables: DataDefinitionTableInput[];
}

export interface DataDefinitionUpdateInput {
  description?: string | null;
  tables?: DataDefinitionTableInput[];
}
