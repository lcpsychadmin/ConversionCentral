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

export interface LegalRequirement {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SecurityClassification {
  id: string;
  name: string;
  description?: string | null;
  status: string;
  displayOrder?: number | null;
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

export type RelationalDatabaseType = 'postgresql' | 'databricks' | 'sap';

export interface SystemConnection {
  id: string;
  systemId: string;
  connectionType: SystemConnectionType;
  connectionString: string;
  authMethod: SystemConnectionAuthMethod;
  active: boolean;
  ingestionEnabled: boolean;
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
  ingestionEnabled?: boolean;
}

export interface SystemConnectionUpdateInput {
  systemId?: string;
  connectionType?: SystemConnectionType;
  connectionString?: string;
  authMethod?: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
  ingestionEnabled?: boolean;
}

export interface DatabricksSqlSettings {
  id: string;
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  ingestionBatchRows?: number | null;
  ingestionMethod: 'sql' | 'spark';
  sparkCompute?: 'classic' | 'serverless' | null;
  warehouseName?: string | null;
  isActive: boolean;
  hasAccessToken: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface DatabricksSqlSettingsInput {
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  accessToken: string;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  ingestionBatchRows?: number | null;
  ingestionMethod: 'sql' | 'spark';
  sparkCompute?: 'classic' | 'serverless' | null;
  warehouseName?: string | null;
}

export interface DatabricksSqlSettingsUpdate {
  displayName?: string;
  workspaceHost?: string;
  httpPath?: string;
  accessToken?: string | null;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  ingestionBatchRows?: number | null;
  ingestionMethod?: 'sql' | 'spark';
  sparkCompute?: 'classic' | 'serverless' | null;
  warehouseName?: string | null;
  isActive?: boolean;
}

export interface DatabricksSqlSettingsTestResult {
  success: boolean;
  message: string;
  durationMs?: number | null;
}

export interface DatabricksDataType {
  name: string;
  category: string;
  supportsDecimalPlaces: boolean;
}

export interface SapHanaSettings {
  id: string;
  displayName: string;
  host: string;
  port: number;
  databaseName: string;
  username: string;
  schemaName?: string | null;
  tenant?: string | null;
  useSsl: boolean;
  ingestionBatchRows?: number | null;
  isActive: boolean;
  hasPassword: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface SapHanaSettingsInput {
  displayName?: string | null;
  host: string;
  port: number;
  databaseName: string;
  username: string;
  password: string;
  schemaName?: string | null;
  tenant?: string | null;
  useSsl: boolean;
  ingestionBatchRows?: number | null;
}

export interface SapHanaSettingsUpdate {
  displayName?: string;
  host?: string;
  port?: number;
  databaseName?: string;
  username?: string;
  password?: string | null;
  schemaName?: string | null;
  tenant?: string | null;
  useSsl?: boolean;
  ingestionBatchRows?: number | null;
  isActive?: boolean;
}

export interface SapHanaSettingsTestResult {
  success: boolean;
  message: string;
  durationMs?: number | null;
}

export type ApplicationDatabaseEngine = 'default_postgres' | 'custom_postgres' | 'sqlserver';

export interface ApplicationDatabaseConnectionInput {
  host?: string;
  port?: number | null;
  database?: string;
  username?: string;
  password?: string;
  options?: Record<string, string> | null;
  useSsl?: boolean;
}

export interface ApplicationDatabaseApplyInput {
  engine: ApplicationDatabaseEngine;
  displayName?: string | null;
  connection?: ApplicationDatabaseConnectionInput | null;
}

export interface ApplicationDatabaseTestResult {
  success: boolean;
  message: string;
  latencyMs?: number | null;
}

export interface ApplicationDatabaseSetting {
  id: string;
  engine: ApplicationDatabaseEngine;
  connectionDisplay?: string | null;
  appliedAt: string;
  displayName?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface ApplicationDatabaseStatus {
  configured: boolean;
  setting: ApplicationDatabaseSetting | null;
  adminEmail: string | null;
}

export interface CompanySettings {
  siteTitle: string | null;
  logoDataUrl: string | null;
  themeMode: 'light' | 'dark';
  accentColor: string;
}

export interface CompanySettingsUpdateInput extends CompanySettings {}

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
  ingestionEnabled: boolean;
}

export interface ConnectionCatalogTable {
  schemaName: string;
  tableName: string;
  tableType?: string | null;
  columnCount?: number | null;
  estimatedRows?: number | null;
  selected: boolean;
  available: boolean;
  selectionId?: string | null;
}

export interface ConnectionTablePreview {
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface ConnectionCatalogSelectionInput {
  schemaName: string;
  tableName: string;
  tableType?: string | null;
  columnCount?: number | null;
  estimatedRows?: number | null;
}

export type IngestionLoadStrategy = 'timestamp' | 'numeric_key' | 'full';

export type DataWarehouseTarget = 'databricks_sql';

export type UploadTableMode = 'create' | 'replace';

export interface UploadDataColumn {
  originalName: string;
  fieldName: string;
  inferredType: string;
}

export interface UploadDataColumnOverrideInput {
  fieldName: string;
  targetName?: string;
  targetType?: string;
  exclude?: boolean;
}

export interface UploadDataPreview {
  columns: UploadDataColumn[];
  sampleRows: (string | null)[][];
  totalRows: number;
}

export interface UploadDataCreateResponse {
  tableName: string;
  schemaName?: string | null;
  catalog?: string | null;
  rowsInserted: number;
  targetWarehouse: DataWarehouseTarget;
  tableId?: string | null;
  constructedTableId?: string | null;
  dataDefinitionId?: string | null;
  dataDefinitionTableId?: string | null;
}

export interface IngestionSchedule {
  id: string;
  connectionTableSelectionId: string;
  scheduleExpression: string;
  timezone?: string | null;
  loadStrategy: IngestionLoadStrategy;
  watermarkColumn?: string | null;
  primaryKeyColumn?: string | null;
  targetSchema?: string | null;
  targetTableName?: string | null;
  targetWarehouse: DataWarehouseTarget;
  sapHanaSettingId?: string | null;
  batchSize: number;
  isActive: boolean;
  lastWatermarkTimestamp?: string | null;
  lastWatermarkId?: number | null;
  lastRunStartedAt?: string | null;
  lastRunCompletedAt?: string | null;
  lastRunStatus?: string | null;
  lastRunError?: string | null;
  totalRuns: number;
  totalRowsLoaded: number;
  createdAt?: string;
  updatedAt?: string;
}

export interface IngestionScheduleInput {
  connectionTableSelectionId: string;
  scheduleExpression: string;
  timezone?: string | null;
  loadStrategy: IngestionLoadStrategy;
  watermarkColumn?: string | null;
  primaryKeyColumn?: string | null;
  targetSchema?: string | null;
  targetTableName?: string | null;
  targetWarehouse: DataWarehouseTarget;
  sapHanaSettingId?: string | null;
  batchSize: number;
  isActive: boolean;
}

export interface IngestionScheduleUpdateInput {
  scheduleExpression?: string;
  timezone?: string | null;
  loadStrategy?: IngestionLoadStrategy;
  watermarkColumn?: string | null;
  primaryKeyColumn?: string | null;
  targetSchema?: string | null;
  targetTableName?: string | null;
  targetWarehouse?: DataWarehouseTarget;
  sapHanaSettingId?: string | null;
  batchSize?: number;
  isActive?: boolean;
}

export interface IngestionRun {
  id: string;
  ingestionScheduleId: string;
  status: 'scheduled' | 'running' | 'completed' | 'failed';
  startedAt?: string | null;
  completedAt?: string | null;
  rowsLoaded?: number | null;
  rowsExpected?: number | null;
  watermarkTimestampBefore?: string | null;
  watermarkTimestampAfter?: string | null;
  watermarkIdBefore?: number | null;
  watermarkIdAfter?: number | null;
  queryText?: string | null;
  errorMessage?: string | null;
  createdAt?: string;
  updatedAt?: string;
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
  legalRequirementId?: string | null;
  legalRequirement?: LegalRequirement | null;
  securityClassificationId?: string | null;
  securityClassification?: SecurityClassification | null;
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
  legalRequirementId?: string | null;
  securityClassificationId?: string | null;
  dataValidation?: string | null;
  referenceTable?: string | null;
  groupingTab?: string | null;
}

export interface DataDefinitionField {
  id: string;
  definitionTableId: string;
  fieldId: string;
  notes?: string | null;
  displayOrder: number;
  isUnique: boolean;
  field: Field;
  createdAt?: string;
  updatedAt?: string;
}

export type DataDefinitionJoinType = 'inner' | 'left' | 'right';

export interface DataDefinitionRelationship {
  id: string;
  dataDefinitionId: string;
  primaryTableId: string;
  primaryFieldId: string;
  foreignTableId: string;
  foreignFieldId: string;
  joinType: DataDefinitionJoinType;
  notes?: string | null;
  primaryField: DataDefinitionField | null;
  foreignField: DataDefinitionField | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface DataDefinitionRelationshipInput {
  primaryFieldId: string;
  foreignFieldId: string;
  joinType: DataDefinitionJoinType;
  notes?: string | null;
}

export interface DataDefinitionRelationshipUpdateInput {
  primaryFieldId?: string;
  foreignFieldId?: string;
  joinType?: DataDefinitionJoinType;
  notes?: string | null;
}

export interface DataDefinitionTable {
  id: string;
  dataDefinitionId: string;
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  isConstruction: boolean;
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
  displayOrder?: number | null;
  isUnique?: boolean;
}

export interface DataDefinitionTableInput {
  tableId: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  isConstruction?: boolean;
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
