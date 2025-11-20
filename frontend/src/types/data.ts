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
  usesDatabricksManagedConnection: boolean;
  notes?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface SystemConnectionInput {
  systemId: string;
  connectionType: SystemConnectionType;
  connectionString?: string;
  authMethod: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
  ingestionEnabled?: boolean;
  useDatabricksManagedConnection?: boolean;
}

export interface SystemConnectionUpdateInput {
  systemId?: string;
  connectionType?: SystemConnectionType;
  connectionString?: string;
  authMethod?: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
  ingestionEnabled?: boolean;
  useDatabricksManagedConnection?: boolean;
}

export interface DatabricksSqlSettings {
  id: string;
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog?: string | null;
  schemaName?: string | null;
  constructedSchema?: string | null;
  dataQualitySchema?: string | null;
  dataQualityStorageFormat: 'delta' | 'hudi';
  dataQualityAutoManageTables: boolean;
  profilingPolicyId?: string | null;
  profilePayloadBasePath?: string | null;
  profilingNotebookPath?: string | null;
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
  dataQualitySchema?: string | null;
  dataQualityStorageFormat: 'delta' | 'hudi';
  dataQualityAutoManageTables: boolean;
  profilingPolicyId?: string | null;
  profilePayloadBasePath?: string | null;
  profilingNotebookPath?: string | null;
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
  dataQualitySchema?: string | null;
  dataQualityStorageFormat?: 'delta' | 'hudi';
  dataQualityAutoManageTables?: boolean;
  profilingPolicyId?: string | null;
  profilePayloadBasePath?: string | null;
  profilingNotebookPath?: string | null;
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

export interface DatabricksClusterPolicy {
  id: string;
  settingId: string;
  policyId: string;
  name: string;
  description?: string | null;
  definition?: Record<string, unknown> | null;
  isActive: boolean;
  syncedAt?: string | null;
  createdAt?: string;
  updatedAt?: string;
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
  useDatabricksManagedConnection: boolean;
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

export interface DataQualityProject {
  projectKey: string;
  name: string;
  description?: string | null;
  sqlFlavor?: string | null;
}

export interface DataQualityConnection {
  connectionId: string;
  projectKey: string;
  systemId?: string | null;
  name: string;
  catalog?: string | null;
  schemaName?: string | null;
  httpPath?: string | null;
  managedCredentialsRef?: string | null;
  isActive?: boolean;
}

export interface DataQualityTableGroup {
  tableGroupId: string;
  connectionId: string;
  name: string;
  description?: string | null;
  profilingIncludeMask?: string | null;
  profilingExcludeMask?: string | null;
}

export interface DataQualityTable {
  tableId: string;
  tableGroupId: string;
  schemaName?: string | null;
  tableName: string;
  sourceTableId?: string | null;
}

export interface DataQualityProfileRun {
  profileRunId: string;
  tableGroupId: string;
  status: string;
  startedAt?: string | null;
  completedAt?: string | null;
  rowCount?: number | null;
  anomalyCount?: number | null;
  payloadPath?: string | null;
}

export interface DataQualityProfileRunEntry {
  profileRunId: string;
  tableGroupId: string;
  tableGroupName?: string | null;
  connectionId?: string | null;
  connectionName?: string | null;
  catalog?: string | null;
  schemaName?: string | null;
  dataObjectId?: string | null;
  dataObjectName?: string | null;
  applicationId?: string | null;
  applicationName?: string | null;
  productTeamId?: string | null;
  productTeamName?: string | null;
  status: string;
  startedAt?: string | null;
  completedAt?: string | null;
  durationMs?: number | null;
  rowCount?: number | null;
  anomalyCount?: number | null;
  payloadPath?: string | null;
  anomaliesBySeverity: Record<string, number>;
}

export interface DataQualityProfileRunTableGroup {
  tableGroupId: string;
  tableGroupName?: string | null;
  connectionId?: string | null;
  connectionName?: string | null;
  catalog?: string | null;
  schemaName?: string | null;
  dataObjectId?: string | null;
  dataObjectName?: string | null;
  applicationId?: string | null;
  applicationName?: string | null;
  productTeamId?: string | null;
  productTeamName?: string | null;
}

export interface DataQualityProfileRunListResponse {
  runs: DataQualityProfileRunEntry[];
  tableGroups: DataQualityProfileRunTableGroup[];
}

export interface DataQualityProfileAnomaly {
  tableName?: string | null;
  columnName?: string | null;
  anomalyType: string;
  severity: string;
  description: string;
  detectedAt?: string | null;
}

export interface DataQualityColumnMetric {
  key: string;
  label: string;
  value?: number | string | null;
  formatted?: string | null;
  unit?: string | null;
}

export interface DataQualityColumnValueFrequency {
  value?: unknown;
  count?: number | null;
  percentage?: number | null;
}

export interface DataQualityColumnHistogramBin {
  label: string;
  count?: number | null;
  lower?: number | null;
  upper?: number | null;
}

export interface DataQualityColumnProfile {
  tableGroupId: string;
  profileRunId?: string | null;
  status?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  rowCount?: number | null;
  tableName?: string | null;
  columnName: string;
  dataType?: string | null;
  metrics: DataQualityColumnMetric[];
  topValues: DataQualityColumnValueFrequency[];
  histogram: DataQualityColumnHistogramBin[];
  anomalies: DataQualityProfileAnomaly[];
}

export interface DataQualityTestRun {
  testRunId: string;
  testSuiteKey?: string | null;
  projectKey: string;
  status: string;
  startedAt?: string | null;
  completedAt?: string | null;
  durationMs?: number | null;
  totalTests?: number | null;
  failedTests?: number | null;
  triggerSource?: string | null;
}

export type DataQualityTestSuiteSeverity = 'low' | 'medium' | 'high' | 'critical';

export interface DataQualityTestSuite {
  testSuiteKey: string;
  projectKey?: string | null;
  name: string;
  description?: string | null;
  severity?: DataQualityTestSuiteSeverity | null;
  productTeamId?: string | null;
  applicationId?: string | null;
  dataObjectId?: string | null;
  dataDefinitionId?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface DataQualityTestSuiteInput {
  name: string;
  description?: string | null;
  severity?: DataQualityTestSuiteSeverity | null;
  projectKey?: string | null;
  productTeamId?: string | null;
  applicationId?: string | null;
  dataObjectId?: string | null;
  dataDefinitionId?: string | null;
}

export type DataQualityTestSuiteUpdate = Partial<DataQualityTestSuiteInput>;

export interface DataQualitySuiteTest {
  testId: string;
  testSuiteKey: string;
  tableGroupId: string;
  tableId?: string | null;
  dataDefinitionTableId?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  physicalName?: string | null;
  columnName?: string | null;
  ruleType: string;
  name: string;
  definition: Record<string, unknown>;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface DataQualitySuiteTestInput {
  name: string;
  ruleType: string;
  dataDefinitionTableId: string;
  columnName?: string | null;
  definition?: Record<string, unknown>;
}

export interface DataQualitySuiteTestUpdate {
  name?: string;
  ruleType?: string;
  dataDefinitionTableId?: string;
  columnName?: string | null;
  definition?: Record<string, unknown>;
}

export interface DataQualityTestTypeParameter {
  name: string;
  prompt?: string | null;
  help?: string | null;
  defaultValue?: string | null;
}

export interface DataQualityTestType {
  testType: string;
  ruleType: string;
  id?: string | null;
  nameShort?: string | null;
  nameLong?: string | null;
  description?: string | null;
  usageNotes?: string | null;
  dqDimension?: string | null;
  runType?: string | null;
  testScope?: string | null;
  defaultSeverity?: string | null;
  columnPrompt?: string | null;
  columnHelp?: string | null;
  sqlFlavors: string[];
  parameters: DataQualityTestTypeParameter[];
  sourceFile?: string | null;
}

export interface DataQualityAlert {
  alertId: string;
  sourceType: string;
  sourceRef: string;
  severity: string;
  title: string;
  details: string;
  acknowledged: boolean;
  acknowledgedBy?: string | null;
  acknowledgedAt?: string | null;
  createdAt?: string | null;
}

export interface DataQualityProfileRunStartResponse {
  profileRunId: string;
}

export interface DataQualityTestRunStartResponse {
  testRunId: string;
}

export interface DataQualityDatasetTable {
  dataDefinitionTableId: string;
  tableId: string;
  schemaName: string | null;
  tableName: string;
  physicalName: string;
  alias?: string | null;
  description?: string | null;
  loadOrder?: number | null;
  isConstructed: boolean;
  tableType?: string | null;
}

export interface DataQualityDatasetDefinition {
  dataDefinitionId: string;
  description?: string | null;
  tables: DataQualityDatasetTable[];
}

export interface DataQualityDatasetObject {
  dataObjectId: string;
  name: string;
  description?: string | null;
  dataDefinitions: DataQualityDatasetDefinition[];
}

export interface DataQualityDatasetApplication {
  applicationId: string;
  name: string;
  description?: string | null;
  physicalName: string;
  dataObjects: DataQualityDatasetObject[];
}

export interface DataQualityDatasetProductTeam {
  productTeamId: string;
  name: string;
  description?: string | null;
  applications: DataQualityDatasetApplication[];
}

export interface DataQualityBulkProfileRunResponse {
  requestedTableCount: number;
  targetedTableGroupCount: number;
  profileRuns: { tableGroupId: string; profileRunId: string }[];
  skippedTableIds: string[];
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
