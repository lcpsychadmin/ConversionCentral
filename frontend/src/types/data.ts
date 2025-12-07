export type RelationalDatabaseType = 'postgresql' | 'databricks' | 'sap';

export type SystemConnectionType = 'jdbc';

export type SystemConnectionAuthMethod = 'username_password';

export interface System {
  id: string;
  name: string;
  physicalName?: string | null;
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

export interface SystemConnection {
  id: string;
  name: string;
  systemId?: string | null;
  connectionType: SystemConnectionType;
  connectionString: string;
  authMethod: SystemConnectionAuthMethod;
  active: boolean;
  notes?: string | null;
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

export interface ReleaseFormValues {
  projectId: string;
  name: string;
  description?: string | null;
  status: string;
}

export interface ReleaseInput extends ReleaseFormValues {}

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

export interface SystemConnectionInput {
  name?: string;
  systemId?: string | null;
  connectionType: SystemConnectionType;
  connectionString?: string;
  authMethod: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
}

export interface SystemConnectionUpdateInput {
  name?: string;
  systemId?: string | null;
  connectionType?: SystemConnectionType;
  connectionString?: string;
  authMethod?: SystemConnectionAuthMethod;
  notes?: string | null;
  active?: boolean;
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
  name: string;
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

export interface TableObservabilitySchedule {
  scheduleId: string;
  categoryKey: string;
  categoryName: string;
  cronExpression: string;
  timezone: string;
  isActive: boolean;
  defaultCronExpression: string;
  defaultTimezone: string;
  cadence: string;
  rationale: string;
  lastRunStatus?: string | null;
  lastRunStartedAt?: string | null;
  lastRunCompletedAt?: string | null;
  lastRunError?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface TableObservabilityScheduleUpdateInput {
  cronExpression?: string;
  timezone?: string;
  isActive?: boolean;
}

export interface TableObservabilityRun {
  runId: string;
  scheduleId?: string | null;
  categoryKey: string;
  categoryName: string;
  status: string;
  startedAt?: string | null;
  completedAt?: string | null;
  tableCount: number;
  metricsCollected: number;
  error?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface TableObservabilityMetric {
  metricId: string;
  runId: string;
  selectionId?: string | null;
  systemConnectionId?: string | null;
  schemaName?: string | null;
  tableName: string;
  metricCategory: string;
  metricName: string;
  metricUnit?: string | null;
  metricValueNumber?: number | null;
  metricValueText?: string | null;
  metricPayload?: Record<string, unknown> | null;
  metricStatus?: string | null;
  recordedAt: string;
}

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
  name: string;
  catalog?: string | null;
  schemaName?: string | null;
  httpPath?: string | null;
  managedCredentialsRef?: string | null;
  isActive?: boolean;
}

export interface DataQualityTableGroup {
  tableGroupId: string;
  tableCount?: number | null;
  fieldCount?: number | null;
  connectionId: string;
  name: string;
  description?: string | null;
  profilingIncludeMask?: string | null;
  profilingExcludeMask?: string | null;
}

export interface DataQualityTable {
  profilingScore?: number | null;
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
  tableCount?: number | null;
  fieldCount?: number | null;
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
  applicationDescription?: string | null;
  productTeamId?: string | null;
  productTeamName?: string | null;
  tableCount?: number | null;
  fieldCount?: number | null;
  status: string;
  startedAt?: string | null;
  completedAt?: string | null;
  durationMs?: number | null;
  rowCount?: number | null;
  anomalyCount?: number | null;
  payloadPath?: string | null;
  profilingScore?: number | null;
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
  applicationDescription?: string | null;
  productTeamId?: string | null;
  productTeamName?: string | null;
  tableCount?: number | null;
  fieldCount?: number | null;
}

export interface DataQualityProfileRunListResponse {
  runs: DataQualityProfileRunEntry[];
  tableGroups: DataQualityProfileRunTableGroup[];
}

export interface DataQualityProfileRunDeleteRequest {
  profileRunIds: string[];
}

export interface DataQualityProfileRunDeleteResponse {
  deletedCount: number;
}

export interface DataQualityProfileValueEntry {
  value?: unknown;
  count?: number | null;
  percentage?: number | null;
  label?: string | null;
  lower?: number | null;
  upper?: number | null;
}

export interface DataQualityProfileColumnEntry {
  columnId?: string | null;
  columnName: string;
  schemaName?: string | null;
  tableName?: string | null;
  dataType?: string | null;
  generalType?: string | null;
  metrics: Record<string, unknown>;
  textProfile?: DataQualityTextColumnProfile | null;
  numericProfile?: DataQualityNumericColumnProfile | null;
  rowCount?: number | null;
  nullCount?: number | null;
  distinctCount?: number | null;
  nonNullCount?: number | null;
  minValue?: unknown;
  maxValue?: unknown;
  avgValue?: number | null;
  stddevValue?: number | null;
  medianValue?: number | null;
  p95Value?: number | null;
  topValues: DataQualityProfileValueEntry[];
  histogram: DataQualityProfileValueEntry[];
  anomalies: DataQualityProfileAnomalyEntry[];
}

export interface DataQualityProfileTableEntry {
  tableId?: string | null;
  tableGroupId?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  metrics: Record<string, unknown>;
  columns: DataQualityProfileColumnEntry[];
  anomalies: DataQualityProfileAnomalyEntry[];
}

export interface DataQualityProfileRunResultSummary {
  profileRunId: string;
  tableGroupId: string;
  status?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  rowCount?: number | null;
  anomalyCount?: number | null;
  databricksRunId?: string | null;
}

export interface DataQualityProfileRunResultResponse {
  tableGroupId: string;
  profileRunId: string;
  summary: DataQualityProfileRunResultSummary;
  tables: DataQualityProfileTableEntry[];
}

export interface DataQualityProfilingSchedule {
  profilingScheduleId: string;
  tableGroupId: string;
  tableGroupName?: string | null;
  connectionId?: string | null;
  connectionName?: string | null;
  applicationId?: string | null;
  applicationName?: string | null;
  dataObjectId?: string | null;
  dataObjectName?: string | null;
  scheduleExpression: string;
  timezone?: string | null;
  isActive: boolean;
  lastProfileRunId?: string | null;
  lastRunStatus?: string | null;
  lastRunStartedAt?: string | null;
  lastRunCompletedAt?: string | null;
  lastRunError?: string | null;
  totalRuns: number;
  createdAt: string;
  updatedAt: string;
}

export interface DataQualityProfilingScheduleInput {
  tableGroupId: string;
  scheduleExpression: string;
  timezone?: string | null;
  isActive?: boolean;
}

export interface DataQualityProfileAnomaly {
  tableName?: string | null;
  columnName?: string | null;
  anomalyType: string;
  severity: string;
  description: string;
  detectedAt?: string | null;
}

export type DataQualityColumnProfileType = 'numeric' | 'text' | 'other';

export interface DataQualityColumnCharacteristics {
  semanticType?: string | null;
  suggestedType?: string | null;
  firstDetectedAt?: string | null;
  semanticConfidence?: number | null;
}

export interface DataQualityColumnTags {
  stakeholderGroup?: string | null;
  dataSource?: string | null;
  sourceSystem?: string | null;
  sourceProcess?: string | null;
  businessDomain?: string | null;
  transformLevel?: string | null;
  dataProduct?: string | null;
  criticalDataElement?: boolean | null;
  piiRisk?: string | null;
}

export interface DataQualityColumnIssueSummary {
  total?: number | null;
  severityCounts?: Record<string, number> | null;
  items?: DataQualityProfileAnomaly[];
}

export interface DataQualityColumnRelatedSuite {
  testSuiteKey: string;
  name: string;
  severity?: DataQualityTestSuiteSeverity | null;
}

export interface DataQualityNumericDistributionBar {
  key: 'nonZero' | 'zero' | 'null';
  label: string;
  count: number;
  percentage?: number | null;
}

export interface DataQualityProfileAnomalyEntry {
  anomalyTypeId?: string | null;
  severity?: string | null;
  likelihood?: string | null;
  detail?: string | null;
  piiRisk?: string | null;
  dqDimension?: string | null;
  columnName?: string | null;
  detectedAt?: string | null;
}

export interface DataQualityNumericBoxPlot {
  min?: number | null;
  p25?: number | null;
  median?: number | null;
  p75?: number | null;
  max?: number | null;
  mean?: number | null;
  stdDev?: number | null;
}

export interface DataQualityNumericProfileStats {
  recordCount?: number | null;
  valueCount?: number | null;
  distinctCount?: number | null;
  average?: number | null;
  stddev?: number | null;
  minimum?: number | null;
  minimumPositive?: number | null;
  maximum?: number | null;
  percentile25?: number | null;
  median?: number | null;
  percentile75?: number | null;
  zeroCount?: number | null;
  nullCount?: number | null;
}

export interface DataQualityNumericColumnProfile {
  stats?: DataQualityNumericProfileStats | null;
  distributionBars?: DataQualityNumericDistributionBar[];
  boxPlot?: DataQualityNumericBoxPlot | null;
  histogram?: DataQualityColumnHistogramBin[];
  topValues?: DataQualityColumnValueFrequency[];
}

export interface DataQualityTextPatternStat {
  label: string;
  count?: number | null;
  percentage?: number | null;
}

export interface DataQualityTextCaseBucket {
  label: string;
  count?: number | null;
  percentage?: number | null;
}

export interface DataQualityTextProfileStats {
  recordCount?: number | null;
  valueCount?: number | null;
  actualValueCount?: number | null;
  nullValueCount?: number | null;
  zeroLengthCount?: number | null;
  dummyValueCount?: number | null;
  missingCount?: number | null;
  missingPercentage?: number | null;
  duplicateCount?: number | null;
  duplicatePercentage?: number | null;
  zeroCount?: number | null;
  numericOnlyCount?: number | null;
  includesDigitCount?: number | null;
  quotedCount?: number | null;
  leadingSpaceCount?: number | null;
  embeddedSpaceCount?: number | null;
  dateValueCount?: number | null;
  averageEmbeddedSpaces?: number | null;
  minLength?: number | null;
  maxLength?: number | null;
  avgLength?: number | null;
  minText?: string | null;
  maxText?: string | null;
  distinctPatterns?: number | null;
  standardPatternMatches?: number | null;
}

export interface DataQualityTextColumnProfile {
  stats?: DataQualityTextProfileStats | null;
  missingBreakdown?: DataQualityTextPatternStat[];
  duplicateBreakdown?: DataQualityTextPatternStat[];
  caseBreakdown?: DataQualityTextCaseBucket[];
  frequentPatterns?: DataQualityTextPatternStat[];
  topValues?: DataQualityColumnValueFrequency[];
  lengthHistogram?: DataQualityColumnHistogramBin[];
}

export interface DataQualityColumnProfileLayout {
  columnType: DataQualityColumnProfileType;
  characteristics?: DataQualityColumnCharacteristics | null;
  tags?: DataQualityColumnTags | null;
  piiSignals?: DataQualityColumnIssueSummary | null;
  hygieneIssues?: DataQualityColumnIssueSummary | null;
  testIssues?: DataQualityColumnIssueSummary | null;
  relatedTestSuites?: DataQualityColumnRelatedSuite[];
  badges?: string[];
  numericProfile?: DataQualityNumericColumnProfile | null;
  textProfile?: DataQualityTextColumnProfile | null;
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
  profileLayout?: DataQualityColumnProfileLayout | null;
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

export interface DataQualityDatasetField {
  dataDefinitionFieldId: string;
  fieldId: string;
  name: string;
  description?: string | null;
  fieldType?: string | null;
  fieldLength?: number | null;
  decimalPlaces?: number | null;
  applicationUsage?: string | null;
  businessDefinition?: string | null;
  notes?: string | null;
  displayOrder?: number | null;
  isUnique?: boolean | null;
  referenceTable?: string | null;
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
  fields: DataQualityDatasetField[];
}

export interface DataQualityDatasetTableContext {
  dataDefinitionTableId: string;
  dataDefinitionId: string;
  dataObjectId: string;
  applicationId: string;
  productTeamId?: string | null;
  tableGroupId: string;
  tableId?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  physicalName?: string | null;
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
