import axios from 'axios';
import client from './api/client';
import {
  DataQualityAlert,
  DataQualityBulkProfileRunResponse,
  DataQualityColumnHistogramBin,
  DataQualityColumnProfile,
  DataQualityColumnValueFrequency,
  DataQualityColumnMetric,
  DataQualityConnection,
  DataQualityDatasetProductTeam,
  DataQualityProfileAnomaly,
  DataQualityProfileRun,
  DataQualityProfileRunEntry,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunStartResponse,
  DataQualityProfileRunTableGroup,
  DataQualityProject,
  DataQualityTable,
  DataQualityTableGroup,
  DataQualityTestRun,
  DataQualityTestRunStartResponse,
  DataQualityTestSuite,
  DataQualityTestSuiteInput,
  DataQualitySuiteTest,
  DataQualitySuiteTestInput,
  DataQualitySuiteTestUpdate,
  DataQualityTestSuiteSeverity,
  DataQualityTestSuiteUpdate,
  DataQualityTestType,
  DataQualityTestTypeParameter
} from '@cc-types/data';

interface TestGenProjectResponse {
  project_key: string;
  name: string;
  description?: string | null;
  sql_flavor?: string | null;
}

interface TestGenConnectionResponse {
  connection_id: string;
  project_key: string;
  system_id?: string | null;
  name: string;
  catalog?: string | null;
  schema_name?: string | null;
  http_path?: string | null;
  managed_credentials_ref?: string | null;
  is_active?: boolean;
}

interface TestGenTableGroupResponse {
  table_group_id: string;
  connection_id: string;
  name: string;
  description?: string | null;
  profiling_include_mask?: string | null;
  profiling_exclude_mask?: string | null;
}

interface TestGenTableResponse {
  table_id: string;
  table_group_id: string;
  schema_name?: string | null;
  table_name: string;
  source_table_id?: string | null;
}

interface TestGenColumnMetricResponse {
  key: string;
  label: string;
  value?: number | string | null;
  formatted?: string | null;
  unit?: string | null;
}

interface TestGenColumnValueFrequencyResponse {
  value?: unknown;
  count?: number | null;
  percentage?: number | null;
}

interface TestGenColumnHistogramResponse {
  label: string;
  count?: number | null;
  lower?: number | null;
  upper?: number | null;
}

interface TestGenProfileAnomalyResponse {
  table_name?: string | null;
  column_name?: string | null;
  anomaly_type: string;
  severity: string;
  description: string;
  detected_at?: string | null;
}

interface TestGenColumnProfileResponse {
  table_group_id: string;
  profile_run_id?: string | null;
  status?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  row_count?: number | null;
  table_name?: string | null;
  column_name: string;
  data_type?: string | null;
  metrics?: TestGenColumnMetricResponse[] | null;
  top_values?: TestGenColumnValueFrequencyResponse[] | null;
  histogram?: TestGenColumnHistogramResponse[] | null;
  anomalies?: TestGenProfileAnomalyResponse[] | null;
}

interface TestGenTestRunResponse {
  test_run_id: string;
  test_suite_key?: string | null;
  project_key: string;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  duration_ms?: number | null;
  total_tests?: number | null;
  failed_tests?: number | null;
  trigger_source?: string | null;
}

interface TestGenAlertResponse {
  alert_id: string;
  source_type: string;
  source_ref: string;
  severity: string;
  title: string;
  details: string;
  acknowledged: boolean;
  acknowledged_by?: string | null;
  acknowledged_at?: string | null;
  created_at?: string | null;
}

interface TestGenProfileRunStartResponse {
  profile_run_id: string;
}

interface TestGenTestRunStartResponse {
  test_run_id: string;
}

interface TestGenTestSuiteResponse {
  testSuiteKey: string;
  projectKey?: string | null;
  name: string;
  description?: string | null;
  severity?: string | null;
  productTeamId?: string | null;
  applicationId?: string | null;
  dataObjectId?: string | null;
  dataDefinitionId?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

const TEST_SUITE_SEVERITIES: readonly DataQualityTestSuiteSeverity[] = [
  'low',
  'medium',
  'high',
  'critical'
] as const;

const mapSeverity = (value?: string | null): DataQualityTestSuiteSeverity | null => {
  if (!value) {
    return null;
  }
  return TEST_SUITE_SEVERITIES.includes(value as DataQualityTestSuiteSeverity)
    ? (value as DataQualityTestSuiteSeverity)
    : null;
};

const mapProject = (payload: TestGenProjectResponse): DataQualityProject => ({
  projectKey: payload.project_key,
  name: payload.name,
  description: payload.description ?? null,
  sqlFlavor: payload.sql_flavor ?? null
});

const mapConnection = (payload: TestGenConnectionResponse): DataQualityConnection => ({
  connectionId: payload.connection_id,
  projectKey: payload.project_key,
  systemId: payload.system_id ?? null,
  name: payload.name,
  catalog: payload.catalog ?? null,
  schemaName: payload.schema_name ?? null,
  httpPath: payload.http_path ?? null,
  managedCredentialsRef: payload.managed_credentials_ref ?? null,
  isActive: payload.is_active
});

const mapTableGroup = (payload: TestGenTableGroupResponse): DataQualityTableGroup => ({
  tableGroupId: payload.table_group_id,
  connectionId: payload.connection_id,
  name: payload.name,
  description: payload.description ?? null,
  profilingIncludeMask: payload.profiling_include_mask ?? null,
  profilingExcludeMask: payload.profiling_exclude_mask ?? null
});

const mapTable = (payload: TestGenTableResponse): DataQualityTable => ({
  tableId: payload.table_id,
  tableGroupId: payload.table_group_id,
  schemaName: payload.schema_name ?? null,
  tableName: payload.table_name,
  sourceTableId: payload.source_table_id ?? null
});

const mapProfileRunEntry = (payload: DataQualityProfileRunEntry): DataQualityProfileRunEntry => ({
  ...payload,
  anomaliesBySeverity: payload.anomaliesBySeverity ?? {}
});

const mapProfileRunTableGroup = (
  payload: DataQualityProfileRunTableGroup
): DataQualityProfileRunTableGroup => ({
  ...payload
});

const mapProfileRunEntryToLegacy = (
  payload: DataQualityProfileRunEntry
): DataQualityProfileRun => ({
  profileRunId: payload.profileRunId,
  tableGroupId: payload.tableGroupId,
  status: payload.status,
  startedAt: payload.startedAt ?? null,
  completedAt: payload.completedAt ?? null,
  rowCount: payload.rowCount ?? null,
  anomalyCount: payload.anomalyCount ?? null,
  payloadPath: payload.payloadPath ?? null
});

const mapProfileAnomaly = (
  payload: TestGenProfileAnomalyResponse
): DataQualityProfileAnomaly => ({
  tableName: payload.table_name ?? null,
  columnName: payload.column_name ?? null,
  anomalyType: payload.anomaly_type,
  severity: payload.severity,
  description: payload.description,
  detectedAt: payload.detected_at ?? null
});

const mapColumnMetric = (
  payload: TestGenColumnMetricResponse
): DataQualityColumnMetric => ({
  key: payload.key,
  label: payload.label,
  value: payload.value ?? null,
  formatted: payload.formatted ?? null,
  unit: payload.unit ?? null
});

const mapColumnValueFrequency = (
  payload: TestGenColumnValueFrequencyResponse
): DataQualityColumnValueFrequency => ({
  value: payload.value ?? null,
  count: payload.count ?? null,
  percentage: payload.percentage ?? null
});

const mapColumnHistogramBin = (
  payload: TestGenColumnHistogramResponse
): DataQualityColumnHistogramBin => ({
  label: payload.label,
  count: payload.count ?? null,
  lower: payload.lower ?? null,
  upper: payload.upper ?? null
});

const mapColumnProfile = (payload: TestGenColumnProfileResponse): DataQualityColumnProfile => ({
  tableGroupId: payload.table_group_id,
  profileRunId: payload.profile_run_id ?? null,
  status: payload.status ?? null,
  startedAt: payload.started_at ?? null,
  completedAt: payload.completed_at ?? null,
  rowCount: payload.row_count ?? null,
  tableName: payload.table_name ?? null,
  columnName: payload.column_name,
  dataType: payload.data_type ?? null,
  metrics: (payload.metrics ?? []).map(mapColumnMetric),
  topValues: (payload.top_values ?? []).map(mapColumnValueFrequency),
  histogram: (payload.histogram ?? []).map(mapColumnHistogramBin),
  anomalies: (payload.anomalies ?? []).map(mapProfileAnomaly)
});

const mapTestRun = (payload: TestGenTestRunResponse): DataQualityTestRun => ({
  testRunId: payload.test_run_id,
  testSuiteKey: payload.test_suite_key ?? null,
  projectKey: payload.project_key,
  status: payload.status,
  startedAt: payload.started_at ?? null,
  completedAt: payload.completed_at ?? null,
  durationMs: payload.duration_ms ?? null,
  totalTests: payload.total_tests ?? null,
  failedTests: payload.failed_tests ?? null,
  triggerSource: payload.trigger_source ?? null
});

const mapAlert = (payload: TestGenAlertResponse): DataQualityAlert => ({
  alertId: payload.alert_id,
  sourceType: payload.source_type,
  sourceRef: payload.source_ref,
  severity: payload.severity,
  title: payload.title,
  details: payload.details,
  acknowledged: payload.acknowledged,
  acknowledgedBy: payload.acknowledged_by ?? null,
  acknowledgedAt: payload.acknowledged_at ?? null,
  createdAt: payload.created_at ?? null
});

const mapTestSuite = (payload: TestGenTestSuiteResponse): DataQualityTestSuite => ({
  testSuiteKey: payload.testSuiteKey,
  projectKey: payload.projectKey ?? null,
  name: payload.name,
  description: payload.description ?? null,
  severity: mapSeverity(payload.severity),
  productTeamId: payload.productTeamId ?? null,
  applicationId: payload.applicationId ?? null,
  dataObjectId: payload.dataObjectId ?? null,
  dataDefinitionId: payload.dataDefinitionId ?? null,
  createdAt: payload.createdAt ?? null,
  updatedAt: payload.updatedAt ?? null
});

interface TestGenSuiteTestResponse {
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
  definition?: Record<string, unknown>;
  createdAt?: string | null;
  updatedAt?: string | null;
}

interface TestGenTestTypeParameterResponse {
  name: string;
  prompt?: string | null;
  help?: string | null;
  defaultValue?: string | null;
}

interface TestGenTestTypeResponse {
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
  sqlFlavors?: string[] | null;
  parameters?: TestGenTestTypeParameterResponse[] | null;
  sourceFile?: string | null;
}

const mapSuiteTest = (payload: TestGenSuiteTestResponse): DataQualitySuiteTest => ({
  testId: payload.testId,
  testSuiteKey: payload.testSuiteKey,
  tableGroupId: payload.tableGroupId,
  tableId: payload.tableId ?? null,
  dataDefinitionTableId: payload.dataDefinitionTableId ?? null,
  schemaName: payload.schemaName ?? null,
  tableName: payload.tableName ?? null,
  physicalName: payload.physicalName ?? null,
  columnName: payload.columnName ?? null,
  ruleType: payload.ruleType,
  name: payload.name,
  definition: payload.definition ?? {},
  createdAt: payload.createdAt ?? null,
  updatedAt: payload.updatedAt ?? null
});

const mapTestTypeParameter = (
  payload: TestGenTestTypeParameterResponse
): DataQualityTestTypeParameter => ({
  name: payload.name,
  prompt: payload.prompt ?? null,
  help: payload.help ?? null,
  defaultValue: payload.defaultValue ?? null
});

const mapTestType = (payload: TestGenTestTypeResponse): DataQualityTestType => ({
  testType: payload.testType,
  ruleType: payload.ruleType,
  id: payload.id ?? null,
  nameShort: payload.nameShort ?? null,
  nameLong: payload.nameLong ?? null,
  description: payload.description ?? null,
  usageNotes: payload.usageNotes ?? null,
  dqDimension: payload.dqDimension ?? null,
  runType: payload.runType ?? null,
  testScope: payload.testScope ?? null,
  defaultSeverity: payload.defaultSeverity ?? null,
  columnPrompt: payload.columnPrompt ?? null,
  columnHelp: payload.columnHelp ?? null,
  sqlFlavors: [...(payload.sqlFlavors ?? [])],
  parameters: (payload.parameters ?? []).map(mapTestTypeParameter),
  sourceFile: payload.sourceFile ?? null
});

const sanitizeTestSuiteKey = (value: string): string => {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error('Test suite identifier is required.');
  }

  const lowered = trimmed.toLowerCase();
  if (lowered === 'undefined' || lowered === 'null') {
    throw new Error('Test suite identifier is invalid.');
  }

  return trimmed;
};

export const fetchDataQualityProjects = async (): Promise<DataQualityProject[]> => {
  const response = await client.get<TestGenProjectResponse[]>('/data-quality/testgen/projects');
  return response.data.map(mapProject);
};

export const fetchDataQualityConnections = async (
  projectKey: string
): Promise<DataQualityConnection[]> => {
  const response = await client.get<TestGenConnectionResponse[]>(
    `/data-quality/testgen/projects/${encodeURIComponent(projectKey)}/connections`
  );
  return response.data.map(mapConnection);
};

export const fetchDataQualityTableGroups = async (
  connectionId: string
): Promise<DataQualityTableGroup[]> => {
  const response = await client.get<TestGenTableGroupResponse[]>(
    `/data-quality/testgen/connections/${encodeURIComponent(connectionId)}/table-groups`
  );
  return response.data.map(mapTableGroup);
};

export const fetchDataQualityTables = async (
  tableGroupId: string
): Promise<DataQualityTable[]> => {
  const response = await client.get<TestGenTableResponse[]>(
    `/data-quality/testgen/table-groups/${encodeURIComponent(tableGroupId)}/tables`
  );
  return response.data.map(mapTable);
};

export const fetchDataQualityProfileRuns = async ({
  tableGroupId,
  limit = 50,
  includeGroups = true
}: {
  tableGroupId?: string | null;
  limit?: number;
  includeGroups?: boolean;
} = {}): Promise<DataQualityProfileRunListResponse> => {
  const params: Record<string, string | number | boolean> = {
    limit,
    includeGroups
  };

  if (tableGroupId) {
    params.tableGroupId = tableGroupId;
  }

  const response = await client.get<DataQualityProfileRunListResponse>(
    '/data-quality/profile-runs',
    { params }
  );

  return {
    runs: (response.data.runs ?? []).map(mapProfileRunEntry),
    tableGroups: (response.data.tableGroups ?? []).map(mapProfileRunTableGroup)
  };
};

export const fetchRecentProfileRuns = async (
  tableGroupId: string,
  limit = 20
): Promise<DataQualityProfileRun[]> => {
  const response = await fetchDataQualityProfileRuns({
    tableGroupId,
    limit,
    includeGroups: false
  });
  return response.runs.map(mapProfileRunEntryToLegacy);
};

export const fetchProfileRunAnomalies = async (
  profileRunId: string
): Promise<DataQualityProfileAnomaly[]> => {
  const response = await client.get<TestGenProfileAnomalyResponse[]>(
    `/data-quality/profile-runs/${encodeURIComponent(profileRunId)}/anomalies`
  );
  return response.data.map(mapProfileAnomaly);
};

export const fetchDataQualityColumnProfile = async (
  tableGroupId: string,
  columnName: string,
  tableName?: string | null,
  physicalName?: string | null
): Promise<DataQualityColumnProfile | null> => {
  const params: Record<string, string> = {
    columnName
  };

  if (tableName) {
    params.tableName = tableName;
  }

  if (physicalName) {
    params.physicalName = physicalName;
  }
  try {
    const response = await client.get<TestGenColumnProfileResponse>(
      `/data-quality/testgen/table-groups/${encodeURIComponent(tableGroupId)}/column-profile`,
      { params }
    );

    return mapColumnProfile(response.data);
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 404) {
      return null;
    }
    throw error;
  }
};

export const fetchRecentTestRuns = async (
  projectKey: string,
  limit = 20
): Promise<DataQualityTestRun[]> => {
  const response = await client.get<TestGenTestRunResponse[]>(
    `/data-quality/testgen/projects/${encodeURIComponent(projectKey)}/test-runs`,
    {
      params: { limit }
    }
  );
  return response.data.map(mapTestRun);
};

export const fetchRecentAlerts = async (
  limit = 50,
  includeAcknowledged = false
): Promise<DataQualityAlert[]> => {
  const response = await client.get<TestGenAlertResponse[]>(
    '/data-quality/testgen/alerts',
    {
      params: {
        limit,
        include_acknowledged: includeAcknowledged
      }
    }
  );
  return response.data.map(mapAlert);
};

export const acknowledgeDataQualityAlert = async (
  alertId: string,
  acknowledged = true
): Promise<void> => {
  await client.post(`/data-quality/testgen/alerts/${encodeURIComponent(alertId)}/acknowledge`, {
    acknowledged
  });
};

export const deleteDataQualityAlert = async (alertId: string): Promise<void> => {
  await client.delete(`/data-quality/testgen/alerts/${encodeURIComponent(alertId)}`);
};

export const startProfileRun = async (
  tableGroupId: string
): Promise<DataQualityProfileRunStartResponse> => {
  const response = await client.post<TestGenProfileRunStartResponse>(
    '/data-quality/testgen/profile-runs',
    {
      table_group_id: tableGroupId
    }
  );
  return { profileRunId: response.data.profile_run_id };
};

export const startTestRun = async (
  projectKey: string
): Promise<DataQualityTestRunStartResponse> => {
  const response = await client.post<TestGenTestRunStartResponse>(
    '/data-quality/testgen/test-runs',
    {
      project_key: projectKey
    }
  );
  return { testRunId: response.data.test_run_id };
};

export const fetchDatasetHierarchy = async (): Promise<DataQualityDatasetProductTeam[]> => {
  const response = await client.get<DataQualityDatasetProductTeam[]>('/data-quality/datasets');
  return response.data;
};

export const startDataObjectProfileRuns = async (
  dataObjectId: string
): Promise<DataQualityBulkProfileRunResponse> => {
  const response = await client.post<DataQualityBulkProfileRunResponse>(
    `/data-quality/datasets/${encodeURIComponent(dataObjectId)}/profile-runs`
  );
  return response.data;
};

export const fetchDataQualityTestSuites = async (params?: {
  projectKey?: string;
  dataObjectId?: string;
}): Promise<DataQualityTestSuite[]> => {
  const response = await client.get<TestGenTestSuiteResponse[]>(
    '/data-quality/testgen/test-suites',
    {
      params: {
        project_key: params?.projectKey,
        dataObjectId: params?.dataObjectId
      }
    }
  );
  return response.data.map(mapTestSuite);
};

export const fetchDataQualityTestSuite = async (
  testSuiteKey: string
): Promise<DataQualityTestSuite> => {
  const normalizedKey = sanitizeTestSuiteKey(testSuiteKey);

  const response = await client.get<TestGenTestSuiteResponse>(
    `/data-quality/testgen/test-suites/${encodeURIComponent(normalizedKey)}`
  );
  return mapTestSuite(response.data);
};

export const createDataQualityTestSuite = async (
  input: DataQualityTestSuiteInput
): Promise<DataQualityTestSuite> => {
  const payload: Record<string, unknown> = {
    name: input.name,
    severity: input.severity
  };

  if (input.description !== undefined) {
    payload.description = input.description;
  }
  if (input.projectKey !== undefined) {
    payload.projectKey = input.projectKey;
  }
  if (input.productTeamId !== undefined) {
    payload.productTeamId = input.productTeamId;
  }
  if (input.applicationId !== undefined) {
    payload.applicationId = input.applicationId;
  }
  if (input.dataObjectId !== undefined) {
    payload.dataObjectId = input.dataObjectId;
  }
  if (input.dataDefinitionId !== undefined) {
    payload.dataDefinitionId = input.dataDefinitionId;
  }

  const response = await client.post<TestGenTestSuiteResponse>('/data-quality/testgen/test-suites', payload);
  return mapTestSuite(response.data);
};

export const updateDataQualityTestSuite = async (
  testSuiteKey: string,
  input: DataQualityTestSuiteUpdate
): Promise<DataQualityTestSuite> => {
  const normalizedKey = sanitizeTestSuiteKey(testSuiteKey);

  const payload: Record<string, unknown> = {};

  if (input.name !== undefined) {
    payload.name = input.name;
  }
  if (input.description !== undefined) {
    payload.description = input.description;
  }
  if (input.severity !== undefined) {
    payload.severity = input.severity;
  }
  if (input.projectKey !== undefined) {
    payload.projectKey = input.projectKey;
  }
  if (input.productTeamId !== undefined) {
    payload.productTeamId = input.productTeamId;
  }
  if (input.applicationId !== undefined) {
    payload.applicationId = input.applicationId;
  }
  if (input.dataObjectId !== undefined) {
    payload.dataObjectId = input.dataObjectId;
  }
  if (input.dataDefinitionId !== undefined) {
    payload.dataDefinitionId = input.dataDefinitionId;
  }

  const response = await client.put<TestGenTestSuiteResponse>(
    `/data-quality/testgen/test-suites/${encodeURIComponent(normalizedKey)}`,
    payload
  );
  return mapTestSuite(response.data);
};

export const deleteDataQualityTestSuite = async (testSuiteKey: string): Promise<void> => {
  const normalizedKey = sanitizeTestSuiteKey(testSuiteKey);

  await client.delete(`/data-quality/testgen/test-suites/${encodeURIComponent(normalizedKey)}`);
};

export const fetchDataQualitySuiteTests = async (
  testSuiteKey: string
): Promise<DataQualitySuiteTest[]> => {
  const normalizedKey = sanitizeTestSuiteKey(testSuiteKey);

  const response = await client.get<TestGenSuiteTestResponse[]>(
    `/data-quality/testgen/test-suites/${encodeURIComponent(normalizedKey)}/tests`
  );
  return response.data.map(mapSuiteTest);
};

export const fetchDataQualityTestTypes = async (): Promise<DataQualityTestType[]> => {
  const response = await client.get<TestGenTestTypeResponse[]>(
    '/data-quality/testgen/test-types'
  );
  return response.data.map(mapTestType);
};

export const fetchDataQualityTestType = async (
  ruleType: string
): Promise<DataQualityTestType> => {
  const response = await client.get<TestGenTestTypeResponse>(
    `/data-quality/testgen/test-types/${encodeURIComponent(ruleType)}`
  );
  return mapTestType(response.data);
};

export const createDataQualitySuiteTest = async (
  testSuiteKey: string,
  input: DataQualitySuiteTestInput
): Promise<DataQualitySuiteTest> => {
  const normalizedKey = sanitizeTestSuiteKey(testSuiteKey);

  const payload: Record<string, unknown> = {
    name: input.name,
    ruleType: input.ruleType,
    dataDefinitionTableId: input.dataDefinitionTableId
  };
  if (input.columnName !== undefined) {
    payload.columnName = input.columnName;
  }
  if (input.definition !== undefined) {
    payload.definition = input.definition;
  }

  const response = await client.post<TestGenSuiteTestResponse>(
    `/data-quality/testgen/test-suites/${encodeURIComponent(normalizedKey)}/tests`,
    payload
  );
  return mapSuiteTest(response.data);
};

export const updateDataQualitySuiteTest = async (
  testId: string,
  input: DataQualitySuiteTestUpdate
): Promise<DataQualitySuiteTest> => {
  const payload: Record<string, unknown> = {};
  if (input.name !== undefined) {
    payload.name = input.name;
  }
  if (input.ruleType !== undefined) {
    payload.ruleType = input.ruleType;
  }
  if (input.dataDefinitionTableId !== undefined) {
    payload.dataDefinitionTableId = input.dataDefinitionTableId;
  }
  if (input.columnName !== undefined) {
    payload.columnName = input.columnName;
  }
  if (input.definition !== undefined) {
    payload.definition = input.definition;
  }

  const response = await client.put<TestGenSuiteTestResponse>(
    `/data-quality/testgen/tests/${encodeURIComponent(testId)}`,
    payload
  );
  return mapSuiteTest(response.data);
};

export const deleteDataQualitySuiteTest = async (testId: string): Promise<void> => {
  await client.delete(`/data-quality/testgen/tests/${encodeURIComponent(testId)}`);
};
