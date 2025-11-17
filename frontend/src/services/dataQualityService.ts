import client from './api/client';
import {
  DataQualityAlert,
  DataQualityConnection,
  DataQualityProfileRun,
  DataQualityProfileRunStartResponse,
  DataQualityProject,
  DataQualityTable,
  DataQualityTableGroup,
  DataQualityTestRun,
  DataQualityTestRunStartResponse
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

interface TestGenProfileRunResponse {
  profile_run_id: string;
  table_group_id: string;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  row_count?: number | null;
  anomaly_count?: number | null;
  payload_path?: string | null;
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

const mapProfileRun = (payload: TestGenProfileRunResponse): DataQualityProfileRun => ({
  profileRunId: payload.profile_run_id,
  tableGroupId: payload.table_group_id,
  status: payload.status,
  startedAt: payload.started_at ?? null,
  completedAt: payload.completed_at ?? null,
  rowCount: payload.row_count ?? null,
  anomalyCount: payload.anomaly_count ?? null,
  payloadPath: payload.payload_path ?? null
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

export const fetchRecentProfileRuns = async (
  tableGroupId: string,
  limit = 20
): Promise<DataQualityProfileRun[]> => {
  const response = await client.get<TestGenProfileRunResponse[]>(
    `/data-quality/testgen/table-groups/${encodeURIComponent(tableGroupId)}/profile-runs`,
    {
      params: { limit }
    }
  );
  return response.data.map(mapProfileRun);
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
