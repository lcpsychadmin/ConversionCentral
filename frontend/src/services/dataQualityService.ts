import axios from 'axios';
import client from './api/client';
import {
  DataQualityAlert,
  DataQualityBulkProfileRunResponse,
  DataQualityColumnCharacteristics,
  DataQualityColumnHistogramBin,
  DataQualityColumnMetric,
  DataQualityColumnProfile,
  DataQualityColumnProfileLayout,
  DataQualityColumnProfileType,
  DataQualityColumnTags,
  DataQualityColumnValueFrequency,
  DataQualityColumnIssueSummary,
  DataQualityColumnRelatedSuite,
  DataQualityConnection,
  DataQualityDatasetProductTeam,
  DataQualityDatasetTableContext,
  DataQualityNumericBoxPlot,
  DataQualityNumericColumnProfile,
  DataQualityNumericProfileStats,
  DataQualityProfileAnomaly,
  DataQualityProfileRun,
  DataQualityProfileRunDeleteResponse,
  DataQualityProfileRunEntry,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunResultResponse,
  DataQualityProfileRunResultSummary,
  DataQualityProfileRunStartResponse,
  DataQualityProfileRunTableGroup,
  DataQualityProfileTableEntry,
  DataQualityProfileColumnEntry,
  DataQualityProfileValueEntry,
  DataQualityProfileAnomalyEntry,
  DataQualityProfilingSchedule,
  DataQualityProfilingScheduleInput,
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
  DataQualityTestTypeParameter,
  DataQualityTextCaseBucket,
  DataQualityTextColumnProfile,
  DataQualityTextPatternStat,
  DataQualityTextProfileStats
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

interface TestGenColumnIssueSummaryResponse {
  total?: number | null;
  severity_counts?: Record<string, number> | null;
  severityCounts?: Record<string, number> | null;
  items?: TestGenProfileAnomalyResponse[] | null;
}

interface TestGenColumnRelatedSuiteResponse {
  test_suite_key: string;
  name: string;
  severity?: string | null;
}

interface TestGenNumericDistributionBarResponse {
  key?: string | null;
  label?: string | null;
  count?: number | null;
  percentage?: number | null;
}

interface TestGenNumericBoxPlotResponse {
  min?: number | null;
  p25?: number | null;
  median?: number | null;
  p75?: number | null;
  max?: number | null;
  mean?: number | null;
  std_dev?: number | null;
  stdDev?: number | null;
}

interface TestGenNumericProfileStatsResponse {
  record_count?: number | null;
  recordCount?: number | null;
  value_count?: number | null;
  valueCount?: number | null;
  distinct_count?: number | null;
  distinctCount?: number | null;
  average?: number | null;
  stddev?: number | null;
  minimum?: number | null;
  minimum_positive?: number | null;
  minimumPositive?: number | null;
  maximum?: number | null;
  percentile_25?: number | null;
  percentile25?: number | null;
  median?: number | null;
  percentile_75?: number | null;
  percentile75?: number | null;
  zero_count?: number | null;
  zeroCount?: number | null;
  null_count?: number | null;
  nullCount?: number | null;
}

interface TestGenNumericColumnProfileResponse {
  stats?: TestGenNumericProfileStatsResponse | null;
  distribution_bars?: TestGenNumericDistributionBarResponse[] | null;
  distributionBars?: TestGenNumericDistributionBarResponse[] | null;
  box_plot?: TestGenNumericBoxPlotResponse | null;
  boxPlot?: TestGenNumericBoxPlotResponse | null;
  histogram?: TestGenColumnHistogramResponse[] | null;
  bins?: TestGenColumnHistogramResponse[] | null;
  top_values?: TestGenColumnValueFrequencyResponse[] | null;
  topValues?: TestGenColumnValueFrequencyResponse[] | null;
}

interface TestGenTextPatternStatResponse {
  label?: string | null;
  count?: number | null;
  percentage?: number | null;
}

interface TestGenTextCaseBucketResponse extends TestGenTextPatternStatResponse {}

interface TestGenTextProfileStatsResponse {
  record_count?: number | null;
  recordCount?: number | null;
  value_count?: number | null;
  valueCount?: number | null;
  missing_count?: number | null;
  missingCount?: number | null;
  missing_percentage?: number | null;
  missingPercentage?: number | null;
  duplicate_count?: number | null;
  duplicateCount?: number | null;
  duplicate_percentage?: number | null;
  duplicatePercentage?: number | null;
  zero_count?: number | null;
  zeroCount?: number | null;
  numeric_only_count?: number | null;
  numericOnlyCount?: number | null;
  quoted_count?: number | null;
  quotedCount?: number | null;
  leading_space_count?: number | null;
  leadingSpaceCount?: number | null;
  embedded_space_count?: number | null;
  embeddedSpaceCount?: number | null;
  average_embedded_spaces?: number | null;
  averageEmbeddedSpaces?: number | null;
  min_length?: number | null;
  minLength?: number | null;
  max_length?: number | null;
  maxLength?: number | null;
  avg_length?: number | null;
  avgLength?: number | null;
  min_text?: string | null;
  minText?: string | null;
  max_text?: string | null;
  maxText?: string | null;
  distinct_patterns?: number | null;
  distinctPatterns?: number | null;
  standard_pattern_matches?: number | null;
  standardPatternMatches?: number | null;
}

interface TestGenTextColumnProfileResponse {
  stats?: TestGenTextProfileStatsResponse | null;
  missing_breakdown?: TestGenTextPatternStatResponse[] | null;
  missingBreakdown?: TestGenTextPatternStatResponse[] | null;
  duplicate_breakdown?: TestGenTextPatternStatResponse[] | null;
  duplicateBreakdown?: TestGenTextPatternStatResponse[] | null;
  case_breakdown?: TestGenTextCaseBucketResponse[] | null;
  caseBreakdown?: TestGenTextCaseBucketResponse[] | null;
  frequent_patterns?: TestGenTextPatternStatResponse[] | null;
  frequentPatterns?: TestGenTextPatternStatResponse[] | null;
  top_values?: TestGenColumnValueFrequencyResponse[] | null;
  topValues?: TestGenColumnValueFrequencyResponse[] | null;
  length_histogram?: TestGenColumnHistogramResponse[] | null;
  lengthHistogram?: TestGenColumnHistogramResponse[] | null;
}

interface TestGenColumnProfileLayoutResponse {
  column_type?: string | null;
  columnType?: string | null;
  characteristics?: Record<string, unknown> | null;
  tags?: Record<string, unknown> | null;
  pii_signals?: TestGenColumnIssueSummaryResponse | null;
  piiSignals?: TestGenColumnIssueSummaryResponse | null;
  hygiene_issues?: TestGenColumnIssueSummaryResponse | null;
  hygieneIssues?: TestGenColumnIssueSummaryResponse | null;
  test_issues?: TestGenColumnIssueSummaryResponse | null;
  testIssues?: TestGenColumnIssueSummaryResponse | null;
  related_test_suites?: TestGenColumnRelatedSuiteResponse[] | null;
  relatedTestSuites?: TestGenColumnRelatedSuiteResponse[] | null;
  badges?: string[] | null;
  numeric_profile?: TestGenNumericColumnProfileResponse | null;
  numericProfile?: TestGenNumericColumnProfileResponse | null;
  text_profile?: TestGenTextColumnProfileResponse | null;
  textProfile?: TestGenTextColumnProfileResponse | null;
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
  profile_layout?: TestGenColumnProfileLayoutResponse | null;
  profileLayout?: TestGenColumnProfileLayoutResponse | null;
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

const mapProfileResultValue = (
  payload: DataQualityProfileValueEntry
): DataQualityProfileValueEntry => ({
  value: payload.value ?? null,
  count: payload.count ?? null,
  percentage: payload.percentage ?? null,
  label: payload.label ?? null,
  lower: payload.lower ?? null,
  upper: payload.upper ?? null
});

const mapProfileResultAnomaly = (
  payload: DataQualityProfileAnomalyEntry
): DataQualityProfileAnomalyEntry => ({
  anomalyTypeId: payload.anomalyTypeId ?? null,
  severity: payload.severity ?? null,
  likelihood: payload.likelihood ?? null,
  detail: payload.detail ?? null,
  piiRisk: payload.piiRisk ?? null,
  dqDimension: payload.dqDimension ?? null,
  columnName: payload.columnName ?? null,
  detectedAt: payload.detectedAt ?? null
});

const mapProfileResultColumn = (
  payload: DataQualityProfileColumnEntry
): DataQualityProfileColumnEntry => ({
  columnId: payload.columnId ?? null,
  columnName: payload.columnName,
  schemaName: payload.schemaName ?? null,
  tableName: payload.tableName ?? null,
  dataType: payload.dataType ?? null,
  generalType: payload.generalType ?? null,
  metrics: payload.metrics ?? {},
  rowCount: payload.rowCount ?? null,
  nullCount: payload.nullCount ?? null,
  distinctCount: payload.distinctCount ?? null,
  nonNullCount: payload.nonNullCount ?? null,
  minValue: payload.minValue ?? null,
  maxValue: payload.maxValue ?? null,
  avgValue: payload.avgValue ?? null,
  stddevValue: payload.stddevValue ?? null,
  medianValue: payload.medianValue ?? null,
  p95Value: payload.p95Value ?? null,
  topValues: payload.topValues?.map(mapProfileResultValue) ?? [],
  histogram: payload.histogram?.map(mapProfileResultValue) ?? [],
  anomalies: payload.anomalies?.map(mapProfileResultAnomaly) ?? []
});

const mapProfileResultTable = (
  payload: DataQualityProfileTableEntry
): DataQualityProfileTableEntry => ({
  tableId: payload.tableId ?? null,
  tableGroupId: payload.tableGroupId ?? null,
  schemaName: payload.schemaName ?? null,
  tableName: payload.tableName ?? null,
  metrics: payload.metrics ?? {},
  columns: payload.columns?.map(mapProfileResultColumn) ?? [],
  anomalies: payload.anomalies?.map(mapProfileResultAnomaly) ?? []
});

const mapProfileResultSummary = (
  payload: DataQualityProfileRunResultSummary
): DataQualityProfileRunResultSummary => ({
  profileRunId: payload.profileRunId,
  tableGroupId: payload.tableGroupId,
  status: payload.status ?? null,
  startedAt: payload.startedAt ?? null,
  completedAt: payload.completedAt ?? null,
  rowCount: payload.rowCount ?? null,
  anomalyCount: payload.anomalyCount ?? null,
  databricksRunId: payload.databricksRunId ?? null
});

const mapProfileRunResult = (
  payload: DataQualityProfileRunResultResponse
): DataQualityProfileRunResultResponse => ({
  tableGroupId: payload.tableGroupId,
  profileRunId: payload.profileRunId,
  summary: mapProfileResultSummary(payload.summary),
  tables: payload.tables?.map(mapProfileResultTable) ?? []
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

const pickValue = <T>(
  source: Record<string, unknown> | undefined | null,
  ...keys: string[]
): T | null => {
  if (!source) {
    return null;
  }
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      const value = source[key];
      if (value !== undefined && value !== null) {
        return value as T;
      }
    }
  }
  return null;
};

const pickString = (
  source: Record<string, unknown> | undefined | null,
  ...keys: string[]
): string | null => {
  const value = pickValue<unknown>(source, ...keys);
  if (value === null) {
    return null;
  }
  if (typeof value === 'string') {
    return value;
  }
  return String(value);
};

const pickNumber = (
  source: Record<string, unknown> | undefined | null,
  ...keys: string[]
): number | null => {
  const value = pickValue<unknown>(source, ...keys);
  if (value === null) {
    return null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const pickBoolean = (
  source: Record<string, unknown> | undefined | null,
  ...keys: string[]
): boolean | null => {
  const value = pickValue<unknown>(source, ...keys);
  if (value === null) {
    return null;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  if (typeof value === 'string') {
    const lowered = value.toLowerCase();
    if (lowered === 'true' || lowered === 'yes' || lowered === '1') {
      return true;
    }
    if (lowered === 'false' || lowered === 'no' || lowered === '0') {
      return false;
    }
  }
  return null;
};

const mapColumnCharacteristics = (
  payload?: Record<string, unknown> | null
): DataQualityColumnCharacteristics | null => {
  if (!payload) {
    return null;
  }
  const characteristics: DataQualityColumnCharacteristics = {
    semanticType: pickString(payload, 'semantic_type', 'semanticType'),
    suggestedType: pickString(payload, 'suggested_type', 'suggestedType'),
    firstDetectedAt: pickString(payload, 'first_detected_at', 'firstDetectedAt'),
    semanticConfidence: pickNumber(payload, 'semantic_confidence', 'semanticConfidence')
  };
  if (
    !characteristics.semanticType &&
    !characteristics.suggestedType &&
    !characteristics.firstDetectedAt &&
    characteristics.semanticConfidence === null
  ) {
    return null;
  }
  return characteristics;
};

const mapColumnTags = (
  payload?: Record<string, unknown> | null
): DataQualityColumnTags | null => {
  if (!payload) {
    return null;
  }
  const tags: DataQualityColumnTags = {
    stakeholderGroup: pickString(payload, 'stakeholder_group', 'stakeholderGroup'),
    dataSource: pickString(payload, 'data_source', 'dataSource'),
    sourceSystem: pickString(payload, 'source_system', 'sourceSystem'),
    sourceProcess: pickString(payload, 'source_process', 'sourceProcess'),
    businessDomain: pickString(payload, 'business_domain', 'businessDomain'),
    transformLevel: pickString(payload, 'transform_level', 'transformLevel'),
    dataProduct: pickString(payload, 'data_product', 'dataProduct'),
    criticalDataElement: pickBoolean(payload, 'critical_data_element', 'criticalDataElement'),
    piiRisk: pickString(payload, 'pii_risk', 'piiRisk')
  };
  if (Object.values(tags).every((value) => value === null || value === undefined)) {
    return null;
  }
  return tags;
};

const mapIssueSummary = (
  payload?: TestGenColumnIssueSummaryResponse | null
): DataQualityColumnIssueSummary | null => {
  if (!payload) {
    return null;
  }
  const severityCounts = payload.severity_counts ?? payload.severityCounts ?? null;
  const items = (payload.items ?? []) as TestGenProfileAnomalyResponse[];
  const mappedItems = items.map(mapProfileAnomaly);
  if (
    payload.total === undefined &&
    !severityCounts &&
    !mappedItems.length
  ) {
    return null;
  }
  return {
    total: payload.total ?? mappedItems.length,
    severityCounts,
    items: mappedItems
  };
};

const mapRelatedSuite = (
  payload: TestGenColumnRelatedSuiteResponse
): DataQualityColumnRelatedSuite => ({
  testSuiteKey: payload.test_suite_key,
  name: payload.name,
  severity: mapSeverity(payload.severity)
});

const mapNumericProfileStats = (
  payload?: TestGenNumericProfileStatsResponse | null
): DataQualityNumericProfileStats | null => {
  if (!payload) {
    return null;
  }
  const stats: DataQualityNumericProfileStats = {
    recordCount: payload.record_count ?? payload.recordCount ?? null,
    valueCount: payload.value_count ?? payload.valueCount ?? null,
    distinctCount: payload.distinct_count ?? payload.distinctCount ?? null,
    average: payload.average ?? null,
    stddev: payload.stddev ?? null,
    minimum: payload.minimum ?? null,
    minimumPositive: payload.minimum_positive ?? payload.minimumPositive ?? null,
    maximum: payload.maximum ?? null,
    percentile25: payload.percentile_25 ?? payload.percentile25 ?? null,
    median: payload.median ?? null,
    percentile75: payload.percentile_75 ?? payload.percentile75 ?? null,
    zeroCount: payload.zero_count ?? payload.zeroCount ?? null,
    nullCount: payload.null_count ?? payload.nullCount ?? null
  };
  if (Object.values(stats).every((value) => value === null || value === undefined)) {
    return null;
  }
  return stats;
};

const normalizeDistributionKey = (value?: string | null): 'nonZero' | 'zero' | 'null' => {
  const normalized = (value ?? '').toLowerCase();
  if (normalized.includes('zero')) {
    return 'zero';
  }
  if (normalized.includes('null')) {
    return 'null';
  }
  return 'nonZero';
};

const mapNumericProfile = (
  payload?: TestGenNumericColumnProfileResponse | null
): DataQualityNumericColumnProfile | null => {
  if (!payload) {
    return null;
  }
  const stats = mapNumericProfileStats(payload.stats);
  const distributionSource = payload.distribution_bars ?? payload.distributionBars ?? [];
  const distributionBars = distributionSource
    .filter(Boolean)
    .map((entry) => ({
      key: normalizeDistributionKey(entry.key) as 'nonZero' | 'zero' | 'null',
      label: entry.label ?? '',
      count: entry.count ?? 0,
      percentage: entry.percentage ?? null
    }));
  const boxPlotSource = payload.box_plot ?? payload.boxPlot ?? null;
  const boxPlot: DataQualityNumericBoxPlot | null = boxPlotSource
    ? {
        min: boxPlotSource.min ?? null,
        p25: boxPlotSource.p25 ?? null,
        median: boxPlotSource.median ?? null,
        p75: boxPlotSource.p75 ?? null,
        max: boxPlotSource.max ?? null,
        mean: boxPlotSource.mean ?? null,
        stdDev: boxPlotSource.std_dev ?? boxPlotSource.stdDev ?? null
      }
    : null;
  const histogramSource = payload.histogram ?? payload.bins ?? [];
  const histogram = histogramSource.map(mapColumnHistogramBin);
  const topValuesSource = payload.top_values ?? payload.topValues ?? [];
  const topValues = topValuesSource.map(mapColumnValueFrequency);

  if (
    !stats &&
    !distributionBars.length &&
    !boxPlot &&
    !histogram.length &&
    !topValues.length
  ) {
    return null;
  }

  return {
    stats,
    distributionBars,
    boxPlot,
    histogram,
    topValues
  };
};

const mapTextProfileStats = (
  payload?: TestGenTextProfileStatsResponse | null
): DataQualityTextProfileStats | null => {
  if (!payload) {
    return null;
  }
  const stats: DataQualityTextProfileStats = {
    recordCount: payload.record_count ?? payload.recordCount ?? null,
    valueCount: payload.value_count ?? payload.valueCount ?? null,
    missingCount: payload.missing_count ?? payload.missingCount ?? null,
    missingPercentage: payload.missing_percentage ?? payload.missingPercentage ?? null,
    duplicateCount: payload.duplicate_count ?? payload.duplicateCount ?? null,
    duplicatePercentage: payload.duplicate_percentage ?? payload.duplicatePercentage ?? null,
    zeroCount: payload.zero_count ?? payload.zeroCount ?? null,
    numericOnlyCount: payload.numeric_only_count ?? payload.numericOnlyCount ?? null,
    quotedCount: payload.quoted_count ?? payload.quotedCount ?? null,
    leadingSpaceCount: payload.leading_space_count ?? payload.leadingSpaceCount ?? null,
    embeddedSpaceCount: payload.embedded_space_count ?? payload.embeddedSpaceCount ?? null,
    averageEmbeddedSpaces: payload.average_embedded_spaces ?? payload.averageEmbeddedSpaces ?? null,
    minLength: payload.min_length ?? payload.minLength ?? null,
    maxLength: payload.max_length ?? payload.maxLength ?? null,
    avgLength: payload.avg_length ?? payload.avgLength ?? null,
    minText: payload.min_text ?? payload.minText ?? null,
    maxText: payload.max_text ?? payload.maxText ?? null,
    distinctPatterns: payload.distinct_patterns ?? payload.distinctPatterns ?? null,
    standardPatternMatches: payload.standard_pattern_matches ?? payload.standardPatternMatches ?? null
  };
  if (Object.values(stats).every((value) => value === null || value === undefined)) {
    return null;
  }
  return stats;
};

const mapPatternStats = (
  payload?: (TestGenTextPatternStatResponse | null | undefined)[] | null
): DataQualityTextPatternStat[] => {
  if (!payload) {
    return [];
  }
  return payload
    .filter(Boolean)
    .map((entry) => ({
      label: entry?.label ?? '',
      count: entry?.count ?? null,
      percentage: entry?.percentage ?? null
    }));
};

const mapTextProfile = (
  payload?: TestGenTextColumnProfileResponse | null
): DataQualityTextColumnProfile | null => {
  if (!payload) {
    return null;
  }
  const stats = mapTextProfileStats(payload.stats);
  const missingBreakdown = mapPatternStats(payload.missing_breakdown ?? payload.missingBreakdown);
  const duplicateBreakdown = mapPatternStats(payload.duplicate_breakdown ?? payload.duplicateBreakdown);
  const caseBreakdown = mapPatternStats(payload.case_breakdown ?? payload.caseBreakdown) as DataQualityTextCaseBucket[];
  const frequentPatterns = mapPatternStats(payload.frequent_patterns ?? payload.frequentPatterns);
  const topValues = (payload.top_values ?? payload.topValues ?? []).map(mapColumnValueFrequency);
  const lengthHistogram = (payload.length_histogram ?? payload.lengthHistogram ?? []).map(mapColumnHistogramBin);

  if (
    !stats &&
    !missingBreakdown.length &&
    !duplicateBreakdown.length &&
    !caseBreakdown.length &&
    !frequentPatterns.length &&
    !topValues.length &&
    !lengthHistogram.length
  ) {
    return null;
  }

  return {
    stats,
    missingBreakdown,
    duplicateBreakdown,
    caseBreakdown,
    frequentPatterns,
    topValues,
    lengthHistogram
  };
};

const mapColumnProfileLayout = (
  payload?: TestGenColumnProfileLayoutResponse | null
): DataQualityColumnProfileLayout | null => {
  if (!payload) {
    return null;
  }

  const columnType = (payload.column_type ?? payload.columnType ?? 'other') as DataQualityColumnProfileType;
  const relatedSuitesSource = payload.related_test_suites ?? payload.relatedTestSuites ?? [];
  const badges = payload.badges ?? [];

  return {
    columnType,
    characteristics: mapColumnCharacteristics(payload.characteristics ?? null),
    tags: mapColumnTags(payload.tags ?? null),
    piiSignals: mapIssueSummary(payload.pii_signals ?? payload.piiSignals ?? null),
    hygieneIssues: mapIssueSummary(payload.hygiene_issues ?? payload.hygieneIssues ?? null),
    testIssues: mapIssueSummary(payload.test_issues ?? payload.testIssues ?? null),
    relatedTestSuites: relatedSuitesSource.map(mapRelatedSuite),
    badges: badges ?? [],
    numericProfile: mapNumericProfile(payload.numeric_profile ?? payload.numericProfile ?? null),
    textProfile: mapTextProfile(payload.text_profile ?? payload.textProfile ?? null)
  };
};

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
  anomalies: (payload.anomalies ?? []).map(mapProfileAnomaly),
  profileLayout: mapColumnProfileLayout(payload.profile_layout ?? payload.profileLayout ?? null)
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

export const fetchProfileRunResults = async (
  profileRunId: string,
  tableGroupId: string
): Promise<DataQualityProfileRunResultResponse> => {
  const response = await client.get<DataQualityProfileRunResultResponse>(
    `/data-quality/profile-runs/${encodeURIComponent(profileRunId)}/results`,
    {
      params: { tableGroupId }
    }
  );
  return mapProfileRunResult(response.data);
};

export const deleteProfileRuns = async (
  profileRunIds: string[]
): Promise<DataQualityProfileRunDeleteResponse> => {
  const response = await client.delete<DataQualityProfileRunDeleteResponse>(
    '/data-quality/profile-runs',
    {
      data: {
        profileRunIds
      }
    }
  );
  return response.data;
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
    `/data-quality/table-groups/${encodeURIComponent(tableGroupId)}/profile-runs`
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

export const fetchDataQualityTableContext = async (
  dataDefinitionTableId: string
): Promise<DataQualityDatasetTableContext> => {
  const response = await client.get<DataQualityDatasetTableContext>(
    `/data-quality/datasets/tables/${encodeURIComponent(dataDefinitionTableId)}/context`
  );
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

export const fetchProfilingSchedules = async (): Promise<DataQualityProfilingSchedule[]> => {
  const response = await client.get<DataQualityProfilingSchedule[]>('/data-quality/profiling-schedules');
  return response.data;
};

export const createProfilingSchedule = async (
  input: DataQualityProfilingScheduleInput
): Promise<DataQualityProfilingSchedule> => {
  const payload = {
    tableGroupId: input.tableGroupId,
    scheduleExpression: input.scheduleExpression,
    timezone: input.timezone ?? 'UTC',
    isActive: input.isActive ?? true
  };
  const response = await client.post<DataQualityProfilingSchedule>('/data-quality/profiling-schedules', payload);
  return response.data;
};

export const deleteProfilingSchedule = async (profilingScheduleId: string): Promise<void> => {
  await client.delete(`/data-quality/profiling-schedules/${profilingScheduleId}`);
};
