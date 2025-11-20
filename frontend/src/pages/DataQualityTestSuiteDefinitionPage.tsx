import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  IconButton,
  LinearProgress,
  MenuItem,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import { useMutation, useQuery, useQueryClient } from 'react-query';

import PageHeader from '@components/common/PageHeader';
import ConfirmDialog from '@components/common/ConfirmDialog';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import {
  fetchDataQualityTestSuite,
  fetchDataQualitySuiteTests,
  createDataQualitySuiteTest,
  updateDataQualitySuiteTest,
  deleteDataQualitySuiteTest,
  fetchDataQualityTestTypes,
  fetchDataQualityColumnProfile
} from '@services/dataQualityService';
import { fetchDataDefinitionsByContext } from '@services/dataDefinitionService';
import {
  DataDefinition,
  DataDefinitionField,
  DataDefinitionTable,
  DataQualityColumnProfile,
  DataQualityColumnMetric,
  DataQualitySuiteTest,
  DataQualitySuiteTestInput,
  DataQualitySuiteTestUpdate,
  DataQualityTestSuite,
  DataQualityTestType,
  DataQualityTestTypeParameter
} from '@cc-types/data';

interface TableOption {
  id: string;
  label: string;
  definitionId: string;
  definitionDescription: string | null;
  definitionTable: DataDefinitionTable;
}

interface ColumnOption {
  id: string;
  label: string;
  field: DataDefinitionField;
}

interface FormState {
  name: string;
  ruleType: string;
  table: TableOption | null;
  column: ColumnOption | null;
  parameters: Record<string, string>;
}

interface ColumnProfilingContext {
  tableGroupId: string;
  columnName: string;
  tableName: string | null;
  physicalName: string | null;
}

const buildColumnOptions = (table: TableOption | null): ColumnOption[] => {
  if (!table) {
    return [];
  }
  const fields: DataDefinitionField[] = table.definitionTable.fields ?? [];
  return fields
    .slice()
    .sort((left: DataDefinitionField, right: DataDefinitionField) =>
      left.field.name.localeCompare(right.field.name)
    )
    .map((fieldEntry: DataDefinitionField) => ({
      id: fieldEntry.id,
      label: fieldEntry.field.name,
      field: fieldEntry
    }));
};

const SUITE_QUERY_KEY = (testSuiteKey: string) => ['data-quality', 'test-suite', testSuiteKey];
const SUITE_TESTS_QUERY_KEY = (testSuiteKey: string) => ['data-quality', 'test-suites', testSuiteKey, 'tests'];
const DEFINITIONS_QUERY_KEY = (applicationId: string, dataObjectId: string) => [
  'data-quality',
  'definitions',
  applicationId,
  dataObjectId
];
const TEST_TYPES_QUERY_KEY = ['data-quality', 'test-types'] as const;
const COLUMN_PROFILE_QUERY_KEY = (
  tableGroupId: string,
  columnName: string,
  tableName: string | null,
  physicalName: string | null
) => ['data-quality', 'column-profile', tableGroupId, columnName, tableName, physicalName];

const normalizeParameterValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch (error) {
    return String(value);
  }
};

const parseParameterValue = (value: string): unknown => {
  const trimmed = value.trim();
  if (!trimmed) {
    return '';
  }

  const lowered = trimmed.toLowerCase();
  if (lowered === 'null') {
    return null;
  }
  if (lowered === 'true') {
    return true;
  }
  if (lowered === 'false') {
    return false;
  }

  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    const numeric = Number(trimmed);
    if (!Number.isNaN(numeric)) {
      return numeric;
    }
  }

  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']')) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      // Fall through to return the original string if JSON parsing fails.
    }
  }

  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }

  return value;
};

const formatDetailValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '—';
  }
  if (value instanceof Date) {
    return value.toLocaleString();
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return String(value);
    }
  }
  return String(value);
};

const EMPTY_TEST_TYPES: DataQualityTestType[] = [];
const EMPTY_PARAMETERS: DataQualityTestTypeParameter[] = [];

const formatMetricDisplay = (metric: DataQualityColumnMetric): string => {
  if (metric.formatted && metric.formatted.trim().length > 0) {
    return metric.formatted;
  }

  const { value, unit } = metric;

  if (value === null || value === undefined) {
    return '—';
  }

  if (typeof value === 'number') {
    const formatted = Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 4 });
    if (!unit) {
      return formatted;
    }
    return unit === '%' ? `${formatted}${unit}` : `${formatted} ${unit}`;
  }

  const text = String(value);
  if (!unit) {
    return text;
  }
  return unit === '%' ? `${text}${unit}` : `${text} ${unit}`;
};

const formatPercentageDisplay = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(2)}%`;
};

const formatCountDisplay = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(2);
};

const formatFrequencyValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toLocaleString() : value.toString();
  }
  if (typeof value === 'boolean') {
    return value ? 'True' : 'False';
  }
  return String(value);
};

const mapAnomalySeverity = (severity: string): 'error' | 'warning' | 'info' => {
  const normalized = severity.toLowerCase();
  if (normalized === 'critical' || normalized === 'high') {
    return 'error';
  }
  if (normalized === 'medium') {
    return 'warning';
  }
  return 'info';
};

const formatDateTimeDisplay = (value?: string | null): string => {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '—';
  }
  return parsed.toLocaleString();
};

const buildDefaultParameters = (metadata: DataQualityTestType | undefined): Record<string, string> => {
  const defaults: Record<string, string> = {};
  if (!metadata) {
    return defaults;
  }
  metadata.parameters.forEach((parameter) => {
    defaults[parameter.name] = parameter.defaultValue ?? '';
  });
  return defaults;
};

const buildParameterStateFromDefinition = (
  definition: Record<string, unknown> | undefined,
  metadata: DataQualityTestType | undefined
): Record<string, string> => {
  const base = buildDefaultParameters(metadata);
  if (!definition) {
    return base;
  }
  const parameters =
    (definition.parameters as Record<string, unknown> | undefined) ?? definition;
  Object.entries(parameters).forEach(([key, value]) => {
    base[key] = normalizeParameterValue(value);
  });
  return base;
};

const DataQualityTestSuiteDefinitionPage = () => {
  const { testSuiteKey: rawTestSuiteKey } = useParams<{ testSuiteKey: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { snackbar, showSuccess, showError } = useSnackbarFeedback();

  const trimmedKey = rawTestSuiteKey?.trim();
  const testSuiteKey =
    trimmedKey && trimmedKey.toLowerCase() !== 'undefined' && trimmedKey.toLowerCase() !== 'null'
      ? trimmedKey
      : undefined;
  const suiteKey = testSuiteKey ?? '';

  const [formState, setFormState] = useState<FormState>({
    name: '',
    ruleType: '',
    table: null,
    column: null,
    parameters: {}
  });
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingTest, setEditingTest] = useState<DataQualitySuiteTest | null>(null);
  const [selectedTest, setSelectedTest] = useState<DataQualitySuiteTest | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DataQualitySuiteTest | null>(null);
  const [customParamName, setCustomParamName] = useState('');
  const [customParamValue, setCustomParamValue] = useState('');
  const [profilingContext, setProfilingContext] = useState<ColumnProfilingContext | null>(null);

  const suiteQuery = useQuery<DataQualityTestSuite>(
    SUITE_QUERY_KEY(suiteKey || 'missing'),
    () => fetchDataQualityTestSuite(suiteKey),
    {
      enabled: Boolean(testSuiteKey),
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to load test suite.';
        showError(message);
      }
    }
  );

  const testTypesQuery = useQuery<DataQualityTestType[]>(
    TEST_TYPES_QUERY_KEY,
    fetchDataQualityTestTypes,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        const message =
          error instanceof Error ? error.message : 'Unable to load test types from the catalog.';
        showError(message);
      }
    }
  );

  const suite = suiteQuery.data;

  const definitionsQuery = useQuery<DataDefinition[]>(
    DEFINITIONS_QUERY_KEY(suite?.applicationId ?? '', suite?.dataObjectId ?? ''),
    () => fetchDataDefinitionsByContext(String(suite?.dataObjectId), String(suite?.applicationId)),
    {
      enabled: Boolean(suite?.applicationId && suite?.dataObjectId),
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to load data definitions.';
        showError(message);
      }
    }
  );

  const testsQuery = useQuery<DataQualitySuiteTest[]>(
    SUITE_TESTS_QUERY_KEY(suiteKey || 'missing'),
    () => fetchDataQualitySuiteTests(suiteKey),
    {
      enabled: Boolean(testSuiteKey),
      staleTime: 60 * 1000,
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to load suite definitions.';
        showError(message);
      }
    }
  );

  const profilingQuery = useQuery<DataQualityColumnProfile | null>(
    profilingContext
      ? COLUMN_PROFILE_QUERY_KEY(
          profilingContext.tableGroupId,
          profilingContext.columnName,
          profilingContext.tableName,
          profilingContext.physicalName
        )
      : ['data-quality', 'column-profile', 'idle'],
    () =>
      fetchDataQualityColumnProfile(
        profilingContext?.tableGroupId ?? '',
        profilingContext?.columnName ?? '',
        profilingContext?.tableName ?? null,
        profilingContext?.physicalName ?? null
      ),
    {
      enabled: Boolean(profilingContext),
      staleTime: 0,
      cacheTime: 0,
      retry: false,
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to load column profiling.';
        showError(message);
      }
    }
  );

  const testTypeOptions = useMemo(
    () => testTypesQuery.data ?? EMPTY_TEST_TYPES,
    [testTypesQuery.data]
  );

  const testTypeMap = useMemo(() => {
    const map = new Map<string, DataQualityTestType>();
    testTypeOptions.forEach((item) => {
      map.set(item.ruleType, item);
    });
    return map;
  }, [testTypeOptions]);

  const getTestTypeLabel = useCallback(
    (ruleType: string) => {
      const metadata = testTypeMap.get(ruleType);
      if (!metadata) {
        return ruleType;
      }
      return metadata.nameShort ?? metadata.testType ?? ruleType;
    },
    [testTypeMap]
  );

  const selectedTestType = formState.ruleType ? testTypeMap.get(formState.ruleType) : undefined;
  const selectedTestMetadata = selectedTest ? testTypeMap.get(selectedTest.ruleType) : undefined;
  const metadataParameters = useMemo(
    () => selectedTestType?.parameters ?? EMPTY_PARAMETERS,
    [selectedTestType]
  );
  const metadataParameterNames = useMemo(
    () => new Set(metadataParameters.map((parameter) => parameter.name)),
    [metadataParameters]
  );
  const extraParameterEntries = useMemo(
    () =>
      Object.entries(formState.parameters)
        .filter(([key]) => !metadataParameterNames.has(key))
        .sort((left, right) => left[0].localeCompare(right[0])),
    [formState.parameters, metadataParameterNames]
  );
  const columnHelperText =
    selectedTestType?.columnHelp ??
    selectedTestType?.columnPrompt ??
    'Select a column when the rule targets a specific field.';

  const selectedTestParameterEntries = useMemo(() => {
    if (!selectedTest) {
      return [] as Array<[string, unknown]>;
    }
    const definition = selectedTest.definition;
    if (!definition || typeof definition !== 'object' || Array.isArray(definition)) {
      return [];
    }
    const asRecord = definition as Record<string, unknown>;
    const parameterPayload =
      asRecord.parameters && typeof asRecord.parameters === 'object' && !Array.isArray(asRecord.parameters)
        ? (asRecord.parameters as Record<string, unknown>)
        : asRecord;
    return Object.entries(parameterPayload).sort((left, right) => left[0].localeCompare(right[0]));
  }, [selectedTest]);

  const selectedTestDetailSections = useMemo(() => {
    if (!selectedTest) {
      return {
        contextItems: [] as Array<{ label: string; value: unknown }>,
        metadataItems: [] as Array<{ label: string; value: unknown }>
      };
    }

    const updatedAt = selectedTest.updatedAt ? new Date(selectedTest.updatedAt) : null;

    const contextItems: Array<{ label: string; value: unknown }> = [
      { label: 'Schema', value: selectedTest.schemaName },
      { label: 'Table', value: selectedTest.tableName ?? selectedTest.physicalName ?? 'Unmapped table' },
      { label: 'Column', value: selectedTest.columnName ?? '—' },
      { label: 'Updated', value: updatedAt }
    ];

    const metadataItems: Array<{ label: string; value: unknown }> = [
      { label: 'Test Type', value: selectedTestMetadata?.testType ?? selectedTest.ruleType },
      { label: 'Short Name', value: selectedTestMetadata?.nameShort },
      { label: 'Dimension', value: selectedTestMetadata?.dqDimension },
      { label: 'Run Type', value: selectedTestMetadata?.runType },
      { label: 'Scope', value: selectedTestMetadata?.testScope },
      { label: 'Default Severity', value: selectedTestMetadata?.defaultSeverity }
    ];

    return { contextItems, metadataItems };
  }, [selectedTest, selectedTestMetadata]);

  const renderDetailItems = (items: Array<{ label: string; value: unknown }>) => (
    <Stack spacing={0.75}>
      {items.map((item) => (
        <Stack key={item.label} spacing={0.25}>
          <Typography variant="caption" color="text.secondary" sx={{ textTransform: 'uppercase', letterSpacing: 0.5 }}>
            {item.label}
          </Typography>
          <Typography variant="body2">{formatDetailValue(item.value)}</Typography>
        </Stack>
      ))}
    </Stack>
  );

  useEffect(() => {
    const suiteTests = testsQuery.data ?? [];
    if (!selectedTest) {
      return;
    }
    const refreshed = suiteTests.find((test) => test.testId === selectedTest.testId);
    if (!refreshed) {
      setSelectedTest(null);
      return;
    }
    if (refreshed !== selectedTest) {
      setSelectedTest(refreshed);
    }
  }, [testsQuery.data, selectedTest]);

  // Backfill any metadata-defined parameters that were missing when the modal opened.
  useEffect(() => {
    if (!dialogOpen || !selectedTestType) {
      return;
    }
    setFormState((prev) => {
      const defaults = buildDefaultParameters(selectedTestType);
      const missingKeys = Object.keys(defaults).filter((key) => !(key in prev.parameters));
      if (!missingKeys.length) {
        return prev;
      }
      return {
        ...prev,
        parameters: {
          ...defaults,
          ...prev.parameters
        }
      };
    });
  }, [dialogOpen, selectedTestType]);

  const tableOptions = useMemo<TableOption[]>(() => {
    const definitions = definitionsQuery.data ?? [];
    const options: TableOption[] = [];

    definitions.forEach((definition) => {
      definition.tables.forEach((table) => {
        const labelParts = [
          definition.description ?? 'Definition',
          table.alias ?? table.table.name ?? table.table.physicalName ?? 'Table'
        ];
        options.push({
          id: table.id,
          label: labelParts.filter(Boolean).join(' • '),
          definitionId: definition.id,
          definitionDescription: definition.description ?? null,
          definitionTable: table
        });
      });
    });

    return options.sort((a, b) => a.label.localeCompare(b.label));
  }, [definitionsQuery.data]);

  useEffect(() => {
    if (!dialogOpen || !editingTest) {
      return;
    }
    setFormState((prev) => {
      const matchedTable = tableOptions.find((option) => option.id === editingTest.dataDefinitionTableId) ?? prev.table;
      const availableColumns = buildColumnOptions(matchedTable ?? null);
      const matchedColumn = editingTest.columnName
        ? availableColumns.find((option) => option.label === editingTest.columnName) ?? prev.column
        : null;

      if (
        matchedTable?.id === prev.table?.id &&
        (matchedColumn?.id ?? null) === (prev.column?.id ?? null)
      ) {
        return prev;
      }

      return {
        ...prev,
        table: matchedTable ?? null,
        column: matchedColumn
      };
    });
  }, [dialogOpen, editingTest, tableOptions]);

  const resetForm = () => {
    setFormState({
      name: '',
      ruleType: '',
      table: null,
      column: null,
      parameters: {}
    });
    setEditingTest(null);
    setCustomParamName('');
    setCustomParamValue('');
  };

  const openCreateDialog = () => {
    if (!testTypeOptions.length) {
      showError('Test type metadata is unavailable. Please sync the catalog and try again.');
      return;
    }
    const defaultRuleType = testTypeOptions[0]?.ruleType ?? '';
    const metadata = defaultRuleType ? testTypeMap.get(defaultRuleType) : undefined;
    const defaultParameters = buildDefaultParameters(metadata);
    setFormState({
      name: '',
      ruleType: defaultRuleType,
      table: null,
      column: null,
      parameters: defaultParameters
    });
    setEditingTest(null);
    setCustomParamName('');
    setCustomParamValue('');
    setDialogOpen(true);
  };

  const openEditDialog = (test: DataQualitySuiteTest) => {
    const matchedTable = tableOptions.find((option) => option.id === test.dataDefinitionTableId) ?? null;
    const availableColumns = buildColumnOptions(matchedTable);
    const matchedColumn = availableColumns.find((option) => option.label === test.columnName) ?? null;
    const definition =
      (test.definition && typeof test.definition === 'object'
        ? ((test.definition.parameters as Record<string, unknown> | undefined) ?? test.definition)
        : undefined) as Record<string, unknown> | undefined;
    const metadata = testTypeMap.get(test.ruleType);
    const parameters = buildParameterStateFromDefinition(definition, metadata);

    setFormState({
      name: test.name,
      ruleType: test.ruleType,
      table: matchedTable,
      column: matchedColumn ?? null,
      parameters
    });
    setEditingTest(test);
    setCustomParamName('');
    setCustomParamValue('');
    setDialogOpen(true);
  };

  const createMutation = useMutation(
    (payload: DataQualitySuiteTestInput) => createDataQualitySuiteTest(suiteKey, payload),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(SUITE_TESTS_QUERY_KEY(suiteKey || 'missing')).catch(() => {});
        showSuccess('Test created.');
        setDialogOpen(false);
        resetForm();
      },
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to create test.';
        showError(message);
      }
    }
  );

  const updateMutation = useMutation(
    ({ testId, payload }: { testId: string; payload: DataQualitySuiteTestUpdate }) =>
      updateDataQualitySuiteTest(testId, payload),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(SUITE_TESTS_QUERY_KEY(suiteKey || 'missing')).catch(() => {});
        showSuccess('Test updated.');
        setDialogOpen(false);
        resetForm();
      },
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to update test.';
        showError(message);
      }
    }
  );

  const deleteMutation = useMutation((testId: string) => deleteDataQualitySuiteTest(testId), {
    onSuccess: () => {
      queryClient.invalidateQueries(SUITE_TESTS_QUERY_KEY(suiteKey || 'missing')).catch(() => {});
      showSuccess('Test deleted.');
      setDeleteTarget(null);
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unable to delete test.';
      showError(message);
    }
  });

  const handleDialogSubmit = () => {
    if (!formState.name.trim()) {
      showError('Please provide a test name.');
      return;
    }
    if (!formState.ruleType) {
      showError('Please select a test type.');
      return;
    }
    if (!formState.table) {
      showError('Select a table for this test.');
      return;
    }

    if (!testSuiteKey) {
      showError('Test suite identifier is missing. Navigate from the test suites list and try again.');
      return;
    }

    const definitionParameters: Record<string, unknown> = {};
    Object.entries(formState.parameters).forEach(([key, value]) => {
      definitionParameters[key] = parseParameterValue(value);
    });

    const payload: DataQualitySuiteTestInput = {
      name: formState.name.trim(),
      ruleType: formState.ruleType,
      dataDefinitionTableId: formState.table.id,
      columnName: formState.column?.label ?? null,
      definition: definitionParameters
    };

    if (editingTest) {
      updateMutation.mutate({ testId: editingTest.testId, payload });
    } else {
      createMutation.mutate(payload);
    }
  };

  const handleDialogClose = () => {
    if (createMutation.isLoading || updateMutation.isLoading) {
      return;
    }
    setDialogOpen(false);
    resetForm();
  };

  const handleDelete = () => {
    if (!deleteTarget) {
      return;
    }
    deleteMutation.mutate(deleteTarget.testId);
  };

  const updateParameterValue = (name: string, value: string) => {
    setFormState((prev) => ({
      ...prev,
      parameters: {
        ...prev.parameters,
        [name]: value
      }
    }));
  };

  const removeCustomParameter = (name: string) => {
    setFormState((prev) => {
      if (!(name in prev.parameters)) {
        return prev;
      }
      const nextParameters = { ...prev.parameters };
      delete nextParameters[name];
      return {
        ...prev,
        parameters: nextParameters
      };
    });
  };

  const handleAddCustomParameter = () => {
    const trimmed = customParamName.trim();
    if (!trimmed) {
      showError('Provide a parameter name before adding it.');
      return;
    }
    if (Object.prototype.hasOwnProperty.call(formState.parameters, trimmed)) {
      showError(`Parameter "${trimmed}" already exists.`);
      return;
    }
    const reserved = selectedTestType?.parameters?.some((parameter) => parameter.name === trimmed);
    if (reserved) {
      showError('Use the fields above to modify this parameter.');
      return;
    }
    setFormState((prev) => ({
      ...prev,
      parameters: {
        ...prev.parameters,
        [trimmed]: customParamValue
      }
    }));
    setCustomParamName('');
    setCustomParamValue('');
  };

  const handleViewProfiling = () => {
    if (!selectedTest) {
      return;
    }
    if (!selectedTest.columnName) {
      showError('Column profiling is only available when a column is specified for the test.');
      return;
    }
    if (!selectedTest.tableGroupId) {
      showError('Table group metadata is missing for this test, unable to load profiling results.');
      return;
    }

    setProfilingContext({
      tableGroupId: selectedTest.tableGroupId,
      columnName: selectedTest.columnName,
      tableName: selectedTest.tableName ?? selectedTest.physicalName ?? null,
      physicalName: selectedTest.physicalName ?? null
    });
  };

  const handleProfilingClose = () => {
    if (profilingContext) {
      const key = COLUMN_PROFILE_QUERY_KEY(
        profilingContext.tableGroupId,
        profilingContext.columnName,
        profilingContext.tableName,
        profilingContext.physicalName
      );
      queryClient.removeQueries(key);
    }
    setProfilingContext(null);
  };

  const tests = testsQuery.data ?? [];
  const profilingData = profilingQuery.data;
  const profilingIsLoading = profilingQuery.isLoading;
  const profilingIsError = profilingQuery.isError;
  const profilingErrorMessage = profilingQuery.isError
    ? profilingQuery.error instanceof Error
      ? profilingQuery.error.message
      : 'Unable to load column profiling.'
    : null;
  const isLoading = suiteQuery.isLoading || testsQuery.isLoading || testTypesQuery.isLoading;
  const testsError = testsQuery.isError;

  const renderProfilingContent = () => {
    if (profilingIsLoading) {
      return (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      );
    }

    if (profilingIsError) {
      return <Alert severity="error">{profilingErrorMessage ?? 'Unable to load column profiling.'}</Alert>;
    }

    if (!profilingData) {
      return (
        <Typography variant="body2" color="text.secondary">
          Profiling results are not available for this column yet. Trigger a profiling run to generate baseline metrics.
        </Typography>
      );
    }

    const summaryItems = [
      { label: 'Status', value: profilingData.status ?? '—' },
      { label: 'Profile Run', value: profilingData.profileRunId ?? '—' },
      { label: 'Started', value: formatDateTimeDisplay(profilingData.startedAt) },
      { label: 'Completed', value: formatDateTimeDisplay(profilingData.completedAt) },
      { label: 'Rows Profiled', value: formatCountDisplay(profilingData.rowCount ?? null) },
      { label: 'Anomaly Count', value: formatCountDisplay(profilingData.anomalies.length) }
    ];

    const histogramMax = profilingData.histogram.reduce<number>((max, bin) => {
      const count = typeof bin.count === 'number' ? bin.count : 0;
      return count > max ? count : max;
    }, 0);

    return (
      <Stack spacing={3}>
        <Stack spacing={0.25}>
          <Typography variant="h6">{profilingData.columnName}</Typography>
          <Typography variant="body2" color="text.secondary">
            {profilingData.tableName ?? 'Unmapped table'}
          </Typography>
          {profilingData.dataType ? (
            <Typography variant="body2" color="text.secondary">
              Detected type: {profilingData.dataType}
            </Typography>
          ) : null}
        </Stack>

        <Paper variant="outlined" sx={{ p: 2, backgroundColor: 'grey.50' }}>
          <Grid container spacing={2}>
            {summaryItems.map((item) => (
              <Grid item xs={12} sm={6} md={4} key={item.label}>
                <Typography variant="body2" color="text.secondary">
                  {item.label}
                </Typography>
                <Typography variant="subtitle2">{item.value}</Typography>
              </Grid>
            ))}
          </Grid>
        </Paper>

        {profilingData.metrics.length ? (
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Column Metrics
            </Typography>
            <Grid container spacing={2}>
              {profilingData.metrics.map((metric) => (
                <Grid item xs={12} sm={6} md={4} key={metric.key}>
                  <Typography variant="body2" color="text.secondary">
                    {metric.label}
                  </Typography>
                  <Typography variant="subtitle2">{formatMetricDisplay(metric)}</Typography>
                </Grid>
              ))}
            </Grid>
          </Box>
        ) : null}

        {profilingData.topValues.length ? (
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Top Values
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Value</TableCell>
                  <TableCell>Count</TableCell>
                  <TableCell>Percentage</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {profilingData.topValues.map((entry, index) => (
                  <TableRow key={`${String(entry.value)}-${index}`}>
                    <TableCell>{formatFrequencyValue(entry.value)}</TableCell>
                    <TableCell>{formatCountDisplay(entry.count ?? null)}</TableCell>
                    <TableCell>{formatPercentageDisplay(entry.percentage ?? null)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
        ) : null}

        {profilingData.histogram.length ? (
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Distribution
            </Typography>
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Stack spacing={1.5}>
                {profilingData.histogram.map((bin, index) => {
                  const count = typeof bin.count === 'number' ? bin.count : 0;
                  const percent = histogramMax > 0 ? (count / histogramMax) * 100 : 0;
                  const key = `${bin.label}-${bin.lower ?? 'min'}-${bin.upper ?? 'max'}-${index}`;
                  return (
                    <Box key={key}>
                      <Stack direction="row" justifyContent="space-between" alignItems="center">
                        <Typography variant="body2">{bin.label || 'Range'}</Typography>
                        <Typography variant="body2" color="text.secondary">
                          {formatCountDisplay(bin.count ?? null)}
                        </Typography>
                      </Stack>
                      <LinearProgress
                        variant="determinate"
                        value={percent}
                        sx={{ height: 6, borderRadius: 999, mt: 0.5 }}
                      />
                    </Box>
                  );
                })}
              </Stack>
            </Paper>
          </Box>
        ) : null}

        {profilingData.anomalies.length ? (
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Recent Anomalies
            </Typography>
            <Stack spacing={1}>
              {profilingData.anomalies.map((anomaly, index) => (
                <Alert
                  key={`${anomaly.anomalyType}-${anomaly.detectedAt ?? index}`}
                  severity={mapAnomalySeverity(anomaly.severity)}
                >
                  <Stack spacing={0.5}>
                    <Typography variant="subtitle2">{anomaly.anomalyType}</Typography>
                    <Typography variant="body2">{anomaly.description}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      {formatDateTimeDisplay(anomaly.detectedAt)}
                    </Typography>
                  </Stack>
                </Alert>
              ))}
            </Stack>
          </Box>
        ) : null}
      </Stack>
    );
  };

  return (
    <Stack spacing={3}>
      <Stack direction="row" spacing={1} alignItems="center">
        <Button startIcon={<ArrowBackIcon />} onClick={() => navigate(-1)}>
          Back
        </Button>
      </Stack>

      <PageHeader
        title={suite?.name ?? 'Test Suite'}
        subtitle={suite?.description ?? 'Configure the detailed test coverage for this suite.'}
      />

      {!testSuiteKey ? (
        <Alert severity="error">Missing test suite identifier.</Alert>
      ) : null}

      {suiteQuery.isError ? (
        <Alert severity="error">Unable to load the selected test suite.</Alert>
      ) : null}

      {definitionsQuery.isError ? (
        <Alert severity="warning">
          Data definition context could not be loaded. Existing tests will display, but creating new ones requires table metadata.
        </Alert>
      ) : null}

      {testTypesQuery.isError ? (
        <Alert severity="error">
          Test type catalog could not be loaded. Run the sync script or refresh once the metadata is available.
        </Alert>
      ) : null}

      <Paper elevation={1} sx={{ p: 3 }}>
        <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} justifyContent="space-between" alignItems={{ xs: 'flex-start', md: 'center' }}>
          <Box>
            <Typography variant="h6">Defined Tests</Typography>
            <Typography variant="body2" color="text.secondary">
              Assign rule templates to specific tables and columns. Parameters are stored alongside metadata so downstream runs stay reproducible.
            </Typography>
          </Box>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={openCreateDialog}
            disabled={
              isLoading ||
              !tableOptions.length ||
              !testSuiteKey ||
              testTypesQuery.isLoading ||
              testTypesQuery.isError
            }
          >
            New Test
          </Button>
        </Stack>

        <Divider sx={{ my: 2 }} />

        {testsError ? (
          <Alert severity="error">
            Unable to load the tests for this suite. Refresh the page or try again after verifying the data quality service.
          </Alert>
        ) : isLoading ? (
          <Box display="flex" justifyContent="center" py={4}>
            <CircularProgress />
          </Box>
        ) : tests.length > 0 ? (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Table</TableCell>
                <TableCell>Column</TableCell>
                <TableCell>Updated</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {tests.map((test) => {
                const tableLabel = test.tableName ?? test.physicalName ?? 'Unmapped table';
                const updatedAt = test.updatedAt ? new Date(test.updatedAt).toLocaleString() : '—';
                return (
                  <TableRow
                    key={test.testId}
                    hover
                    onClick={() => setSelectedTest(test)}
                    sx={{ cursor: 'pointer' }}
                  >
                    <TableCell>{test.name}</TableCell>
                    <TableCell>
                      <Chip label={getTestTypeLabel(test.ruleType)} size="small" />
                    </TableCell>
                    <TableCell>{tableLabel}</TableCell>
                    <TableCell>{test.columnName ?? '—'}</TableCell>
                    <TableCell>{updatedAt}</TableCell>
                    <TableCell align="right">
                      <Tooltip title="Edit test">
                        <span>
                          <IconButton
                            size="small"
                            onClick={(event) => {
                              event.stopPropagation();
                              openEditDialog(test);
                            }}
                          >
                            <EditIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                      <Tooltip title="Delete test">
                        <span>
                          <IconButton
                            size="small"
                            color="error"
                            onClick={(event) => {
                              event.stopPropagation();
                              setDeleteTarget(test);
                            }}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        ) : (
          <Box textAlign="center" py={6}>
            <Typography variant="body1" fontWeight={600} gutterBottom>
              No tests defined yet
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Start by adding a rule and linking it to a specific table and column within this dataset.
            </Typography>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={openCreateDialog}
              disabled={
                !tableOptions.length ||
                !testSuiteKey ||
                testTypesQuery.isLoading ||
                testTypesQuery.isError
              }
            >
              Create your first test
            </Button>
          </Box>
        )}
      </Paper>

      {selectedTest ? (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Stack spacing={1.5}>
            <Typography variant="h6">{selectedTest.name}</Typography>
            <Stack direction="row" spacing={1} alignItems="center">
              <Chip label={getTestTypeLabel(selectedTest.ruleType)} size="small" />
              <Typography color="text.secondary">{selectedTest.columnName ?? 'Column not set'}</Typography>
            </Stack>
            <Typography variant="body2" color="text.secondary">
              {selectedTest.tableName ?? selectedTest.physicalName ?? 'No table selected'}
            </Typography>
            {selectedTestMetadata?.description ? (
              <Typography variant="body2">{selectedTestMetadata.description}</Typography>
            ) : null}
            {(selectedTestDetailSections.contextItems.length || selectedTestDetailSections.metadataItems.length) ? (
              <Box
                display="grid"
                gridTemplateColumns={{ xs: '1fr', md: 'repeat(2, minmax(0, 1fr))' }}
                gap={2}
              >
                {selectedTestDetailSections.contextItems.length
                  ? renderDetailItems(selectedTestDetailSections.contextItems)
                  : null}
                {selectedTestDetailSections.metadataItems.length
                  ? renderDetailItems(selectedTestDetailSections.metadataItems)
                  : null}
              </Box>
            ) : null}
            {selectedTestMetadata?.usageNotes ? (
              <Paper variant="outlined" sx={{ p: 2, backgroundColor: 'grey.50' }}>
                <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: 'pre-wrap' }}>
                  {selectedTestMetadata.usageNotes}
                </Typography>
              </Paper>
            ) : null}
            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Parameters
              </Typography>
              {selectedTestParameterEntries.length ? (
                <Paper variant="outlined" sx={{ p: 2, backgroundColor: 'grey.50' }}>
                  {renderDetailItems(
                    selectedTestParameterEntries.map(([key, value]) => ({
                      label: key,
                      value
                    }))
                  )}
                </Paper>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  This test does not define any parameters.
                </Typography>
              )}
            </Box>
            <Box>
              <Button variant="outlined" onClick={handleViewProfiling}>
                View Column Profiling
              </Button>
            </Box>
          </Stack>
        </Paper>
      ) : null}

      <Dialog open={dialogOpen} onClose={handleDialogClose} fullWidth maxWidth="md">
        <DialogTitle>{editingTest ? 'Edit Test' : 'Create Test'}</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Test Name"
              value={formState.name}
              onChange={(event) => setFormState((prev) => ({ ...prev, name: event.target.value }))}
              required
              fullWidth
            />
            <TextField
              select
              label="Test Type"
              value={formState.ruleType}
              onChange={(event) => {
                const nextRuleType = event.target.value;
                const metadata = testTypeMap.get(nextRuleType);
                const defaults = buildDefaultParameters(metadata);
                setFormState((prev) => ({
                  ...prev,
                  ruleType: nextRuleType,
                  parameters: defaults
                }));
                setCustomParamName('');
                setCustomParamValue('');
              }}
            >
              {testTypeOptions.map((option) => (
                <MenuItem key={option.ruleType} value={option.ruleType}>
                  <Stack>
                    <Typography fontWeight={600}>{getTestTypeLabel(option.ruleType)}</Typography>
                    <Typography variant="body2" color="text.secondary">
                      {option.description ?? option.usageNotes ?? 'Metadata-driven test type'}
                    </Typography>
                  </Stack>
                </MenuItem>
              ))}
            </TextField>
            <Autocomplete
              options={tableOptions}
              value={formState.table}
              onChange={(_, value) => {
                setFormState((prev) => ({
                  ...prev,
                  table: value,
                  column: null
                }));
              }}
              getOptionLabel={(option) => option.label}
              isOptionEqualToValue={(option, value) => option.id === value.id}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Table"
                  placeholder="Select a table"
                  helperText={tableOptions.length ? 'Choose the table that this test should evaluate.' : 'No tables available for this suite.'}
                />
              )}
            />
            <Autocomplete
              options={buildColumnOptions(formState.table)}
              value={formState.column}
              onChange={(_, value) => setFormState((prev) => ({ ...prev, column: value }))}
              getOptionLabel={(option) => option.label}
              isOptionEqualToValue={(option, value) => option.id === value.id}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Column"
                  placeholder="Select a column (optional)"
                  helperText={columnHelperText}
                />
              )}
              disabled={!formState.table}
            />
            {selectedTestType?.description ? (
              <Typography variant="body2" color="text.secondary">
                {selectedTestType.description}
              </Typography>
            ) : null}
            {selectedTestType?.usageNotes ? (
              <Alert severity="info" sx={{ whiteSpace: 'pre-wrap' }}>
                {selectedTestType.usageNotes}
              </Alert>
            ) : null}
            <Stack spacing={1.5}>
              <Typography variant="subtitle2">Parameters</Typography>
              {metadataParameters.length ? (
                metadataParameters.map((parameter) => (
                  <TextField
                    key={parameter.name}
                    label={parameter.prompt ?? parameter.name.replace(/_/g, ' ')}
                    value={formState.parameters[parameter.name] ?? ''}
                    onChange={(event) => updateParameterValue(parameter.name, event.target.value)}
                    helperText={parameter.help ?? undefined}
                    placeholder={parameter.defaultValue ?? ''}
                    fullWidth
                  />
                ))
              ) : (
                <Typography variant="body2" color="text.secondary">
                  This test type does not define any parameters. You can still add custom overrides below if needed.
                </Typography>
              )}
            </Stack>
            <Stack spacing={1.5}>
              <Typography variant="subtitle2">Custom Parameters</Typography>
              {extraParameterEntries.length ? (
                <Stack spacing={1}>
                  {extraParameterEntries.map(([key, value]) => (
                    <Stack
                      key={key}
                      direction={{ xs: 'column', sm: 'row' }}
                      spacing={1}
                      alignItems={{ xs: 'stretch', sm: 'flex-start' }}
                    >
                      <TextField
                        label={key}
                        value={value}
                        onChange={(event) => updateParameterValue(key, event.target.value)}
                        fullWidth
                      />
                      <IconButton
                        aria-label={`Remove parameter ${key}`}
                        onClick={() => removeCustomParameter(key)}
                        size="small"
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Stack>
                  ))}
                </Stack>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  No custom parameters added yet.
                </Typography>
              )}
              <Stack
                direction={{ xs: 'column', sm: 'row' }}
                spacing={1}
                alignItems={{ xs: 'stretch', sm: 'flex-end' }}
              >
                <TextField
                  label="Parameter Name"
                  value={customParamName}
                  onChange={(event) => setCustomParamName(event.target.value)}
                  placeholder="threshold_buffer"
                  fullWidth
                />
                <TextField
                  label="Value"
                  value={customParamValue}
                  onChange={(event) => setCustomParamValue(event.target.value)}
                  placeholder="42"
                  fullWidth
                />
                <Button
                  variant="outlined"
                  onClick={handleAddCustomParameter}
                  disabled={!customParamName.trim()}
                >
                  Add
                </Button>
              </Stack>
            </Stack>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleDialogClose} disabled={createMutation.isLoading || updateMutation.isLoading}>
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={handleDialogSubmit}
            disabled={createMutation.isLoading || updateMutation.isLoading || !tableOptions.length}
          >
            {editingTest ? 'Save Changes' : 'Create Test'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={Boolean(profilingContext)}
        onClose={handleProfilingClose}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>Column Profiling</DialogTitle>
        <DialogContent dividers>{renderProfilingContent()}</DialogContent>
        <DialogActions>
          <Button onClick={handleProfilingClose}>Close</Button>
        </DialogActions>
      </Dialog>

      <ConfirmDialog
        open={Boolean(deleteTarget)}
        title="Delete Test"
        description={deleteTarget ? `Delete "${deleteTarget.name}" from this suite? This action cannot be undone.` : ''}
        onClose={() => setDeleteTarget(null)}
        onConfirm={handleDelete}
        confirmColor="error"
        confirmLabel="Delete"
        loading={deleteMutation.isLoading}
      />

      {snackbar}
    </Stack>
  );
};

export default DataQualityTestSuiteDefinitionPage;
