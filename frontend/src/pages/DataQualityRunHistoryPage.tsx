import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormHelperText,
  Grid,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import {
  acknowledgeDataQualityAlert,
  deleteDataQualityAlert,
  fetchDataQualityConnections,
  fetchDataQualityProfileRuns,
  fetchDataQualityProjects,
  fetchDataQualityTableGroups,
  fetchDatasetHierarchy,
  fetchRecentAlerts,
  fetchProfileRunAnomalies,
  fetchRecentTestRuns,
  startProfileRun,
  startTestRun
} from '@services/dataQualityService';
import {
  DataQualityAlert,
  DataQualityConnection,
  DataQualityDatasetApplication,
  DataQualityDatasetDefinition,
  DataQualityDatasetObject,
  DataQualityDatasetProductTeam,
  DataQualityProfileAnomaly,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunEntry,
  DataQualityProfileRunTableGroup,
  DataQualityProject,
  DataQualityTableGroup,
  DataQualityTestRun
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import PageHeader from '../components/common/PageHeader';

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return '—';
  }

  return parsed.toLocaleString();
};

const formatDurationMs = (duration?: number | null) => {
  if (!duration || duration <= 0) {
    return '—';
  }

  const seconds = duration / 1000;

  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);

  return `${minutes}m ${remainingSeconds}s`;
};

type ProfileStatusFilter = 'all' | 'running' | 'completed' | 'failed';

const PROFILE_STATUS_FILTERS: { value: ProfileStatusFilter; label: string }[] = [
  { value: 'all', label: 'All statuses' },
  { value: 'running', label: 'Running / queued' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed / canceled' }
];

const STATUS_RUNNING = ['running', 'queued', 'pending', 'in_progress', 'starting'];
const STATUS_FAILED = ['failed', 'error', 'errored', 'cancelled', 'canceled'];

const resolveProfileStatusCategory = (status?: string | null): ProfileStatusFilter => {
  const normalized = (status ?? '').toLowerCase();
  if (!normalized) {
    return 'completed';
  }
  if (STATUS_RUNNING.includes(normalized)) {
    return 'running';
  }
  if (STATUS_FAILED.includes(normalized)) {
    return 'failed';
  }
  return 'completed';
};

const SEVERITY_ORDER = ['critical', 'high', 'medium', 'low', 'info', 'warning'];

const formatSeveritySummary = (counts?: Record<string, number> | null): string => {
  if (!counts) {
    return '—';
  }
  const entries = Object.entries(counts).filter(([, value]) => (value ?? 0) > 0);
  if (entries.length === 0) {
    return '—';
  }
  return entries
    .sort((a, b) => {
      const aIndex = SEVERITY_ORDER.indexOf(a[0].toLowerCase());
      const bIndex = SEVERITY_ORDER.indexOf(b[0].toLowerCase());
      const normalizedA = aIndex === -1 ? SEVERITY_ORDER.length : aIndex;
      const normalizedB = bIndex === -1 ? SEVERITY_ORDER.length : bIndex;
      return normalizedA - normalizedB;
    })
    .map(([severity, value]) => `${severity}: ${value}`)
    .join(' · ');
};

const resolveSeverityChipColor = (
  severity?: string | null
): 'default' | 'error' | 'warning' | 'info' | 'success' => {
  const normalized = (severity ?? '').toLowerCase();
  if (normalized === 'critical') {
    return 'error';
  }
  if (normalized === 'high') {
    return 'warning';
  }
  if (normalized === 'medium') {
    return 'info';
  }
  if (normalized === 'low') {
    return 'success';
  }
  return 'default';
};

const useProjects = () =>
  useQuery<DataQualityProject[]>(['data-quality', 'projects'], fetchDataQualityProjects, {
    staleTime: 5 * 60 * 1000
  });

const useConnections = (projectKey: string | null) =>
  useQuery<DataQualityConnection[]>(
    ['data-quality', 'connections', projectKey],
    () => fetchDataQualityConnections(projectKey ?? ''),
    {
      enabled: Boolean(projectKey),
      staleTime: 2 * 60 * 1000
    }
  );

const useTableGroups = (connectionId: string | null) =>
  useQuery<DataQualityTableGroup[]>(
    ['data-quality', 'table-groups', connectionId],
    () => fetchDataQualityTableGroups(connectionId ?? ''),
    {
      enabled: Boolean(connectionId),
      staleTime: 2 * 60 * 1000
    }
  );

const DataQualityRunHistoryPage = () => {
  const projectsQuery = useProjects();
  const [projectKey, setProjectKey] = useState<string | null>(null);
  const [projectSelectionMode, setProjectSelectionMode] = useState<'auto' | 'manual'>('auto');
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [tableGroupId, setTableGroupId] = useState<string | null>(null);
  const [selectedProductTeamId, setSelectedProductTeamId] = useState<string | null>(null);
  const [selectedApplicationId, setSelectedApplicationId] = useState<string | null>(null);
  const [selectedDataObjectId, setSelectedDataObjectId] = useState<string | null>(null);
  const [selectedDefinitionId, setSelectedDefinitionId] = useState<string | null>(null);
  const [profileRunScope, setProfileRunScope] = useState<'selected' | 'all'>('selected');
  const [profileRunLimit, setProfileRunLimit] = useState<number>(50);
  const [profileStatusFilter, setProfileStatusFilter] = useState<ProfileStatusFilter>('all');
  const [activeAnomalyRunId, setActiveAnomalyRunId] = useState<string | null>(null);

  const { snackbar, showSuccess, showError } = useSnackbarFeedback();

  const resolveErrorMessage = (error: unknown) =>
    error instanceof Error ? error.message : 'Unexpected error.';

  const datasetQuery = useQuery<DataQualityDatasetProductTeam[]>(
    ['data-quality', 'dataset-hierarchy'],
    fetchDatasetHierarchy,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        showError(`Unable to load dataset hierarchy: ${resolveErrorMessage(error)}`);
      }
    }
  );

  const selectedProductTeam = useMemo<DataQualityDatasetProductTeam | null>(() => {
    if (!datasetQuery.data) {
      return null;
    }
    if (!selectedProductTeamId) {
      return null;
    }
    return datasetQuery.data.find((team) => team.productTeamId === selectedProductTeamId) ?? null;
  }, [datasetQuery.data, selectedProductTeamId]);

  const selectedApplication = useMemo<DataQualityDatasetApplication | null>(() => {
    if (!selectedProductTeam) {
      return null;
    }
    if (!selectedApplicationId) {
      return null;
    }
    return (
      selectedProductTeam.applications.find(
        (application) => application.applicationId === selectedApplicationId
      ) ?? null
    );
  }, [selectedApplicationId, selectedProductTeam]);

  const selectedDataObject = useMemo<DataQualityDatasetObject | null>(() => {
    if (!selectedApplication) {
      return null;
    }
    if (!selectedDataObjectId) {
      return null;
    }
    return (
      selectedApplication.dataObjects.find(
        (dataObject) => dataObject.dataObjectId === selectedDataObjectId
      ) ?? null
    );
  }, [selectedApplication, selectedDataObjectId]);

  const selectedDefinition = useMemo<DataQualityDatasetDefinition | null>(() => {
    if (!selectedDataObject) {
      return null;
    }
    if (!selectedDefinitionId) {
      return null;
    }
    return (
      selectedDataObject.dataDefinitions.find(
        (definition) => definition.dataDefinitionId === selectedDefinitionId
      ) ?? null
    );
  }, [selectedDataObject, selectedDefinitionId]);

  const derivedProjectKey = useMemo<string | null>(() => {
    if (!projectsQuery.data) {
      return null;
    }
    if (!selectedApplicationId || !selectedDataObjectId) {
      return null;
    }

    const normalizedTarget = `system:${selectedApplicationId}:object:${selectedDataObjectId}`.toLowerCase();
    const matchedProject = projectsQuery.data.find((project) => {
      const key = project?.projectKey;
      if (!key) {
        return false;
      }
      return key.toLowerCase() === normalizedTarget;
    });

    return matchedProject?.projectKey ?? null;
  }, [projectsQuery.data, selectedApplicationId, selectedDataObjectId]);

  useEffect(() => {
    if (!datasetQuery.data || datasetQuery.data.length === 0) {
      if (selectedProductTeamId !== null) {
        setSelectedProductTeamId(null);
      }
      return;
    }

    if (!selectedProductTeamId || !datasetQuery.data.some((team) => team.productTeamId === selectedProductTeamId)) {
      setSelectedProductTeamId(datasetQuery.data[0].productTeamId);
    }
  }, [datasetQuery.data, selectedProductTeamId]);

  useEffect(() => {
    if (!selectedProductTeam || selectedProductTeam.applications.length === 0) {
      if (selectedApplicationId !== null) {
        setSelectedApplicationId(null);
      }
      return;
    }

    if (
      !selectedApplicationId ||
      !selectedProductTeam.applications.some((application) => application.applicationId === selectedApplicationId)
    ) {
      setSelectedApplicationId(selectedProductTeam.applications[0].applicationId);
    }
  }, [selectedApplicationId, selectedProductTeam]);

  useEffect(() => {
    if (!selectedApplication || selectedApplication.dataObjects.length === 0) {
      if (selectedDataObjectId !== null) {
        setSelectedDataObjectId(null);
      }
      return;
    }

    if (
      !selectedDataObjectId ||
      !selectedApplication.dataObjects.some((dataObject) => dataObject.dataObjectId === selectedDataObjectId)
    ) {
      setSelectedDataObjectId(selectedApplication.dataObjects[0].dataObjectId);
    }
  }, [selectedApplication, selectedDataObjectId]);

  useEffect(() => {
    if (!selectedDataObject || selectedDataObject.dataDefinitions.length === 0) {
      if (selectedDefinitionId !== null) {
        setSelectedDefinitionId(null);
      }
      return;
    }

    if (
      !selectedDefinitionId ||
      !selectedDataObject.dataDefinitions.some((definition) => definition.dataDefinitionId === selectedDefinitionId)
    ) {
      setSelectedDefinitionId(selectedDataObject.dataDefinitions[0].dataDefinitionId);
    }
  }, [selectedDataObject, selectedDefinitionId]);

  useEffect(() => {
    if (projectSelectionMode !== 'auto') {
      return;
    }
    setProjectKey(derivedProjectKey);
  }, [projectSelectionMode, derivedProjectKey]);

  useEffect(() => {
    setConnectionId(null);
    setTableGroupId(null);
  }, [projectKey]);

  useEffect(() => {
    setProjectSelectionMode('auto');
    setConnectionId(null);
    setTableGroupId(null);
  }, [selectedApplicationId, selectedDataObjectId]);

  const connectionsQuery = useConnections(projectKey);

  useEffect(() => {
    if (!connectionId && connectionsQuery.data && connectionsQuery.data.length > 0) {
      setConnectionId(connectionsQuery.data[0].connectionId);
    }
  }, [connectionId, connectionsQuery.data]);

  const tableGroupsQuery = useTableGroups(connectionId);

  useEffect(() => {
    if (!tableGroupId && tableGroupsQuery.data && tableGroupsQuery.data.length > 0) {
      setTableGroupId(tableGroupsQuery.data[0].tableGroupId);
    }
  }, [tableGroupId, tableGroupsQuery.data]);

  const profileRunFilters = useMemo(
    () => ({
      scope: profileRunScope,
      tableGroupId,
      limit: profileRunLimit
    }),
    [profileRunScope, tableGroupId, profileRunLimit]
  );

  const profileRunsQuery = useQuery<DataQualityProfileRunListResponse>(
    ['data-quality', 'profile-runs', profileRunFilters],
    () =>
      fetchDataQualityProfileRuns({
        tableGroupId: profileRunScope === 'selected' ? tableGroupId ?? undefined : undefined,
        limit: profileRunLimit,
        includeGroups: true
      }),
    {
      enabled: profileRunScope === 'all' || Boolean(tableGroupId),
      keepPreviousData: true
    }
  );

  const profileRuns: DataQualityProfileRunEntry[] = useMemo(
    () => profileRunsQuery.data?.runs ?? [],
    [profileRunsQuery.data]
  );
  const profileRunGroups: DataQualityProfileRunTableGroup[] = useMemo(
    () => profileRunsQuery.data?.tableGroups ?? [],
    [profileRunsQuery.data]
  );
  const filteredProfileRuns = useMemo(() => {
    if (profileStatusFilter === 'all') {
      return profileRuns;
    }
    return profileRuns.filter(
      (run) => resolveProfileStatusCategory(run.status) === profileStatusFilter
    );
  }, [profileRuns, profileStatusFilter]);

  const isProfileRunsScopeDisabled = profileRunScope === 'selected' && !tableGroupId;
  const hasProfileRunGroupMetadata = profileRunGroups.length > 0;
  const activeAnomalyRun = useMemo(
    () => profileRuns.find((run) => run.profileRunId === activeAnomalyRunId) ?? null,
    [profileRuns, activeAnomalyRunId]
  );

  const profileRunAnomaliesQuery = useQuery<DataQualityProfileAnomaly[]>(
    ['data-quality', 'profile-run-anomalies', activeAnomalyRunId],
    () => fetchProfileRunAnomalies(activeAnomalyRunId ?? ''),
    {
      enabled: Boolean(activeAnomalyRunId)
    }
  );

  const testRunsQuery = useQuery<DataQualityTestRun[]>(
    ['data-quality', 'test-runs', projectKey],
    () => fetchRecentTestRuns(projectKey ?? '', 20),
    {
      enabled: Boolean(projectKey)
    }
  );

  const alertsQuery = useQuery<DataQualityAlert[]>(
    ['data-quality', 'alerts'],
    () => fetchRecentAlerts(20, true),
    {
      staleTime: 60 * 1000
    }
  );

  const queryClient = useQueryClient();

  const acknowledgeMutation = useMutation((alertId: string) => acknowledgeDataQualityAlert(alertId), {
    onSuccess: () => {
      queryClient.invalidateQueries(['data-quality', 'alerts']);
      showSuccess('Alert acknowledged.');
    },
    onError: (error) => {
      showError(`Failed to acknowledge alert: ${resolveErrorMessage(error)}`);
    }
  });

  const deleteAlertMutation = useMutation((alertId: string) => deleteDataQualityAlert(alertId), {
    onSuccess: () => {
      queryClient.invalidateQueries(['data-quality', 'alerts']);
      showSuccess('Alert deleted.');
    },
    onError: (error) => {
      showError(`Failed to delete alert: ${resolveErrorMessage(error)}`);
    }
  });

  const startProfileRunMutation = useMutation(
    ({ tableGroupId: groupId }: { tableGroupId: string }) => startProfileRun(groupId),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(['data-quality', 'profile-runs']);
        queryClient.invalidateQueries(['data-quality', 'alerts']);
        showSuccess('Profile run triggered.');
      },
      onError: (error) => {
        showError(`Failed to start profile run: ${resolveErrorMessage(error)}`);
      }
    }
  );

  const startTestRunMutation = useMutation(
    ({ projectKey: key }: { projectKey: string }) => startTestRun(key),
    {
      onSuccess: (_data, variables) => {
        queryClient.invalidateQueries(['data-quality', 'test-runs', variables.projectKey]);
        queryClient.invalidateQueries(['data-quality', 'alerts']);
        showSuccess('Test run triggered.');
      },
      onError: (error) => {
        showError(`Failed to start test run: ${resolveErrorMessage(error)}`);
      }
    }
  );

  const handleProductTeamChange = (event: SelectChangeEvent<string>) => {
    setSelectedProductTeamId(event.target.value || null);
  };

  const handleApplicationChange = (event: SelectChangeEvent<string>) => {
    setSelectedApplicationId(event.target.value || null);
  };

  const handleDataObjectChange = (event: SelectChangeEvent<string>) => {
    setSelectedDataObjectId(event.target.value || null);
  };

  const handleDefinitionChange = (event: SelectChangeEvent<string>) => {
    setSelectedDefinitionId(event.target.value || null);
  };

  const handleProjectSelectChange = (event: SelectChangeEvent<string>) => {
    const value = event.target.value || null;
    setProjectSelectionMode('manual');
    setProjectKey(value);
    setConnectionId(null);
    setTableGroupId(null);
  };

  const handleProjectOverride = () => {
    setProjectSelectionMode('manual');
    if (!projectKey && projectsQuery.data && projectsQuery.data.length > 0) {
      setProjectKey(projectsQuery.data[0].projectKey);
    }
    setConnectionId(null);
    setTableGroupId(null);
  };

  const handleUseDatasetProject = () => {
    setProjectSelectionMode('auto');
    setProjectKey(derivedProjectKey);
    setConnectionId(null);
    setTableGroupId(null);
  };

  const handleConnectionChange = (event: SelectChangeEvent<string>) => {
    setConnectionId(event.target.value || null);
    setTableGroupId(null);
  };

  const handleTableGroupChange = (event: SelectChangeEvent<string>) => {
    setTableGroupId(event.target.value || null);
  };

  const handleProfileRunScopeChange = (
    _event: React.MouseEvent<HTMLElement>,
    nextValue: 'selected' | 'all' | null
  ) => {
    if (nextValue) {
      setProfileRunScope(nextValue);
    }
  };

  const handleProfileRunLimitChange = (event: SelectChangeEvent<string>) => {
    const parsed = Number(event.target.value);
    setProfileRunLimit(Number.isNaN(parsed) ? 50 : parsed);
  };

  const handleProfileStatusFilterChange = (event: SelectChangeEvent<string>) => {
    const next = event.target.value as ProfileStatusFilter | null;
    setProfileStatusFilter(next ?? 'all');
  };

  const handleOpenAnomalyDialog = (runId: string) => {
    setActiveAnomalyRunId(runId);
  };

  const handleCloseAnomalyDialog = () => {
    setActiveAnomalyRunId(null);
  };

  const selectedProject = useMemo(() => {
    if (!projectKey || !projectsQuery.data) {
      return null;
    }

    return projectsQuery.data.find((project) => project.projectKey === projectKey) ?? null;
  }, [projectKey, projectsQuery.data]);

  const selectedConnection = useMemo(() => {
    if (!connectionId || !connectionsQuery.data) {
      return null;
    }

    return connectionsQuery.data.find((connection) => connection.connectionId === connectionId) ?? null;
  }, [connectionId, connectionsQuery.data]);

  const projectDisplayValue = projectsQuery.isLoading
    ? 'Loading projects...'
    : projectKey
    ? selectedProject?.name ?? projectKey
    : 'Not provisioned';

  const projectHelperText = projectsQuery.isLoading
    ? 'Fetching TestGen projects...'
    : projectSelectionMode === 'auto'
    ? projectKey
      ? 'Automatically derived from the selected application and data object.'
      : 'Provision this dataset in TestGen to enable run history and alerts.'
    : 'Select a TestGen project to load connections.';

  const isSingleConnection = (connectionsQuery.data?.length ?? 0) === 1;

  const connectionHelperText = !projectKey
    ? 'Select a dataset to resolve the TestGen connection.'
    : connectionsQuery.isLoading
    ? 'Loading connections...'
    : connectionsQuery.data?.length
    ? isSingleConnection
      ? 'Databricks connection selected automatically.'
      : 'Choose a connection if different Databricks catalogs are available.'
    : 'No connections available for this project.';

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Run History & Alerts"
        subtitle="Drill into profiling runs, test executions, and alerts for the selected dataset hierarchy."
      />

      <Paper elevation={1} sx={{ p: 3 }}>
        <Stack spacing={3}>
          <Grid container spacing={3}>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-product-team-label">Product Team</InputLabel>
                <Select
                  labelId="dq-run-product-team-label"
                  label="Product Team"
                  value={selectedProductTeamId ?? ''}
                  onChange={handleProductTeamChange}
                  disabled={datasetQuery.isLoading || !datasetQuery.data?.length}
                >
                  {datasetQuery.data?.map((team) => (
                    <MenuItem key={team.productTeamId} value={team.productTeamId}>
                      {team.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-application-label">Application</InputLabel>
                <Select
                  labelId="dq-run-application-label"
                  label="Application"
                  value={selectedApplicationId ?? ''}
                  onChange={handleApplicationChange}
                  disabled={
                    datasetQuery.isLoading ||
                    !selectedProductTeam ||
                    selectedProductTeam.applications.length === 0
                  }
                >
                  {selectedProductTeam?.applications.map((application) => (
                    <MenuItem key={application.applicationId} value={application.applicationId}>
                      {application.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-data-object-label">Data Object</InputLabel>
                <Select
                  labelId="dq-run-data-object-label"
                  label="Data Object"
                  value={selectedDataObjectId ?? ''}
                  onChange={handleDataObjectChange}
                  disabled={
                    datasetQuery.isLoading ||
                    !selectedApplication ||
                    selectedApplication.dataObjects.length === 0
                  }
                >
                  {selectedApplication?.dataObjects.map((dataObject) => (
                    <MenuItem key={dataObject.dataObjectId} value={dataObject.dataObjectId}>
                      {dataObject.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-definition-label">Data Definition</InputLabel>
                <Select
                  labelId="dq-run-definition-label"
                  label="Data Definition"
                  value={selectedDefinitionId ?? ''}
                  onChange={handleDefinitionChange}
                  disabled={
                    datasetQuery.isLoading ||
                    !selectedDataObject ||
                    selectedDataObject.dataDefinitions.length === 0
                  }
                >
                  {selectedDataObject?.dataDefinitions.map((definition) => (
                    <MenuItem key={definition.dataDefinitionId} value={definition.dataDefinitionId}>
                      {definition.description ?? 'Untitled definition'}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
          </Grid>

          <Grid container spacing={3}>
            <Grid item xs={12}>
              {projectSelectionMode === 'auto' ? (
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'stretch', sm: 'center' }}>
                  <TextField
                    fullWidth
                    label="TestGen Project"
                    value={projectDisplayValue}
                    helperText={projectHelperText}
                    InputProps={{ readOnly: true }}
                  />
                  {projectsQuery.data && projectsQuery.data.length > 0 ? (
                    <Button variant="outlined" onClick={handleProjectOverride} sx={{ whiteSpace: 'nowrap' }}>
                      Override project
                    </Button>
                  ) : null}
                </Stack>
              ) : (
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'stretch', sm: 'center' }}>
                  <FormControl
                    fullWidth
                    disabled={projectsQuery.isLoading || !projectsQuery.data?.length}
                  >
                    <InputLabel id="dq-run-project-select-label">TestGen Project</InputLabel>
                    <Select
                      labelId="dq-run-project-select-label"
                      label="TestGen Project"
                      value={projectKey ?? ''}
                      onChange={handleProjectSelectChange}
                    >
                      {!projectsQuery.data?.length ? (
                        <MenuItem value="" disabled>
                          {projectsQuery.isLoading ? 'Loading projects...' : 'No TestGen projects available'}
                        </MenuItem>
                      ) : null}
                      {projectsQuery.data?.map((project) => (
                        <MenuItem key={project.projectKey} value={project.projectKey}>
                          {project.name}
                        </MenuItem>
                      ))}
                    </Select>
                    <FormHelperText>{projectHelperText}</FormHelperText>
                  </FormControl>
                  <Button variant="outlined" onClick={handleUseDatasetProject} sx={{ whiteSpace: 'nowrap' }}>
                    Use dataset selection
                  </Button>
                </Stack>
              )}
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-connection-label">Connection</InputLabel>
                <Select
                  labelId="dq-run-connection-label"
                  label="Connection"
                  value={connectionId ?? ''}
                  onChange={handleConnectionChange}
                  disabled={
                    !projectKey ||
                    connectionsQuery.isLoading ||
                    !connectionsQuery.data?.length ||
                    isSingleConnection
                  }
                >
                  {connectionsQuery.data?.map((connection) => (
                    <MenuItem key={connection.connectionId} value={connection.connectionId}>
                      {connection.name}
                    </MenuItem>
                  ))}
                </Select>
                <FormHelperText>{connectionHelperText}</FormHelperText>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel id="dq-run-table-group-label">Table Group</InputLabel>
                <Select
                  labelId="dq-run-table-group-label"
                  label="Table Group"
                  value={tableGroupId ?? ''}
                  onChange={handleTableGroupChange}
                  disabled={
                    !connectionId ||
                    tableGroupsQuery.isLoading ||
                    !tableGroupsQuery.data?.length
                  }
                >
                  {tableGroupsQuery.data?.map((group) => (
                    <MenuItem key={group.tableGroupId} value={group.tableGroupId}>
                      {group.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
          </Grid>

          <Box>
            <Typography variant="body2" color="text.secondary">
              Product team description: {selectedProductTeam?.description ?? '—'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Data object description: {selectedDataObject?.description ?? '—'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Data definition description: {selectedDefinition?.description ?? '—'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              TestGen project:{' '}
              {projectKey ? `${selectedProject?.name ?? projectKey} (${projectKey})` : 'Not provisioned'}
              {' · '}
              {projectKey
                ? 'derived from the dataset selection'
                : 'dataset selection not provisioned'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Connection catalog/schema: {selectedConnection?.catalog ?? 'default'} /
              {selectedConnection?.schemaName ?? 'default'}
            </Typography>
            {selectedDefinition ? (
              <Typography variant="body2" color="text.secondary">
                Tables registered: {selectedDefinition.tables.length}
              </Typography>
            ) : null}
          </Box>

          {!projectKey && !projectsQuery.isLoading ? (
            <Alert severity="warning">
              {projectSelectionMode === 'auto'
                ? 'The selected application has not been provisioned in TestGen yet. Provision the dataset in Databricks to enable run history and alerts.'
                : 'Select a TestGen project to load connections and enable run history.'}
            </Alert>
          ) : null}

          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={2}
            alignItems={{ xs: 'stretch', sm: 'center' }}
          >
            <Button
              variant="contained"
              onClick={() => tableGroupId && startProfileRunMutation.mutate({ tableGroupId })}
              disabled={!tableGroupId || startProfileRunMutation.isLoading}
            >
              Trigger Profile Run
            </Button>
            <Button
              variant="outlined"
              onClick={() => projectKey && startTestRunMutation.mutate({ projectKey })}
              disabled={!projectKey || startTestRunMutation.isLoading}
            >
              Trigger Test Run
            </Button>
          </Stack>
          <Typography variant="caption" color="text.secondary" display="block">
            Actions launch immediate TestGen executions using the current connection metadata.
          </Typography>
        </Stack>
      </Paper>

      <Grid container spacing={3}>
        <Grid item xs={12} lg={6}>
          <Paper elevation={1} sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Recent Profile Runs
            </Typography>
            <Stack
              direction={{ xs: 'column', md: 'row' }}
              spacing={2}
              mb={2}
              alignItems={{ xs: 'stretch', md: 'center' }}
            >
              <ToggleButtonGroup
                size="small"
                value={profileRunScope}
                exclusive
                onChange={handleProfileRunScopeChange}
              >
                <ToggleButton value="selected" disabled={!tableGroupId}>
                  Selected table group
                </ToggleButton>
                <ToggleButton value="all">All table groups</ToggleButton>
              </ToggleButtonGroup>
              <FormControl size="small" sx={{ minWidth: 140 }}>
                <InputLabel id="dq-run-limit-label">Run limit</InputLabel>
                <Select
                  labelId="dq-run-limit-label"
                  label="Run limit"
                  value={String(profileRunLimit)}
                  onChange={handleProfileRunLimitChange}
                >
                  {[20, 50, 100].map((option) => (
                    <MenuItem key={option} value={String(option)}>
                      {option}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FormControl size="small" sx={{ minWidth: 180 }}>
                <InputLabel id="dq-run-status-filter-label">Status filter</InputLabel>
                <Select
                  labelId="dq-run-status-filter-label"
                  label="Status filter"
                  value={profileStatusFilter}
                  onChange={handleProfileStatusFilterChange}
                >
                  {PROFILE_STATUS_FILTERS.map((filter) => (
                    <MenuItem key={filter.value} value={filter.value}>
                      {filter.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Stack>
            {isProfileRunsScopeDisabled ? (
              <Alert severity="info">
                Select a table group or switch to &quot;All table groups&quot; to load profiling runs.
              </Alert>
            ) : profileRunsQuery.isLoading ? (
              <Box display="flex" justifyContent="center" py={3}>
                <CircularProgress size={24} />
              </Box>
            ) : profileRunsQuery.isError ? (
              <Alert severity="error">Unable to load profile runs.</Alert>
            ) : filteredProfileRuns.length > 0 ? (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Run ID</TableCell>
                    <TableCell>Table Group</TableCell>
                    <TableCell>Dataset</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Started</TableCell>
                    <TableCell>Completed</TableCell>
                    <TableCell align="right">Rows</TableCell>
                    <TableCell align="right">Anomalies</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredProfileRuns.map((run) => (
                    <TableRow key={run.profileRunId} hover>
                      <TableCell>
                        <Typography variant="body2" fontWeight={500} noWrap>
                          {run.profileRunId}
                        </Typography>
                        {run.payloadPath ? (
                          <Typography variant="caption" color="text.secondary" display="block" noWrap>
                            {run.payloadPath}
                          </Typography>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" fontWeight={500}>
                          {run.tableGroupName ?? run.tableGroupId}
                        </Typography>
                        <Typography variant="caption" color="text.secondary" display="block">
                          {run.connectionName ?? '—'} · {run.catalog ?? 'default'} / {run.schemaName ?? 'default'}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">{run.dataObjectName ?? run.dataObjectId ?? '—'}</Typography>
                        <Typography variant="caption" color="text.secondary" display="block">
                          {run.applicationName ?? '—'} · {run.productTeamName ?? '—'}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">{run.status}</Typography>
                        <Typography variant="caption" color="text.secondary" display="block">
                          {formatDurationMs(run.durationMs)}
                        </Typography>
                      </TableCell>
                      <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                      <TableCell>{formatDateTime(run.completedAt)}</TableCell>
                      <TableCell align="right">{run.rowCount ?? '—'}</TableCell>
                      <TableCell align="right">
                        <Stack spacing={0.5} alignItems="flex-end">
                          <Typography variant="body2">{run.anomalyCount ?? '—'}</Typography>
                          <Typography variant="caption" color="text.secondary" display="block">
                            {formatSeveritySummary(run.anomaliesBySeverity)}
                          </Typography>
                          {run.anomalyCount ? (
                            <Button
                              size="small"
                              variant="text"
                              onClick={() => handleOpenAnomalyDialog(run.profileRunId)}
                            >
                              View
                            </Button>
                          ) : null}
                        </Stack>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Typography color="text.secondary">
                No profile runs match the selected filters.
              </Typography>
            )}
            {hasProfileRunGroupMetadata ? (
              <Box mt={3}>
                <Typography variant="subtitle2" gutterBottom>
                  Table groups included ({profileRunGroups.length})
                </Typography>
                <Stack spacing={1.5}>
                  {profileRunGroups.map((group) => (
                    <Box
                      key={group.tableGroupId}
                      sx={{
                        border: (theme) => `1px solid ${theme.palette.divider}`,
                        borderRadius: 1,
                        p: 1.5
                      }}
                    >
                      <Typography variant="body2" fontWeight={500}>
                        {group.tableGroupName ?? group.tableGroupId}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block">
                        {group.productTeamName ?? '—'} · {group.applicationName ?? '—'} ·{' '}
                        {group.dataObjectName ?? '—'}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block">
                        {group.connectionName ?? '—'} · {group.catalog ?? 'default'} / {group.schemaName ?? 'default'}
                      </Typography>
                    </Box>
                  ))}
                </Stack>
              </Box>
            ) : null}
          </Paper>
        </Grid>

        <Grid item xs={12} lg={6}>
          <Paper elevation={1} sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Recent Test Runs
            </Typography>
            {testRunsQuery.isLoading ? (
              <Box display="flex" justifyContent="center" py={3}>
                <CircularProgress size={24} />
              </Box>
            ) : testRunsQuery.isError ? (
              <Alert severity="error">Unable to load test runs.</Alert>
            ) : testRunsQuery.data && testRunsQuery.data.length > 0 ? (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Run ID</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Started</TableCell>
                    <TableCell>Completed</TableCell>
                    <TableCell align="right">Failed</TableCell>
                    <TableCell align="right">Duration</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {testRunsQuery.data.map((run) => (
                    <TableRow key={run.testRunId} hover>
                      <TableCell>{run.testRunId}</TableCell>
                      <TableCell>{run.status}</TableCell>
                      <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                      <TableCell>{formatDateTime(run.completedAt)}</TableCell>
                      <TableCell align="right">{run.failedTests ?? 0}/{run.totalTests ?? 0}</TableCell>
                      <TableCell align="right">{formatDurationMs(run.durationMs)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Typography color="text.secondary">No test runs recorded yet.</Typography>
            )}
          </Paper>
        </Grid>
      </Grid>

      <Paper elevation={1} sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Alerts
        </Typography>
        {alertsQuery.isLoading ? (
          <Box display="flex" justifyContent="center" py={3}>
            <CircularProgress size={24} />
          </Box>
        ) : alertsQuery.isError ? (
          <Alert severity="error">Unable to load alerts.</Alert>
        ) : alertsQuery.data && alertsQuery.data.length > 0 ? (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Title</TableCell>
                <TableCell>Severity</TableCell>
                <TableCell>Source</TableCell>
                <TableCell>Created</TableCell>
                <TableCell>Acknowledged</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {alertsQuery.data.map((alert) => (
                <TableRow key={alert.alertId} hover>
                  <TableCell>{alert.title}</TableCell>
                  <TableCell>{alert.severity}</TableCell>
                  <TableCell>
                    {alert.sourceType} → {alert.sourceRef}
                  </TableCell>
                  <TableCell>{formatDateTime(alert.createdAt)}</TableCell>
                  <TableCell>{alert.acknowledged ? 'Yes' : 'No'}</TableCell>
                  <TableCell align="right">
                    <Stack direction="row" spacing={1} justifyContent="flex-end">
                      <Button
                        size="small"
                        variant="outlined"
                        disabled={alert.acknowledged || acknowledgeMutation.isLoading}
                        onClick={() => acknowledgeMutation.mutate(alert.alertId)}
                      >
                        Acknowledge
                      </Button>
                      <Button
                        size="small"
                        color="error"
                        variant="text"
                        disabled={deleteAlertMutation.isLoading}
                        onClick={() => deleteAlertMutation.mutate(alert.alertId)}
                      >
                        Delete
                      </Button>
                    </Stack>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Typography color="text.secondary">No alerts found.</Typography>
        )}
      </Paper>

      <Dialog
        open={Boolean(activeAnomalyRunId)}
        onClose={handleCloseAnomalyDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>Profiling anomalies</DialogTitle>
        <DialogContent dividers>
          {activeAnomalyRun ? (
            <Box mb={2}>
              <Typography variant="subtitle2" gutterBottom>
                Run {activeAnomalyRun.profileRunId}
              </Typography>
              <Typography variant="caption" color="text.secondary" display="block">
                {activeAnomalyRun.tableGroupName ?? activeAnomalyRun.tableGroupId} ·{' '}
                {activeAnomalyRun.connectionName ?? '—'}
              </Typography>
              <Typography variant="caption" color="text.secondary" display="block">
                {activeAnomalyRun.dataObjectName ?? activeAnomalyRun.dataObjectId ?? '—'} ·{' '}
                {activeAnomalyRun.applicationName ?? '—'} · {activeAnomalyRun.productTeamName ?? '—'}
              </Typography>
            </Box>
          ) : null}
          {profileRunAnomaliesQuery.isLoading ? (
            <Box display="flex" justifyContent="center" py={3}>
              <CircularProgress size={24} />
            </Box>
          ) : profileRunAnomaliesQuery.isError ? (
            <Alert severity="error">Unable to load anomalies.</Alert>
          ) : profileRunAnomaliesQuery.data && profileRunAnomaliesQuery.data.length > 0 ? (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Table</TableCell>
                  <TableCell>Column</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Severity</TableCell>
                  <TableCell>Detected</TableCell>
                  <TableCell>Description</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {profileRunAnomaliesQuery.data.map((anomaly, index) => (
                  <TableRow
                    key={`${anomaly.tableName ?? 'table'}-${anomaly.columnName ?? 'column'}-${index}`}
                  >
                    <TableCell>{anomaly.tableName ?? '—'}</TableCell>
                    <TableCell>{anomaly.columnName ?? '—'}</TableCell>
                    <TableCell>{anomaly.anomalyType}</TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        label={anomaly.severity}
                        color={resolveSeverityChipColor(anomaly.severity)}
                      />
                    </TableCell>
                    <TableCell>{formatDateTime(anomaly.detectedAt)}</TableCell>
                    <TableCell>{anomaly.description}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Typography color="text.secondary">No anomalies reported for this run.</Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseAnomalyDialog}>Close</Button>
        </DialogActions>
      </Dialog>

      {snackbar}
    </Stack>
  );
};

export default DataQualityRunHistoryPage;
