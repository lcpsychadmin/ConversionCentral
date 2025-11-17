import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
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
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import {
  acknowledgeDataQualityAlert,
  deleteDataQualityAlert,
  fetchDataQualityConnections,
  fetchDataQualityProjects,
  fetchDataQualityTableGroups,
  fetchRecentAlerts,
  fetchRecentProfileRuns,
  fetchRecentTestRuns,
  startProfileRun,
  startTestRun
} from '@services/dataQualityService';
import {
  DataQualityAlert,
  DataQualityConnection,
  DataQualityProfileRun,
  DataQualityProject,
  DataQualityTableGroup,
  DataQualityTestRun
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';

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
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [tableGroupId, setTableGroupId] = useState<string | null>(null);

  const { snackbar, showSuccess, showError } = useSnackbarFeedback();

  useEffect(() => {
    if (!projectKey && projectsQuery.data && projectsQuery.data.length > 0) {
      setProjectKey(projectsQuery.data[0].projectKey);
    }
  }, [projectKey, projectsQuery.data]);

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

  const profileRunsQuery = useQuery<DataQualityProfileRun[]>(
    ['data-quality', 'profile-runs', tableGroupId],
    () => fetchRecentProfileRuns(tableGroupId ?? '', 20),
    {
      enabled: Boolean(tableGroupId)
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

  const resolveErrorMessage = (error: unknown) =>
    error instanceof Error ? error.message : 'Unexpected error.';

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
      onSuccess: (_data, variables) => {
        queryClient.invalidateQueries(['data-quality', 'profile-runs', variables.tableGroupId]);
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

  const handleProjectChange = (event: SelectChangeEvent<string>) => {
    setProjectKey(event.target.value || null);
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

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h4" gutterBottom>
          Run History & Alerts
        </Typography>
        <Typography color="text.secondary">
          Inspect recent profiling activity, test executions, and alerts generated by TestGen.
        </Typography>
      </Box>

      <Paper elevation={1} sx={{ p: 3 }}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel id="dq-run-project-label">Project</InputLabel>
              <Select
                labelId="dq-run-project-label"
                label="Project"
                value={projectKey ?? ''}
                onChange={handleProjectChange}
                disabled={projectsQuery.isLoading || !projectsQuery.data?.length}
              >
                {projectsQuery.data?.map((project) => (
                  <MenuItem key={project.projectKey} value={project.projectKey}>
                    {project.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel id="dq-run-connection-label">Connection</InputLabel>
              <Select
                labelId="dq-run-connection-label"
                label="Connection"
                value={connectionId ?? ''}
                onChange={handleConnectionChange}
                disabled={connectionsQuery.isLoading || !connectionsQuery.data?.length}
              >
                {connectionsQuery.data?.map((connection) => (
                  <MenuItem key={connection.connectionId} value={connection.connectionId}>
                    {connection.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel id="dq-run-table-group-label">Table Group</InputLabel>
              <Select
                labelId="dq-run-table-group-label"
                label="Table Group"
                value={tableGroupId ?? ''}
                onChange={handleTableGroupChange}
                disabled={tableGroupsQuery.isLoading || !tableGroupsQuery.data?.length}
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

        <Box mt={3}>
          <Typography variant="body2" color="text.secondary">
            Project description: {selectedProject?.description ?? '—'}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Connection catalog/schema: {selectedConnection?.catalog ?? 'default'}/
            {selectedConnection?.schemaName ?? 'default'}
          </Typography>
          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={2}
            alignItems={{ xs: 'stretch', sm: 'center' }}
            mt={3}
          >
            <Button
              variant="contained"
              onClick={() =>
                tableGroupId && startProfileRunMutation.mutate({ tableGroupId })
              }
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
          <Typography variant="caption" color="text.secondary" display="block" mt={1.5}>
            Actions launch immediate TestGen executions using the current connection metadata.
          </Typography>
        </Box>
      </Paper>

      <Grid container spacing={3}>
        <Grid item xs={12} lg={6}>
          <Paper elevation={1} sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Recent Profile Runs
            </Typography>
            {profileRunsQuery.isLoading ? (
              <Box display="flex" justifyContent="center" py={3}>
                <CircularProgress size={24} />
              </Box>
            ) : profileRunsQuery.isError ? (
              <Alert severity="error">Unable to load profile runs.</Alert>
            ) : profileRunsQuery.data && profileRunsQuery.data.length > 0 ? (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Run ID</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Started</TableCell>
                    <TableCell>Completed</TableCell>
                    <TableCell align="right">Rows</TableCell>
                    <TableCell align="right">Anomalies</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {profileRunsQuery.data.map((run) => (
                    <TableRow key={run.profileRunId} hover>
                      <TableCell>{run.profileRunId}</TableCell>
                      <TableCell>{run.status}</TableCell>
                      <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                      <TableCell>{formatDateTime(run.completedAt)}</TableCell>
                      <TableCell align="right">{run.rowCount ?? '—'}</TableCell>
                      <TableCell align="right">{run.anomalyCount ?? '—'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <Typography color="text.secondary">No profile runs recorded yet.</Typography>
            )}
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

      {snackbar}
    </Stack>
  );
};

export default DataQualityRunHistoryPage;
