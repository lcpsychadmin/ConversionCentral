import { Box, Button, CircularProgress, Grid, List, ListItem, ListItemText, Paper, Stack, Typography, Chip, Divider } from '@mui/material';
import { useEffect, useMemo, useRef } from 'react';
import { useQuery } from 'react-query';
import {
  fetchDataQualityConnections,
  fetchDataQualityProjects,
  fetchRecentAlerts,
  fetchRecentTestRuns
} from '@services/dataQualityService';
import { DataQualityAlert, DataQualityProject, DataQualityTestRun } from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

interface OverviewSnapshot {
  projects: DataQualityProject[];
  connectionsCount: number;
  alerts: DataQualityAlert[];
  recentTestRuns: DataQualityTestRun[];
}

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
};

const useOverview = () =>
  useQuery<OverviewSnapshot>(
    ['data-quality', 'overview'],
    async () => {
      const projects = await fetchDataQualityProjects();
      const projectKeys = projects.map((project) => project.projectKey);
      const [alerts, connectionLists, testRunLists] = await Promise.all([
        fetchRecentAlerts(10, true),
        projectKeys.length
          ? Promise.all(projectKeys.map((key) => fetchDataQualityConnections(key)))
          : Promise.resolve([]),
        projectKeys.length
          ? Promise.all(projectKeys.map((key) => fetchRecentTestRuns(key, 5)))
          : Promise.resolve([])
      ]);

      const connectionsCount = connectionLists.reduce((total, list) => total + list.length, 0);
      const combinedTestRuns = testRunLists
        .flat()
        .sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''))
        .slice(0, 10);

      return {
        projects,
        connectionsCount,
        alerts,
        recentTestRuns: combinedTestRuns
      } satisfies OverviewSnapshot;
    },
    {
      staleTime: 60 * 1000
    }
  );

const SummaryCard = ({ title, value }: { title: string; value: string }) => (
  <Paper elevation={1} sx={{ p: 3 }}>
    <Typography variant="overline" color="text.secondary">
      {title}
    </Typography>
    <Typography variant="h4" fontWeight={600} mt={1}>
      {value}
    </Typography>
  </Paper>
);

const EmptyState = ({ message }: { message: string }) => (
  <Paper elevation={0} sx={{ p: 3, textAlign: 'center', color: 'text.secondary' }}>
    <Typography>{message}</Typography>
  </Paper>
);

const DataQualityOverviewPage = () => {
  const { snackbar, showSuccess, showError } = useSnackbarFeedback();
  const { data, isLoading, isError, error, refetch } = useOverview();
  const hasShownError = useRef(false);

  useEffect(() => {
    if (isError && error && !hasShownError.current) {
      showError(`Failed to load the data quality overview: ${resolveErrorMessage(error)}`);
      hasShownError.current = true;
    } else if (!isError) {
      hasShownError.current = false;
    }
  }, [isError, error, showError]);

  const handleRetry = async () => {
    try {
      const result = await refetch();
      if (result.error) {
        showError(`Failed to refresh the overview: ${resolveErrorMessage(result.error)}`);
        return;
      }
      showSuccess('Overview data refreshed.');
    } catch (err) {
      showError(`Failed to refresh the overview: ${resolveErrorMessage(err)}`);
    }
  };

  const openAlertCount = useMemo(() => {
    if (!data) {
      return 0;
    }
    return data.alerts.filter((alert) => !alert.acknowledged).length;
  }, [data]);

  if (isLoading) {
    return (
      <>
        <Box display="flex" justifyContent="center" alignItems="center" minHeight="50vh">
          <CircularProgress />
        </Box>
        {snackbar}
      </>
    );
  }

  if (isError || !data) {
    return (
      <>
        <Box>
          <Typography variant="h5" gutterBottom>
            Data Quality Overview
          </Typography>
          <Paper elevation={0} sx={{ p: 3 }}>
            <Typography color="error" gutterBottom>
              Unable to load data quality summary. Please try again.
            </Typography>
            <Button variant="contained" onClick={handleRetry}>
              Retry
            </Button>
          </Paper>
        </Box>
        {snackbar}
      </>
    );
  }

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h4" gutterBottom>
          Data Quality Overview
        </Typography>
        <Typography color="text.secondary">
          Snapshot of TestGen projects, connections, and recent runs across the platform.
        </Typography>
      </Box>

      <Grid container spacing={3}>
        <Grid item xs={12} md={4}>
          <SummaryCard title="Projects" value={data.projects.length.toString()} />
        </Grid>
        <Grid item xs={12} md={4}>
          <SummaryCard title="Active Connections" value={data.connectionsCount.toString()} />
        </Grid>
        <Grid item xs={12} md={4}>
          <SummaryCard title="Open Alerts" value={openAlertCount.toString()} />
        </Grid>
      </Grid>

      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper elevation={1} sx={{ p: 3, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Latest Test Runs
            </Typography>
            {data.recentTestRuns.length === 0 ? (
              <EmptyState message="No test runs recorded yet." />
            ) : (
              <List disablePadding>
                {data.recentTestRuns.map((run, index) => (
                  <Box key={`${run.testRunId}-${index}`}>
                    {index > 0 ? <Divider component="li" /> : null}
                    <ListItem alignItems="flex-start">
                      <ListItemText
                        primary={
                          <Stack direction="row" alignItems="center" spacing={1}>
                            <Typography fontWeight={600}>{run.testRunId}</Typography>
                            <Chip
                              size="small"
                              label={run.status}
                              color={
                                run.status === 'failed'
                                  ? 'error'
                                  : run.status === 'completed'
                                  ? 'success'
                                  : 'default'
                              }
                            />
                          </Stack>
                        }
                        secondary={
                          <Stack spacing={0.5} mt={1}>
                            <Typography variant="body2" color="text.secondary">
                              Project: {run.projectKey}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Started: {formatDateTime(run.startedAt)}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Failed Tests: {run.failedTests ?? 0} / {run.totalTests ?? 0}
                            </Typography>
                          </Stack>
                        }
                      />
                    </ListItem>
                  </Box>
                ))}
              </List>
            )}
          </Paper>
        </Grid>

        <Grid item xs={12} md={6}>
          <Paper elevation={1} sx={{ p: 3, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Recent Alerts
            </Typography>
            {data.alerts.length === 0 ? (
              <EmptyState message="No alerts have been generated." />
            ) : (
              <List disablePadding>
                {data.alerts.map((alert, index) => (
                  <Box key={`${alert.alertId}-${index}`}>
                    {index > 0 ? <Divider component="li" /> : null}
                    <ListItem alignItems="flex-start">
                      <ListItemText
                        primary={
                          <Stack direction="row" alignItems="center" spacing={1}>
                            <Typography fontWeight={600}>{alert.title}</Typography>
                            <Chip
                              size="small"
                              color={alert.acknowledged ? 'default' : 'warning'}
                              label={alert.acknowledged ? 'Acknowledged' : 'New'}
                            />
                          </Stack>
                        }
                        secondary={
                          <Stack spacing={0.5} mt={1}>
                            <Typography variant="body2" color="text.secondary">
                              Severity: {alert.severity}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Source: {alert.sourceType} → {alert.sourceRef}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Created: {formatDateTime(alert.createdAt)}
                            </Typography>
                          </Stack>
                        }
                      />
                    </ListItem>
                  </Box>
                ))}
              </List>
            )}
          </Paper>
        </Grid>
      </Grid>
      {snackbar}
    </Stack>
  );
};

export default DataQualityOverviewPage;
