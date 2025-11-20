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
import { Link as RouterLink } from 'react-router-dom';

import PageHeader from '@components/common/PageHeader';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import {
  fetchDataQualityProfileRuns,
  fetchProfileRunAnomalies,
  startProfileRun
} from '@services/dataQualityService';
import {
  DataQualityProfileAnomaly,
  DataQualityProfileRunEntry,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunTableGroup
} from '@cc-types/data';

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
  { value: 'running', label: 'In progress' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed' }
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

const formatSeveritySummary = (counts?: Record<string, number> | null): string => {
  if (!counts) {
    return '—';
  }
  const entries = Object.entries(counts).filter(([, value]) => (value ?? 0) > 0);
  if (entries.length === 0) {
    return '—';
  }
  const severityOrder = ['critical', 'high', 'medium', 'low', 'info', 'warning'];
  return entries
    .sort((a, b) => {
      const aIndex = severityOrder.indexOf(a[0].toLowerCase());
      const bIndex = severityOrder.indexOf(b[0].toLowerCase());
      const normalizedA = aIndex === -1 ? severityOrder.length : aIndex;
      const normalizedB = bIndex === -1 ? severityOrder.length : bIndex;
      return normalizedA - normalizedB;
    })
    .map(([severity, value]) => `${severity}: ${value}`)
    .join(' · ');
};

const LIMIT_OPTIONS = [25, 50, 100];

const DataQualityProfilingRunsPage = () => {
  const queryClient = useQueryClient();
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();

  const [selectedGroupId, setSelectedGroupId] = useState<string>('all');
  const [statusFilter, setStatusFilter] = useState<ProfileStatusFilter>('all');
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [limit, setLimit] = useState<number>(50);

  const profileRunsQuery = useQuery<DataQualityProfileRunListResponse>(
    ['data-quality', 'profiling-runs', limit],
    () =>
      fetchDataQualityProfileRuns({
        limit,
        includeGroups: true
      }),
    {
      keepPreviousData: true,
      staleTime: 30 * 1000
    }
  );

  const runs: DataQualityProfileRunEntry[] = useMemo(
    () => profileRunsQuery.data?.runs ?? [],
    [profileRunsQuery.data]
  );

  const tableGroups: DataQualityProfileRunTableGroup[] = useMemo(
    () => profileRunsQuery.data?.tableGroups ?? [],
    [profileRunsQuery.data]
  );

  useEffect(() => {
    if (selectedGroupId === 'all') {
      return;
    }
    const exists = tableGroups.some((group) => group.tableGroupId === selectedGroupId);
    if (!exists) {
      setSelectedGroupId('all');
    }
  }, [selectedGroupId, tableGroups]);

  const filteredRuns = useMemo(() => {
    return runs.filter((run) => {
      if (selectedGroupId !== 'all' && run.tableGroupId !== selectedGroupId) {
        return false;
      }
      if (statusFilter === 'all') {
        return true;
      }
      return resolveProfileStatusCategory(run.status) === statusFilter;
    });
  }, [runs, selectedGroupId, statusFilter]);

  const activeRun = useMemo(
    () => runs.find((run) => run.profileRunId === activeRunId) ?? null,
    [runs, activeRunId]
  );

  const profileRunAnomaliesQuery = useQuery<DataQualityProfileAnomaly[]>(
    ['data-quality', 'profile-run-anomalies', activeRunId],
    () => fetchProfileRunAnomalies(activeRunId ?? ''),
    {
      enabled: Boolean(activeRunId)
    }
  );

  const mutation = useMutation(
    ({ tableGroupId }: { tableGroupId: string }) => startProfileRun(tableGroupId),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(['data-quality', 'profiling-runs']);
        showSuccess('Profiling run started.');
      },
      onError: (error: unknown) => {
        const message = error instanceof Error ? error.message : 'Unexpected error starting run.';
        showError(message);
      }
    }
  );

  const handleGroupChange = (event: SelectChangeEvent<string>) => {
    setSelectedGroupId(event.target.value);
  };

  const handleStatusChange = (event: SelectChangeEvent<string>) => {
    setStatusFilter((event.target.value as ProfileStatusFilter | null) ?? 'all');
  };

  const handleLimitChange = (event: SelectChangeEvent<string>) => {
    const parsed = Number(event.target.value);
    setLimit(Number.isFinite(parsed) ? parsed : 50);
  };

  const handleStartRun = () => {
    if (selectedGroupId === 'all') {
      showError('Select a table group before starting a profiling run.');
      return;
    }
    mutation.mutate({ tableGroupId: selectedGroupId });
  };

  const handleOpenAnomalies = (runId: string) => {
    setActiveRunId(runId);
  };

  const handleCloseAnomalies = () => {
    setActiveRunId(null);
  };

  return (
    <Box>
      <PageHeader
        title="Profiling Runs"
        subtitle="Monitor recent profiling activity, trigger new jobs, and drill into anomalies without leaving Conversion Central."
        actions={
          <>
            <Button
              component={RouterLink}
              to="/data-quality/datasets"
              variant="outlined"
              color="secondary"
            >
              Profiling Schedules
            </Button>
            <Button
              variant="contained"
              onClick={handleStartRun}
              disabled={selectedGroupId === 'all' || mutation.isLoading}
            >
              {mutation.isLoading ? 'Starting…' : 'Run Profiling'}
            </Button>
          </>
        }
      />

      <Paper sx={{ p: 3 }}>
        <Stack
          direction={{ xs: 'column', md: 'row' }}
          spacing={2}
          alignItems={{ xs: 'stretch', md: 'center' }}
        >
          <FormControl sx={{ minWidth: 220 }} size="small">
            <InputLabel id="table-group-filter-label">Table group</InputLabel>
            <Select
              labelId="table-group-filter-label"
              label="Table group"
              value={selectedGroupId}
              onChange={handleGroupChange}
            >
              <MenuItem value="all">All table groups</MenuItem>
              {tableGroups.map((group) => (
                <MenuItem key={group.tableGroupId} value={group.tableGroupId}>
                  {group.tableGroupName ?? group.tableGroupId}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <FormControl sx={{ minWidth: 180 }} size="small">
            <InputLabel id="status-filter-label">Status</InputLabel>
            <Select
              labelId="status-filter-label"
              label="Status"
              value={statusFilter}
              onChange={handleStatusChange}
            >
              {PROFILE_STATUS_FILTERS.map((filter) => (
                <MenuItem key={filter.value} value={filter.value}>
                  {filter.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <FormControl sx={{ minWidth: 140 }} size="small">
            <InputLabel id="limit-filter-label">Show</InputLabel>
            <Select
              labelId="limit-filter-label"
              label="Show"
              value={String(limit)}
              onChange={handleLimitChange}
            >
              {LIMIT_OPTIONS.map((option) => (
                <MenuItem key={option} value={option}>
                  {option} runs
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Stack>

        <Box mt={3}>
          {profileRunsQuery.isLoading ? (
            <Box display="flex" justifyContent="center" py={4}>
              <CircularProgress size={32} />
            </Box>
          ) : profileRunsQuery.isError ? (
            <Alert severity="error">Unable to load profiling runs.</Alert>
          ) : filteredRuns.length === 0 ? (
            <Alert severity="info">No profiling runs match the selected filters.</Alert>
          ) : (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Run</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Started</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell align="right">Rows</TableCell>
                  <TableCell align="right">Hygiene issues</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredRuns.map((run) => (
                  <TableRow key={run.profileRunId} hover>
                    <TableCell>
                      <Typography variant="body2" fontWeight={600} noWrap>
                        {run.profileRunId}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" noWrap>
                        {run.tableGroupName ?? run.tableGroupId} · {run.connectionName ?? '—'}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" noWrap>
                        {run.catalog ?? 'default'} / {run.schemaName ?? 'default'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={run.status}
                        size="small"
                        color={(() => {
                          const category = resolveProfileStatusCategory(run.status);
                          if (category === 'running') return 'info';
                          if (category === 'failed') return 'error';
                          return 'success';
                        })()}
                      />
                    </TableCell>
                    <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                    <TableCell>{formatDurationMs(run.durationMs)}</TableCell>
                    <TableCell align="right">{run.rowCount ?? '—'}</TableCell>
                    <TableCell align="right">
                      <Typography variant="body2" fontWeight={600}>
                        {run.anomalyCount ?? '—'}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {formatSeveritySummary(run.anomaliesBySeverity)}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Stack direction="row" spacing={1} justifyContent="flex-end">
                        {run.payloadPath ? (
                          <Button
                            component="a"
                            href={run.payloadPath}
                            target="_blank"
                            rel="noopener noreferrer"
                            size="small"
                          >
                            Results
                          </Button>
                        ) : null}
                        {run.anomalyCount ? (
                          <Button size="small" onClick={() => handleOpenAnomalies(run.profileRunId)}>
                            Anomalies
                          </Button>
                        ) : null}
                      </Stack>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </Box>
      </Paper>

      <Dialog open={Boolean(activeRun)} onClose={handleCloseAnomalies} fullWidth maxWidth="md">
        <DialogTitle>Profiling anomalies</DialogTitle>
        <DialogContent dividers>
          {profileRunAnomaliesQuery.isLoading ? (
            <Box display="flex" justifyContent="center" py={4}>
              <CircularProgress size={24} />
            </Box>
          ) : profileRunAnomaliesQuery.isError ? (
            <Alert severity="error">Unable to load anomalies for this run.</Alert>
          ) : profileRunAnomaliesQuery.data && profileRunAnomaliesQuery.data.length > 0 ? (
            <Stack spacing={2}>
              {profileRunAnomaliesQuery.data.map((anomaly, index) => (
                <Box key={`${anomaly.tableName}-${anomaly.columnName}-${index}`}>
                  <Typography variant="subtitle2" gutterBottom>
                    {anomaly.tableName ?? 'Unknown table'} · {anomaly.columnName ?? '—'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {anomaly.anomalyType} · {anomaly.severity}
                  </Typography>
                  <Typography variant="body2">{anomaly.description}</Typography>
                </Box>
              ))}
            </Stack>
          ) : (
            <Typography color="text.secondary">
              No anomalies were captured for this run.
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseAnomalies}>Close</Button>
        </DialogActions>
      </Dialog>

      {snackbar}
    </Box>
  );
};

export default DataQualityProfilingRunsPage;
