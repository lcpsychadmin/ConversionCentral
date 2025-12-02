import { ChangeEvent, useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import { useMutation, useQueries, useQuery, useQueryClient, UseQueryResult } from 'react-query';
import { useLocation, useNavigate } from 'react-router-dom';

import PageHeader from '@components/common/PageHeader';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import {
  fetchDataQualityProfileRuns,
  fetchProfileRunAnomalies,
  fetchProfilingSchedules,
  startProfileRun,
  createProfilingSchedule,
  deleteProfilingSchedule,
  deleteProfileRuns
} from '@services/dataQualityService';
import {
  DataQualityProfileAnomaly,
  DataQualityProfileRunEntry,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunTableGroup,
  DataQualityProfilingSchedule,
  DataQualityProfilingScheduleInput
} from '@cc-types/data';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';

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

const describeScheduleLastRun = (schedule: DataQualityProfilingSchedule) => {
  if (!schedule.lastRunStatus && !schedule.lastRunStartedAt) {
    return 'No profiling runs have been submitted by this schedule yet.';
  }
  const details: string[] = [];
  if (schedule.lastRunStartedAt) {
    details.push(`Started ${formatDateTime(schedule.lastRunStartedAt)}`);
  }
  if (schedule.lastRunCompletedAt) {
    details.push(`Completed ${formatDateTime(schedule.lastRunCompletedAt)}`);
  }
  if (schedule.lastRunError) {
    details.push(schedule.lastRunError);
  }
  if (!details.length) {
    details.push('Waiting for next window.');
  }
  return details.join(' · ');
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

const resolveStatusChipColor = (
  status?: string | null
): 'default' | 'info' | 'success' | 'warning' | 'error' => {
  const normalized = (status ?? '').toLowerCase();
  if (!normalized || STATUS_RUNNING.includes(normalized)) {
    return 'info';
  }
  if (STATUS_FAILED.includes(normalized)) {
    return 'error';
  }
  return 'success';
};

const isTerminalStatus = (status?: string | null) => {
  const normalized = (status ?? '').toLowerCase();
  if (!normalized) {
    return false;
  }
  if (STATUS_RUNNING.includes(normalized)) {
    return false;
  }
  return true;
};

const formatStatusLabel = (status?: string | null) => {
  if (!status) {
    return 'Pending';
  }
  const normalized = status.replace(/_/g, ' ').trim();
  return normalized.length ? normalized[0].toUpperCase() + normalized.slice(1) : 'Pending';
};

const describeStatusTimestamps = (run: {
  status?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
}) => {
  const statusLabel = formatStatusLabel(run.status);
  const started = run.startedAt ? formatDateTime(run.startedAt) : null;
  const completed = run.completedAt ? formatDateTime(run.completedAt) : null;
  if (completed) {
    return `${statusLabel} • Completed ${completed}`;
  }
  if (started) {
    return `${statusLabel} • Started ${started}`;
  }
  return statusLabel;
};

const buildCronExpression = (builder: CronBuilderState): string => {
  const minute = builder.minute || '0';
  const hour = builder.hour || '0';
  const dayOfWeek = builder.dayOfWeek || 'MON';
  const dayOfMonth = builder.dayOfMonth || '1';
  switch (builder.frequency) {
    case 'hourly':
      return `${minute} * * * *`;
    case 'daily':
      return `${minute} ${hour} * * *`;
    case 'weekly':
      return `${minute} ${hour} * * ${dayOfWeek}`;
    case 'monthly':
      return `${minute} ${hour} ${dayOfMonth} * *`;
    case 'custom':
    default:
      return builder.customExpression.trim() || DEFAULT_CRON_BUILDER.customExpression;
  }
};

const DAY_OF_WEEK_OPTIONS = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'];
const DAY_OF_MONTH_OPTIONS = Array.from({ length: 28 }, (_, index) => String(index + 1));

const MINUTE_OPTIONS = Array.from({ length: 60 }, (_, index) => String(index));
const HOUR_OPTIONS = Array.from({ length: 24 }, (_, index) => String(index));

const filterSchedules = (
  schedules: DataQualityProfilingSchedule[],
  filters: ScheduleFilters
): DataQualityProfilingSchedule[] => {
  const normalizedFilters = {
    tableGroup: filters.tableGroup.trim().toLowerCase(),
    connection: filters.connection.trim().toLowerCase(),
    timezone: filters.timezone.trim().toLowerCase(),
    status: filters.status.trim().toLowerCase()
  };
  return schedules.filter((schedule) => {
    if (
      normalizedFilters.tableGroup &&
      !(`${schedule.tableGroupName ?? ''} ${schedule.tableGroupId}`.toLowerCase().includes(normalizedFilters.tableGroup))
    ) {
      return false;
    }
    if (
      normalizedFilters.connection &&
      !(`${schedule.connectionName ?? ''}`.toLowerCase().includes(normalizedFilters.connection))
    ) {
      return false;
    }
    if (
      normalizedFilters.timezone &&
      !(schedule.timezone ?? 'utc').toLowerCase().includes(normalizedFilters.timezone)
    ) {
      return false;
    }
    if (
      normalizedFilters.status &&
      !(`${schedule.lastRunStatus ?? ''}`.toLowerCase().includes(normalizedFilters.status))
    ) {
      return false;
    }
    return true;
  });
};

const formatTableGroupLabel = (group: DataQualityProfileRunTableGroup): string => {
  const application = group.applicationName?.trim();
  const definition =
    group.tableGroupName?.trim() ??
    group.dataObjectName?.trim() ??
    group.connectionName?.trim() ??
    group.tableGroupId;
  if (application && definition) {
    return `${application} · ${definition}`;
  }
  return definition ?? application ?? group.tableGroupId;
};

const formatTableGroupDropdownLabel = (group: DataQualityProfileRunTableGroup): string => {
  const parts: string[] = [];
  if (group.applicationName) {
    parts.push(group.applicationName);
  }
  const definition = group.dataObjectName ?? group.tableGroupName ?? group.tableGroupId;
  if (definition) {
    parts.push(definition);
  }
  return parts.join(' ') || '—';
};

const HYGIENE_SEVERITY_GROUPS: { label: string; color: 'error' | 'warning' | 'info' | 'default'; keys: string[] }[] = [
  { label: 'Definite', color: 'error', keys: ['critical', 'definite'] },
  { label: 'Likely', color: 'warning', keys: ['high', 'likely'] },
  { label: 'Possible', color: 'info', keys: ['medium', 'possible'] },
  { label: 'Dismissed', color: 'default', keys: ['dismissed'] }
];

const summarizeHygieneIssues = (counts?: Record<string, number> | null) => {
  if (!counts) {
    return [];
  }
  return HYGIENE_SEVERITY_GROUPS.map((group) => {
    const total = group.keys.reduce((sum, key) => sum + (counts[key] ?? 0), 0);
    if (!total) {
      return null;
    }
    return { ...group, count: total };
  }).filter(Boolean) as Array<{ label: string; color: 'error' | 'warning' | 'info' | 'default'; count: number }>;
};

const formatTableFieldSummary = (tableCount?: number | null, fieldCount?: number | null) => {
  const parts: string[] = [];
  if (typeof tableCount === 'number') {
    parts.push(`${tableCount} table${tableCount === 1 ? '' : 's'}`);
  }
  if (typeof fieldCount === 'number') {
    parts.push(`${fieldCount} field${fieldCount === 1 ? '' : 's'}`);
  }
  return parts.length ? parts.join(', ') : '—';
};

const buildProfilingRunResultsPath = (
  profileRunId: string,
  tableGroupId: string,
  options: { tableGroupLabel?: string | null } = {}
) => {
  const encodedRunId = encodeURIComponent(profileRunId);
  const params = new URLSearchParams({ tableGroupId });
  if (options.tableGroupLabel) {
    params.set('tableGroupLabel', options.tableGroupLabel);
  }
  return `/data-quality/profiling-runs/${encodedRunId}/results?${params.toString()}`;
};

const renderStatusBadge = (status?: string | null) => (
  <Box
    component="span"
    sx={(theme) => {
      const paletteKey = resolveStatusChipColor(status);
      const palette = theme.palette;
      const background =
        paletteKey === 'default'
          ? palette.action.hover
          : palette[paletteKey]?.light || palette.action.hover;
      const color =
        paletteKey === 'default'
          ? palette.text.primary
          : palette[paletteKey]?.main || palette.text.primary;
      return {
        display: 'inline-flex',
        alignItems: 'center',
        padding: theme.spacing(0.25, 1),
        borderRadius: 999,
        fontSize: '0.75rem',
        fontWeight: 600,
        textTransform: 'capitalize',
        backgroundColor: background,
        color,
        width: 'fit-content',
      };
    }}
  >
    {formatStatusLabel(status)}
  </Box>
);

const LIMIT_OPTIONS = [25, 50, 100];

type ScheduleFrequency = 'hourly' | 'daily' | 'weekly' | 'monthly' | 'custom';

interface ScheduleFormState {
  tableGroupId: string;
  timezone: string;
  isActive: boolean;
}

interface CronBuilderState {
  frequency: ScheduleFrequency;
  minute: string;
  hour: string;
  dayOfWeek: string;
  dayOfMonth: string;
  customExpression: string;
}

const DEFAULT_SCHEDULE_FORM: ScheduleFormState = {
  tableGroupId: '',
  timezone: 'UTC',
  isActive: true
};

const DEFAULT_CRON_BUILDER: CronBuilderState = {
  frequency: 'daily',
  minute: '0',
  hour: '2',
  dayOfWeek: 'MON',
  dayOfMonth: '1',
  customExpression: '0 2 * * *'
};

interface ScheduleFilters {
  tableGroup: string;
  connection: string;
  timezone: string;
  status: string;
}

const DEFAULT_SCHEDULE_FILTERS: ScheduleFilters = {
  tableGroup: '',
  connection: '',
  timezone: '',
  status: ''
};

interface MonitoredRun {
  tableGroupId: string;
  profileRunId: string;
  status?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  tableGroupLabel?: string | null;
}

const DataQualityProfilingRunsPage = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();

  const [selectedGroupId, setSelectedGroupId] = useState<string>('all');
  const [statusFilter, setStatusFilter] = useState<ProfileStatusFilter>('all');
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [limit, setLimit] = useState<number>(50);
  const [runModalOpen, setRunModalOpen] = useState(false);
  const [runModalGroupId, setRunModalGroupId] = useState('');
  const [runModalError, setRunModalError] = useState<string | null>(null);
  const [monitoredRuns, setMonitoredRuns] = useState<MonitoredRun[]>([]);
  const [scheduleModalOpen, setScheduleModalOpen] = useState(false);
  const [scheduleForm, setScheduleForm] = useState<ScheduleFormState>(DEFAULT_SCHEDULE_FORM);
  const [cronBuilder, setCronBuilder] = useState<CronBuilderState>(DEFAULT_CRON_BUILDER);
  const [scheduleFormError, setScheduleFormError] = useState<string | null>(null);
  const [scheduleFilters, setScheduleFilters] = useState<ScheduleFilters>(DEFAULT_SCHEDULE_FILTERS);
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null);
  const [selectedRunIds, setSelectedRunIds] = useState<string[]>([]);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteRunsError, setDeleteRunsError] = useState<string | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const paramGroupId = params.get('tableGroupId') || 'all';
    setSelectedGroupId((current) => (current === paramGroupId ? current : paramGroupId));
  }, [location.search]);

  const updateSelectedGroupParam = useCallback(
    (groupId: string) => {
      const params = new URLSearchParams(location.search);
      if (!groupId || groupId === 'all') {
        params.delete('tableGroupId');
      } else {
        params.set('tableGroupId', groupId);
      }
      navigate(
        {
          pathname: location.pathname,
          search: params.toString() ? `?${params.toString()}` : ''
        },
        { replace: true }
      );
    },
    [location.pathname, location.search, navigate]
  );

  const handleNavigateToRunResults = useCallback(
    (run: DataQualityProfileRunEntry, options?: { label?: string | null }) => {
      if (!run.tableGroupId) {
        return;
      }
      navigate(
        buildProfilingRunResultsPath(run.profileRunId, run.tableGroupId, {
          tableGroupLabel: options?.label
        })
      );
    },
    [navigate]
  );

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

  const profilingSchedulesQuery = useQuery<DataQualityProfilingSchedule[]>(
    ['data-quality', 'profiling-schedules'],
    fetchProfilingSchedules,
    {
      staleTime: 60 * 1000,
      onError: (error) => {
        showError(`Unable to load profiling schedules: ${error instanceof Error ? error.message : 'Unknown error.'}`);
      }
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

  const tableGroupLookup = useMemo(() => {
    const lookup = new Map<string, DataQualityProfileRunTableGroup>();
    tableGroups.forEach((group) => lookup.set(group.tableGroupId, group));
    return lookup;
  }, [tableGroups]);

  const profilingSchedules = useMemo(
    () => profilingSchedulesQuery.data ?? [],
    [profilingSchedulesQuery.data]
  );

  const filteredSchedules = useMemo(
    () => filterSchedules(profilingSchedules, scheduleFilters),
    [profilingSchedules, scheduleFilters]
  );

  useEffect(() => {
    if (selectedGroupId === 'all') {
      return;
    }
    const exists = tableGroups.some((group) => group.tableGroupId === selectedGroupId);
    if (!exists) {
      setSelectedGroupId('all');
      updateSelectedGroupParam('all');
    }
  }, [selectedGroupId, tableGroups, updateSelectedGroupParam]);

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

  useEffect(() => {
    const visibleIds = new Set(filteredRuns.map((run) => run.profileRunId));
    setSelectedRunIds((previous) => previous.filter((runId) => visibleIds.has(runId)));
  }, [filteredRuns]);

  const selectedRuns = useMemo(() => {
    const lookup = new Map(filteredRuns.map((run) => [run.profileRunId, run]));
    return selectedRunIds
      .map((runId) => lookup.get(runId))
      .filter((run): run is DataQualityProfileRunEntry => Boolean(run));
  }, [filteredRuns, selectedRunIds]);

  const selectionCount = selectedRunIds.length;
  const allVisibleSelected = filteredRuns.length > 0 && selectionCount === filteredRuns.length;
  const hasSelection = selectionCount > 0;
  const selectionHelperText = hasSelection
    ? `${selectionCount} run${selectionCount === 1 ? '' : 's'} selected`
    : 'Select runs to enable bulk actions.';
  const previewSelectedRuns = selectedRuns.slice(0, 5);
  const remainingSelections = Math.max(selectedRuns.length - previewSelectedRuns.length, 0);

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

  const monitorQueries = useQueries(
    monitoredRuns.map((run) => ({
      queryKey: ['data-quality', 'profiling-runs', 'monitor', run.tableGroupId],
      queryFn: () =>
        fetchDataQualityProfileRuns({
          tableGroupId: run.tableGroupId,
          limit: 5,
          includeGroups: true
        }),
      enabled: Boolean(monitoredRuns.length),
      refetchInterval: (data?: DataQualityProfileRunListResponse) => {
        const match = data?.runs.find((entry) => entry.profileRunId === run.profileRunId);
        return match && isTerminalStatus(match.status) ? false : 5000;
      }
    }))
  ) as UseQueryResult<DataQualityProfileRunListResponse>[];

  const monitoredRunStatuses = useMemo(() => {
    return monitoredRuns.map((run, idx) => {
      const query = monitorQueries[idx];
      const entry = query?.data?.runs.find((candidate) => candidate.profileRunId === run.profileRunId);
      const fallbackGroup = (entry?.tableGroupId && tableGroupLookup.get(entry.tableGroupId)) ||
        tableGroupLookup.get(run.tableGroupId);
      const label =
        entry?.tableGroupName ??
        run.tableGroupLabel ??
        (fallbackGroup ? formatTableGroupLabel(fallbackGroup) : run.tableGroupId);
      return {
        ...run,
        status: entry?.status ?? run.status ?? null,
        startedAt: entry?.startedAt ?? run.startedAt ?? null,
        completedAt: entry?.completedAt ?? run.completedAt ?? null,
        tableGroupLabel: label,
        isLoading: query?.isFetching ?? query?.isLoading ?? false
      };
    });
  }, [monitorQueries, monitoredRuns, tableGroupLookup]);

  const monitorError = monitorQueries.find((query) => query?.isError);
  const monitorErrorMessage = monitorError
    ? monitorError.error instanceof Error
      ? monitorError.error.message
      : 'Unable to refresh profiling run status.'
    : null;
  const hasPendingMonitoredRuns = monitoredRunStatuses.some((run) => !isTerminalStatus(run.status));
  const monitorIsFetching = monitorQueries.some((query) => query?.isFetching);

  const createScheduleMutation = useMutation(
    (input: DataQualityProfilingScheduleInput) => createProfilingSchedule(input),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(['data-quality', 'profiling-schedules']);
        showSuccess('Profiling schedule created.');
        setScheduleForm(DEFAULT_SCHEDULE_FORM);
        setCronBuilder(DEFAULT_CRON_BUILDER);
        setScheduleFormError(null);
      },
      onError: (error: unknown) => {
        const message = error instanceof Error ? error.message : 'Unexpected error creating schedule.';
        setScheduleFormError(message);
      }
    }
  );

  const deleteScheduleMutation = useMutation((scheduleId: string) => deleteProfilingSchedule(scheduleId), {
    onMutate: (scheduleId: string) => {
      setPendingDeleteId(scheduleId);
    },
    onSuccess: () => {
      queryClient.invalidateQueries(['data-quality', 'profiling-schedules']);
      showSuccess('Profiling schedule removed.');
    },
    onError: (error) => {
      showError(`Unable to delete profiling schedule: ${error instanceof Error ? error.message : 'Unknown error.'}`);
    },
    onSettled: () => {
      setPendingDeleteId(null);
    }
  });

  const mutation = useMutation(
    ({ tableGroupId }: { tableGroupId: string }) => startProfileRun(tableGroupId),
    {
      onSuccess: (result, variables) => {
        queryClient.invalidateQueries(['data-quality', 'profiling-runs']);
        setRunModalOpen(false);
        setRunModalGroupId('');
        setRunModalError(null);
        if (result?.profileRunId) {
          setMonitoredRuns((previous) => {
            const dedup = new Map(previous.map((run) => [run.profileRunId, run]));
            const tableGroup = tableGroupLookup.get(variables.tableGroupId);
            dedup.set(result.profileRunId, {
              tableGroupId: variables.tableGroupId,
              profileRunId: result.profileRunId,
              status: 'queued',
              tableGroupLabel: tableGroup ? formatTableGroupLabel(tableGroup) : variables.tableGroupId
            });
            const entries = Array.from(dedup.values());
            return entries.length > 10 ? entries.slice(entries.length - 10) : entries;
          });
        }
        showSuccess('Profiling run started.');
      },
      onError: (error: unknown) => {
        const message = error instanceof Error ? error.message : 'Unexpected error starting run.';
        setRunModalError(message);
        showError(message);
      }
    }
  );

  const deleteRunsMutation = useMutation((runIds: string[]) => deleteProfileRuns(runIds), {
    onMutate: () => {
      setDeleteRunsError(null);
    },
    onSuccess: (response, runIds) => {
      queryClient.invalidateQueries(['data-quality', 'profiling-runs']);
      setDeleteDialogOpen(false);
      setSelectedRunIds((previous) => previous.filter((id) => !runIds.includes(id)));
      setDeleteRunsError(null);
      const deleted = response?.deletedCount ?? runIds.length;
      const label = deleted === 1 ? 'profiling run' : 'profiling runs';
      showSuccess(`Deleted ${deleted} ${label}.`);
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unexpected error deleting profiling runs.';
      setDeleteRunsError(message);
      showError(message);
    }
  });

  useEffect(() => {
    if (!selectedRunIds.length && deleteDialogOpen && !deleteRunsMutation.isLoading) {
      setDeleteDialogOpen(false);
    }
  }, [deleteDialogOpen, deleteRunsMutation.isLoading, selectedRunIds.length]);

  const handleGroupChange = (event: SelectChangeEvent<string>) => {
    const value = event.target.value;
    setSelectedGroupId(value);
    updateSelectedGroupParam(value);
  };

  const handleStatusChange = (event: SelectChangeEvent<string>) => {
    setStatusFilter((event.target.value as ProfileStatusFilter | null) ?? 'all');
  };

  const handleLimitChange = (event: SelectChangeEvent<string>) => {
    const parsed = Number(event.target.value);
    setLimit(Number.isFinite(parsed) ? parsed : 50);
  };

  const handleOpenRunModal = () => {
    setRunModalGroupId(selectedGroupId !== 'all' ? selectedGroupId : '');
    setRunModalError(null);
    setRunModalOpen(true);
  };

  const handleCloseRunModal = () => {
    if (mutation.isLoading) {
      return;
    }
    setRunModalOpen(false);
    setRunModalError(null);
  };

  const handleRunModalGroupChange = (event: SelectChangeEvent<string>) => {
    setRunModalGroupId(event.target.value);
    setRunModalError(null);
  };

  const handleRunModalSubmit = () => {
    const trimmedGroup = runModalGroupId.trim();
    if (!trimmedGroup) {
      setRunModalError('Select the table group you want to profile now.');
      return;
    }
    mutation.mutate({ tableGroupId: trimmedGroup });
  };

  const handleClearMonitoredRuns = useCallback(() => {
    setMonitoredRuns([]);
  }, []);

  const handleFocusMonitoredGroup = useCallback(
    (groupId: string) => {
      setSelectedGroupId(groupId);
      updateSelectedGroupParam(groupId);
    },
    [updateSelectedGroupParam]
  );

  const handleOpenScheduleModal = useCallback(
    (prefillGroupId?: string | null) => {
      setScheduleForm((prev) => ({
        ...prev,
        tableGroupId: prefillGroupId && prefillGroupId !== 'all' ? prefillGroupId : prev.tableGroupId
      }));
      setScheduleFormError(null);
      setScheduleModalOpen(true);
    },
    []
  );

  const handleCloseScheduleModal = () => {
    if (createScheduleMutation.isLoading) {
      return;
    }
    setScheduleModalOpen(false);
    setScheduleFormError(null);
  };

  const handleScheduleGroupChange = (event: SelectChangeEvent<string>) => {
    setScheduleForm((prev) => ({
      ...prev,
      tableGroupId: event.target.value
    }));
  };

  const handleScheduleTimezoneChange = (event: ChangeEvent<HTMLInputElement>) => {
    setScheduleForm((prev) => ({
      ...prev,
      timezone: event.target.value
    }));
  };

  const handleScheduleActiveToggle = (event: ChangeEvent<HTMLInputElement>) => {
    setScheduleForm((prev) => ({
      ...prev,
      isActive: event.target.checked
    }));
  };

  const handleFrequencyChange = (event: SelectChangeEvent<ScheduleFrequency>) => {
    const frequency = event.target.value as ScheduleFrequency;
    setCronBuilder((prev) => ({
      ...prev,
      frequency
    }));
  };

  const handleCronSelectChange = (
    field: keyof Pick<CronBuilderState, 'minute' | 'hour' | 'dayOfWeek' | 'dayOfMonth'>
  ) =>
    (event: SelectChangeEvent<string>) => {
      const value = event.target.value;
      setCronBuilder((prev) => ({
        ...prev,
        [field]: value
      }));
    };

  const handleCronCustomExpressionChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setCronBuilder((prev) => ({
      ...prev,
      customExpression: value
    }));
  };

  const handleScheduleSubmit = () => {
    const trimmedGroup = scheduleForm.tableGroupId.trim();
    if (!trimmedGroup) {
      setScheduleFormError('Select a table group to schedule.');
      return;
    }
    if (cronBuilder.frequency === 'custom' && !cronBuilder.customExpression.trim()) {
      setScheduleFormError('Provide a cron expression for the custom schedule.');
      return;
    }
    setScheduleFormError(null);
    const expression = buildCronExpression(cronBuilder);
    const input: DataQualityProfilingScheduleInput = {
      tableGroupId: trimmedGroup,
      scheduleExpression: expression,
      timezone: scheduleForm.timezone.trim() || undefined,
      isActive: scheduleForm.isActive
    };
    createScheduleMutation.mutate(input);
  };

  const handleScheduleFilterChange = (field: keyof ScheduleFilters) =>
    (event: ChangeEvent<HTMLInputElement>) => {
      const value = event.target.value;
      setScheduleFilters((prev) => ({
        ...prev,
        [field]: value
      }));
    };

  const handleDeleteSchedule = (scheduleId: string) => {
    if (!window.confirm('Delete this profiling schedule? Future runs will stop.')) {
      return;
    }
    deleteScheduleMutation.mutate(scheduleId);
  };

  const handleToggleRunSelection = (runId: string) => (
    _event: ChangeEvent<HTMLInputElement>,
    checked: boolean
  ) => {
    setSelectedRunIds((prev) => {
      if (checked) {
        return prev.includes(runId) ? prev : [...prev, runId];
      }
      return prev.filter((id) => id !== runId);
    });
  };

  const handleToggleSelectAll = (
    _event: ChangeEvent<HTMLInputElement>,
    checked: boolean
  ) => {
    if (checked) {
      setSelectedRunIds(filteredRuns.map((run) => run.profileRunId));
    } else {
      setSelectedRunIds([]);
    }
  };

  const handleOpenDeleteDialog = () => {
    if (!selectedRunIds.length) {
      return;
    }
    setDeleteRunsError(null);
    setDeleteDialogOpen(true);
  };

  const handleCloseDeleteDialog = () => {
    if (deleteRunsMutation.isLoading) {
      return;
    }
    setDeleteDialogOpen(false);
    setDeleteRunsError(null);
  };

  const handleConfirmDeleteRuns = () => {
    if (!selectedRunIds.length || deleteRunsMutation.isLoading) {
      return;
    }
    deleteRunsMutation.mutate([...selectedRunIds]);
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
              variant="outlined"
              color="secondary"
              startIcon={<AddCircleOutlineIcon />}
              onClick={() => handleOpenScheduleModal(selectedGroupId)}
            >
              Profiling schedules
            </Button>
            <Button
              variant="contained"
              onClick={handleOpenRunModal}
              disabled={mutation.isLoading}
            >
              {mutation.isLoading ? 'Starting…' : 'Run Profiling'}
            </Button>
          </>
        }
      />

      {monitoredRunStatuses.length > 0 ? (
        <Paper variant="outlined" sx={{ p: 2, mt: 3 }}>
          <Stack spacing={2}>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ md: 'center' }}>
              <Typography fontWeight={600}>Recent profiling requests</Typography>
              {hasPendingMonitoredRuns ? (
                <Stack direction="row" spacing={1} alignItems="center">
                  <CircularProgress size={16} />
                  <Typography variant="body2" color="text.secondary">
                    Background jobs are launching. We refresh every few seconds.
                  </Typography>
                </Stack>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  All requested runs have been dispatched.
                </Typography>
              )}
              <Box flexGrow={1} />
              <Button
                size="small"
                onClick={handleClearMonitoredRuns}
                disabled={monitorIsFetching && hasPendingMonitoredRuns}
              >
                Clear
              </Button>
            </Stack>
            {monitorErrorMessage ? <Alert severity="warning">{monitorErrorMessage}</Alert> : null}
            <Stack spacing={1.5}>
              {monitoredRunStatuses.map((run) => (
                <Paper key={run.profileRunId} variant="outlined" sx={{ p: 1.5 }}>
                  <Stack
                    direction={{ xs: 'column', sm: 'row' }}
                    spacing={1.5}
                    alignItems={{ sm: 'center' }}
                    justifyContent="space-between"
                  >
                    <Stack spacing={0.25}>
                      <Typography fontWeight={600}>{run.tableGroupLabel ?? run.tableGroupId}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        Table group ID: {run.tableGroupId}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Run ID: {run.profileRunId}
                      </Typography>
                    </Stack>
                    <Stack direction="row" spacing={1} alignItems="center">
                      {run.isLoading ? <CircularProgress size={16} /> : null}
                      <Chip label={formatStatusLabel(run.status)} color={resolveStatusChipColor(run.status)} size="small" />
                    </Stack>
                    <Button variant="outlined" size="small" onClick={() => handleFocusMonitoredGroup(run.tableGroupId)}>
                      View runs
                    </Button>
                  </Stack>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                    {describeStatusTimestamps(run)}
                  </Typography>
                </Paper>
              ))}
            </Stack>
          </Stack>
        </Paper>
      ) : null}

      <Paper sx={{ p: 3, mt: 3 }}>
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
                  {formatTableGroupDropdownLabel(group)}
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
            <>
              <Stack
                direction={{ xs: 'column', md: 'row' }}
                spacing={1.5}
                alignItems={{ xs: 'flex-start', md: 'center' }}
                justifyContent="space-between"
                mb={1.5}
              >
                <Typography variant="body2" color="text.secondary">
                  {selectionHelperText}
                </Typography>
                <Button
                  color="error"
                  variant="outlined"
                  startIcon={<DeleteOutlineIcon />}
                  disabled={!hasSelection || deleteRunsMutation.isLoading}
                  onClick={handleOpenDeleteDialog}
                >
                  {deleteRunsMutation.isLoading ? 'Deleting…' : 'Delete selected'}
                </Button>
              </Stack>
              <Table size="small" sx={{ '& td, & th': { borderBottomColor: 'divider' } }}>
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox">
                      <Checkbox
                        color="primary"
                        indeterminate={hasSelection && !allVisibleSelected}
                        checked={allVisibleSelected}
                        onChange={handleToggleSelectAll}
                        inputProps={{ 'aria-label': 'Select all profiling runs' }}
                      />
                    </TableCell>
                    <TableCell>Table group · Start time</TableCell>
                    <TableCell>Status · Duration</TableCell>
                    <TableCell>Application</TableCell>
                    <TableCell>Hygiene issues</TableCell>
                    <TableCell align="right">Profiling score</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredRuns.map((run) => {
                    const isSelected = selectedRunIds.includes(run.profileRunId);
                    return (
                      <TableRow key={run.profileRunId} hover selected={isSelected}>
                        <TableCell padding="checkbox">
                          <Checkbox
                            color="primary"
                            checked={isSelected}
                            onChange={handleToggleRunSelection(run.profileRunId)}
                            inputProps={{ 'aria-label': 'Select profiling run' }}
                          />
                        </TableCell>
                        <TableCell>
                          <Stack spacing={0.25}>
                            <Typography variant="body2" fontWeight={600}>
                              {run.tableGroupName ?? run.tableGroupId}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              {formatDateTime(run.startedAt)}
                            </Typography>
                          </Stack>
                        </TableCell>
                        <TableCell>
                          <Stack spacing={0.5}>
                            {renderStatusBadge(run.status)}
                            <Typography variant="caption" color="text.secondary">
                              {formatDurationMs(run.durationMs)} ·
                              {typeof run.rowCount === 'number'
                                ? ` ${run.rowCount.toLocaleString()} rows`
                                : ' Row count pending'}
                            </Typography>
                          </Stack>
                        </TableCell>
                        <TableCell>
                          {(() => {
                            const tableGroup = tableGroupLookup.get(run.tableGroupId);
                            const description =
                              tableGroup?.applicationDescription ??
                              run.applicationDescription ??
                              tableGroup?.applicationName ??
                              run.applicationName ??
                              '—';
                            const tableCount = run.tableCount ?? tableGroup?.tableCount ?? null;
                            const fieldCount = run.fieldCount ?? tableGroup?.fieldCount ?? null;
                            const canViewResults = Boolean(run.tableGroupId) && isTerminalStatus(run.status);
                            const tableGroupLabel = tableGroup
                              ? formatTableGroupDropdownLabel(tableGroup)
                              : formatTableGroupDropdownLabel({
                                  tableGroupId: run.tableGroupId,
                                  tableGroupName: run.tableGroupName,
                                  connectionId: run.connectionId,
                                  connectionName: run.connectionName,
                                  catalog: run.catalog,
                                  schemaName: run.schemaName,
                                  dataObjectId: run.dataObjectId,
                                  dataObjectName: run.dataObjectName,
                                  applicationId: run.applicationId,
                                  applicationName: run.applicationName,
                                  applicationDescription: run.applicationDescription,
                                  productTeamId: run.productTeamId,
                                  productTeamName: run.productTeamName,
                                  tableCount: run.tableCount,
                                  fieldCount: run.fieldCount
                                } as DataQualityProfileRunTableGroup);
                            return (
                              <Stack spacing={0.5}>
                                <Typography variant="body2" fontWeight={600}>
                                  {description}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                  {formatTableFieldSummary(tableCount, fieldCount)}
                                </Typography>
                                <Button
                                  size="small"
                                  variant="text"
                                  sx={{ px: 0, alignSelf: 'flex-start' }}
                                  onClick={() => handleNavigateToRunResults(run, { label: tableGroupLabel })}
                                  disabled={!canViewResults}
                                >
                                  View results
                                </Button>
                              </Stack>
                            );
                          })()}
                        </TableCell>
                        <TableCell>
                          {(() => {
                            const summaries = summarizeHygieneIssues(run.anomaliesBySeverity);
                            const canViewAnomalies = Boolean(run.tableGroupId) && isTerminalStatus(run.status);
                            return (
                              <Stack spacing={0.5} alignItems="flex-start">
                                {summaries.length ? (
                                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                                    {summaries.map((summary) => (
                                      <Stack key={summary.label} spacing={0} alignItems="flex-start">
                                        <Typography variant="caption" color="text.secondary">
                                          {summary.label}
                                        </Typography>
                                        <Chip
                                          label={summary.count}
                                          size="small"
                                          color={summary.color}
                                          variant="outlined"
                                        />
                                      </Stack>
                                    ))}
                                  </Stack>
                                ) : (
                                  <Typography variant="body2">—</Typography>
                                )}
                                <Button
                                  size="small"
                                  variant="text"
                                  sx={{ px: 0 }}
                                  onClick={() => handleOpenAnomalies(run.profileRunId)}
                                  disabled={!canViewAnomalies}
                                >
                                  View anomalies
                                </Button>
                              </Stack>
                            );
                          })()}
                        </TableCell>
                        <TableCell align="right">
                          <Stack spacing={0} alignItems="flex-end">
                            <Typography variant="body2" fontWeight={600}>
                              {typeof run.profilingScore === 'number' ? run.profilingScore.toFixed(1) : '—'}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              Profiling score
                            </Typography>
                          </Stack>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </>
          )}
        </Box>
      </Paper>

      <Dialog open={runModalOpen} onClose={handleCloseRunModal} fullWidth maxWidth="sm">
        <DialogTitle>Run Profiling</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography variant="body2" color="text.secondary">
              Trigger a profiling run immediately for any table group. We will show confirmation and status updates here once the job is queued.
            </Typography>
            {runModalError ? <Alert severity="warning">{runModalError}</Alert> : null}
            <FormControl fullWidth size="small">
              <InputLabel id="run-modal-table-group-label">Table group</InputLabel>
              <Select
                labelId="run-modal-table-group-label"
                label="Table group"
                value={runModalGroupId}
                onChange={handleRunModalGroupChange}
              >
                <MenuItem value="">
                  <em>Select a table group</em>
                </MenuItem>
                {tableGroups.map((group) => (
                  <MenuItem key={group.tableGroupId} value={group.tableGroupId}>
                    {formatTableGroupDropdownLabel(group)}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseRunModal} disabled={mutation.isLoading}>
            Cancel
          </Button>
          <Button variant="contained" onClick={handleRunModalSubmit} disabled={mutation.isLoading}>
            {mutation.isLoading ? 'Starting…' : 'Run profiling'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteDialogOpen} onClose={handleCloseDeleteDialog} fullWidth maxWidth="sm">
        <DialogTitle>Delete profiling runs</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Alert severity="warning">
              Removing profiling runs deletes their anomalies, column metrics, and run history from your Databricks data quality schema. This action cannot be undone.
            </Alert>
            <Typography variant="body2">
              {selectionCount === 1
                ? 'You are about to permanently delete this profiling run.'
                : `You are about to permanently delete ${selectionCount} profiling runs.`}
            </Typography>
            <Stack spacing={0.5}>
              {previewSelectedRuns.map((run) => (
                <Typography key={run.profileRunId} variant="body2">
                  • {run.tableGroupName ?? run.tableGroupId} · {formatDateTime(run.startedAt)}
                </Typography>
              ))}
              {remainingSelections > 0 ? (
                <Typography variant="caption" color="text.secondary">
                  +{remainingSelections} more…
                </Typography>
              ) : null}
            </Stack>
            {deleteRunsError ? <Alert severity="error">{deleteRunsError}</Alert> : null}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDeleteDialog} disabled={deleteRunsMutation.isLoading}>
            Cancel
          </Button>
          <Button
            color="error"
            variant="contained"
            onClick={handleConfirmDeleteRuns}
            disabled={deleteRunsMutation.isLoading}
          >
            {deleteRunsMutation.isLoading ? 'Deleting…' : 'Delete runs'}
          </Button>
        </DialogActions>
      </Dialog>

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

      <Dialog open={scheduleModalOpen} onClose={handleCloseScheduleModal} fullWidth maxWidth="lg">
        <DialogTitle>Manage profiling schedules</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={3} mt={0.5}>
            <Stack spacing={2}>
              {scheduleFormError ? <Alert severity="warning">{scheduleFormError}</Alert> : null}
              <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
                <FormControl fullWidth size="small">
                  <InputLabel id="schedule-table-group-label">Table group</InputLabel>
                  <Select
                    labelId="schedule-table-group-label"
                    label="Table group"
                    value={scheduleForm.tableGroupId}
                    onChange={handleScheduleGroupChange}
                  >
                    <MenuItem value="">
                      <em>Select table group</em>
                    </MenuItem>
                    {tableGroups.map((group) => (
                      <MenuItem key={group.tableGroupId} value={group.tableGroupId}>
                        {formatTableGroupDropdownLabel(group)}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <TextField
                  label="Timezone"
                  value={scheduleForm.timezone}
                  onChange={handleScheduleTimezoneChange}
                  helperText="IANA timezone, e.g. UTC or America/New_York"
                  size="small"
                  fullWidth
                />
                <FormControlLabel
                  control={<Switch checked={scheduleForm.isActive} onChange={handleScheduleActiveToggle} />}
                  label="Schedule active"
                />
              </Stack>
              <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ md: 'flex-end' }}>
                <FormControl fullWidth size="small">
                  <InputLabel id="schedule-frequency-label">Frequency</InputLabel>
                  <Select
                    labelId="schedule-frequency-label"
                    label="Frequency"
                    value={cronBuilder.frequency}
                    onChange={handleFrequencyChange}
                  >
                    <MenuItem value="hourly">Hourly</MenuItem>
                    <MenuItem value="daily">Daily</MenuItem>
                    <MenuItem value="weekly">Weekly</MenuItem>
                    <MenuItem value="monthly">Monthly</MenuItem>
                    <MenuItem value="custom">Custom cron</MenuItem>
                  </Select>
                </FormControl>
                {cronBuilder.frequency === 'hourly' ? (
                  <FormControl fullWidth size="small">
                    <InputLabel id="cron-minute-label">Minute</InputLabel>
                    <Select
                      labelId="cron-minute-label"
                      label="Minute"
                      value={cronBuilder.minute}
                      onChange={handleCronSelectChange('minute')}
                    >
                      {MINUTE_OPTIONS.map((minute) => (
                        <MenuItem key={minute} value={minute}>
                          :{minute.padStart(2, '0')}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : null}
                {cronBuilder.frequency === 'daily' || cronBuilder.frequency === 'weekly' || cronBuilder.frequency === 'monthly' ? (
                  <FormControl fullWidth size="small">
                    <InputLabel id="cron-hour-label">Hour (24h)</InputLabel>
                    <Select
                      labelId="cron-hour-label"
                      label="Hour"
                      value={cronBuilder.hour}
                      onChange={handleCronSelectChange('hour')}
                    >
                      {HOUR_OPTIONS.map((hour) => (
                        <MenuItem key={hour} value={hour}>
                          {hour.padStart(2, '0')}:00
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : null}
                {cronBuilder.frequency !== 'hourly' && cronBuilder.frequency !== 'custom' ? (
                  <FormControl fullWidth size="small">
                    <InputLabel id="cron-minute-generic-label">Minute</InputLabel>
                    <Select
                      labelId="cron-minute-generic-label"
                      label="Minute"
                      value={cronBuilder.minute}
                      onChange={handleCronSelectChange('minute')}
                    >
                      {MINUTE_OPTIONS.map((minute) => (
                        <MenuItem key={minute} value={minute}>
                          :{minute.padStart(2, '0')}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : null}
                {cronBuilder.frequency === 'weekly' ? (
                  <FormControl fullWidth size="small">
                    <InputLabel id="cron-dow-label">Day of week</InputLabel>
                    <Select
                      labelId="cron-dow-label"
                      label="Day of week"
                      value={cronBuilder.dayOfWeek}
                      onChange={handleCronSelectChange('dayOfWeek')}
                    >
                      {DAY_OF_WEEK_OPTIONS.map((dow) => (
                        <MenuItem key={dow} value={dow}>
                          {dow}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : null}
                {cronBuilder.frequency === 'monthly' ? (
                  <FormControl fullWidth size="small">
                    <InputLabel id="cron-dom-label">Day of month</InputLabel>
                    <Select
                      labelId="cron-dom-label"
                      label="Day of month"
                      value={cronBuilder.dayOfMonth}
                      onChange={handleCronSelectChange('dayOfMonth')}
                    >
                      {DAY_OF_MONTH_OPTIONS.map((dom) => (
                        <MenuItem key={dom} value={dom}>
                          {dom}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : null}
              </Stack>
              {cronBuilder.frequency === 'custom' ? (
                <TextField
                  label="Cron expression"
                  value={cronBuilder.customExpression}
                  onChange={handleCronCustomExpressionChange}
                  fullWidth
                  size="small"
                  helperText="Five-field cron expression (minute hour day-of-month month day-of-week)"
                />
              ) : null}
              <Alert severity="info">
                Cron preview: <strong>{buildCronExpression(cronBuilder)}</strong>
              </Alert>
            </Stack>

            <Box>
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ sm: 'flex-start' }} mb={1.5}>
                <Typography variant="subtitle1" fontWeight={600} flex={1}>
                  Existing schedules
                </Typography>
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={<AddCircleOutlineIcon fontSize="small" />}
                  onClick={() => {
                    setScheduleForm(DEFAULT_SCHEDULE_FORM);
                    setCronBuilder(DEFAULT_CRON_BUILDER);
                  }}
                >
                  Reset form
                </Button>
                <Button
                  size="small"
                  variant="outlined"
                  onClick={() => profilingSchedulesQuery.refetch()}
                  disabled={profilingSchedulesQuery.isFetching}
                >
                  Refresh list
                </Button>
              </Stack>
              {profilingSchedulesQuery.isLoading ? (
                <Box display="flex" justifyContent="center" py={3}>
                  <CircularProgress size={24} />
                </Box>
              ) : profilingSchedules.length === 0 ? (
                <Alert severity="info">No profiling schedules configured yet.</Alert>
              ) : (
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Table group</TableCell>
                      <TableCell>Schedule</TableCell>
                      <TableCell>Timezone</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Last run</TableCell>
                      <TableCell align="right">Actions</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>
                        <TextField
                          placeholder="Filter"
                          size="small"
                          value={scheduleFilters.tableGroup}
                          onChange={handleScheduleFilterChange('tableGroup')}
                          fullWidth
                        />
                      </TableCell>
                      <TableCell>
                        <TextField
                          placeholder="Filter"
                          size="small"
                          value={scheduleFilters.connection}
                          onChange={handleScheduleFilterChange('connection')}
                          fullWidth
                        />
                      </TableCell>
                      <TableCell>
                        <TextField
                          placeholder="Filter"
                          size="small"
                          value={scheduleFilters.timezone}
                          onChange={handleScheduleFilterChange('timezone')}
                          fullWidth
                        />
                      </TableCell>
                      <TableCell>
                        <TextField
                          placeholder="Filter"
                          size="small"
                          value={scheduleFilters.status}
                          onChange={handleScheduleFilterChange('status')}
                          fullWidth
                        />
                      </TableCell>
                      <TableCell colSpan={2}>
                        <Typography variant="caption" color="text.secondary">
                          Filters apply instantly
                        </Typography>
                      </TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredSchedules.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={6}>
                          <Typography color="text.secondary">No schedules match the current filters.</Typography>
                        </TableCell>
                      </TableRow>
                    ) : (
                      filteredSchedules.map((schedule) => (
                        <TableRow key={schedule.profilingScheduleId} hover>
                          <TableCell>
                            <Typography fontWeight={600}>{schedule.tableGroupName ?? schedule.tableGroupId}</Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Table group ID: {schedule.tableGroupId}
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              {schedule.connectionName ?? 'Unknown connection'}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <Typography fontFamily="monospace">{schedule.scheduleExpression}</Typography>
                            <Typography variant="caption" color="text.secondary">
                              Total runs: {schedule.totalRuns.toLocaleString()}
                            </Typography>
                          </TableCell>
                          <TableCell>{schedule.timezone ?? 'UTC'}</TableCell>
                          <TableCell>
                            <Chip
                              label={schedule.isActive ? schedule.lastRunStatus ?? 'Queued' : 'Paused'}
                              size="small"
                              color={schedule.isActive ? resolveStatusChipColor(schedule.lastRunStatus ?? undefined) : 'default'}
                            />
                          </TableCell>
                          <TableCell>
                            <Typography variant="body2" color="text.secondary">
                              {describeScheduleLastRun(schedule)}
                            </Typography>
                          </TableCell>
                          <TableCell align="right">
                            <Tooltip title="Delete schedule">
                              <span>
                                <IconButton
                                  size="small"
                                  color="error"
                                  onClick={() => handleDeleteSchedule(schedule.profilingScheduleId)}
                                  disabled={pendingDeleteId === schedule.profilingScheduleId}
                                >
                                  <DeleteOutlineIcon fontSize="small" />
                                </IconButton>
                              </span>
                            </Tooltip>
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              )}
            </Box>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseScheduleModal} disabled={createScheduleMutation.isLoading}>
            Close
          </Button>
          <Button
            variant="contained"
            onClick={handleScheduleSubmit}
            disabled={createScheduleMutation.isLoading}
            startIcon={createScheduleMutation.isLoading ? <CircularProgress size={16} color="inherit" /> : undefined}
          >
            {createScheduleMutation.isLoading ? 'Saving…' : 'Save schedule'}
          </Button>
        </DialogActions>
      </Dialog>

      {snackbar}
    </Box>
  );
};

export default DataQualityProfilingRunsPage;
