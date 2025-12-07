import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  FormControlLabel,
  Snackbar,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import { LoadingButton } from '@mui/lab';
import { useQuery } from 'react-query';

import PageHeader from '@components/common/PageHeader';
import {
  fetchTableObservabilitySchedules,
  triggerTableObservabilitySchedule,
  updateTableObservabilitySchedule
} from '@services/tableObservabilityService';
import type { TableObservabilitySchedule } from '@cc-types/data';
import { formatDateTime } from '@pages/DataQualityProfileRunResultsPage';

interface ScheduleFormState {
  cronExpression: string;
  timezone: string;
  isActive: boolean;
}

type ScheduleStateMap = Record<string, ScheduleFormState>;

type SnackbarState = {
  message: string;
  severity: 'success' | 'error';
} | null;

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (!error) return fallback;
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (typeof error === 'object') {
    const detail = (error as { detail?: unknown }).detail;
    if (typeof detail === 'string') {
      return detail;
    }
  }
  return fallback;
};

const describeLastRun = (schedule: TableObservabilitySchedule): string => {
  if (!schedule.lastRunStatus && !schedule.lastRunStartedAt) {
    return 'This category has not been executed yet.';
  }
  const status = schedule.lastRunStatus ? schedule.lastRunStatus.replace(/_/g, ' ') : 'scheduled';
  const parts: string[] = [`Last run ${status}.`];
  if (schedule.lastRunStartedAt) {
    parts.push(`Started ${formatDateTime(schedule.lastRunStartedAt)}.`);
  }
  if (schedule.lastRunCompletedAt) {
    parts.push(`Completed ${formatDateTime(schedule.lastRunCompletedAt)}.`);
  }
  if (!schedule.lastRunCompletedAt && schedule.lastRunStartedAt && schedule.lastRunStatus === 'running') {
    parts.push('Currently in progress.');
  }
  return parts.join(' ');
};

const TableObservabilitySettingsPage = () => {
  const {
    data: schedules = [],
    isLoading,
    isError,
    error,
    refetch
  } = useQuery(['tableObservability', 'schedules'], fetchTableObservabilitySchedules, {
    staleTime: 60 * 1000
  });

  const [formState, setFormState] = useState<ScheduleStateMap>({});
  const [savingMap, setSavingMap] = useState<Record<string, boolean>>({});
  const [triggeringMap, setTriggeringMap] = useState<Record<string, boolean>>({});
  const [snackbar, setSnackbar] = useState<SnackbarState>(null);

  useEffect(() => {
    const next: ScheduleStateMap = {};
    schedules.forEach((schedule) => {
      next[schedule.scheduleId] = {
        cronExpression: schedule.cronExpression,
        timezone: schedule.timezone,
        isActive: schedule.isActive
      };
    });
    setFormState(next);
  }, [schedules]);

  const pageError = useMemo(() => (
    isError ? getErrorMessage(error, 'Unable to load observability schedules.') : null
  ), [error, isError]);

  const handleFieldChange = useCallback(
    (scheduleId: string, field: keyof ScheduleFormState, value: string | boolean) => {
      setFormState((prev) => ({
        ...prev,
        [scheduleId]: {
          ...(prev[scheduleId] ?? { cronExpression: '', timezone: '', isActive: true }),
          [field]: value
        }
      }));
    },
    []
  );

  const hasChanges = useCallback(
    (schedule: TableObservabilitySchedule) => {
      const values = formState[schedule.scheduleId];
      if (!values) {
        return false;
      }
      return (
        values.cronExpression !== schedule.cronExpression ||
        values.timezone !== schedule.timezone ||
        values.isActive !== schedule.isActive
      );
    },
    [formState]
  );

  const handleSave = useCallback(
    async (schedule: TableObservabilitySchedule) => {
      const values = formState[schedule.scheduleId];
      if (!values) return;
      setSavingMap((prev) => ({ ...prev, [schedule.scheduleId]: true }));
      try {
        await updateTableObservabilitySchedule(schedule.scheduleId, {
          cronExpression: values.cronExpression,
          timezone: values.timezone,
          isActive: values.isActive
        });
        await refetch();
        setSnackbar({ message: `${schedule.categoryName} updated.`, severity: 'success' });
      } catch (err) {
        setSnackbar({
          message: getErrorMessage(err, 'Unable to update schedule. Please try again.'),
          severity: 'error'
        });
      } finally {
        setSavingMap((prev) => ({ ...prev, [schedule.scheduleId]: false }));
      }
    },
    [formState, refetch]
  );

  const handleResetDefaults = useCallback((schedule: TableObservabilitySchedule) => {
    setFormState((prev) => ({
      ...prev,
      [schedule.scheduleId]: {
        ...(prev[schedule.scheduleId] ?? {
          cronExpression: schedule.cronExpression,
          timezone: schedule.timezone,
          isActive: schedule.isActive
        }),
        cronExpression: schedule.defaultCronExpression,
        timezone: schedule.defaultTimezone
      }
    }));
  }, []);

  const handleTrigger = useCallback(async (schedule: TableObservabilitySchedule) => {
    setTriggeringMap((prev) => ({ ...prev, [schedule.scheduleId]: true }));
    try {
      await triggerTableObservabilitySchedule(schedule.scheduleId);
      setSnackbar({ message: `${schedule.categoryName} run started.`, severity: 'success' });
      await refetch();
    } catch (err) {
      setSnackbar({
        message: getErrorMessage(err, 'Unable to trigger schedule.'),
        severity: 'error'
      });
    } finally {
      setTriggeringMap((prev) => ({ ...prev, [schedule.scheduleId]: false }));
    }
  }, [refetch]);

  const renderScheduleCard = (schedule: TableObservabilitySchedule) => {
    const values = formState[schedule.scheduleId] ?? {
      cronExpression: schedule.cronExpression,
      timezone: schedule.timezone,
      isActive: schedule.isActive
    };
    const saving = savingMap[schedule.scheduleId] ?? false;
    const triggering = triggeringMap[schedule.scheduleId] ?? false;
    const disableActions = saving || triggering;
    return (
      <Card key={schedule.scheduleId} elevation={0} sx={{ borderRadius: 3 }}>
        <CardContent>
          <Stack spacing={3}>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ xs: 'flex-start', md: 'center' }} justifyContent="space-between">
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {schedule.categoryName}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {schedule.rationale}
                </Typography>
              </Box>
              <Chip label={schedule.cadence} color="primary" variant="outlined" />
            </Stack>

            <FormControlLabel
              control={(
                <Switch
                  checked={values.isActive}
                  onChange={(event) => handleFieldChange(schedule.scheduleId, 'isActive', event.target.checked)}
                  disabled={disableActions}
                />
              )}
              label={values.isActive ? 'Schedule is active' : 'Schedule is paused'}
            />

            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
              <TextField
                fullWidth
                label="Cron expression"
                value={values.cronExpression}
                onChange={(event) => handleFieldChange(schedule.scheduleId, 'cronExpression', event.target.value)}
                helperText={`Default: ${schedule.defaultCronExpression}`}
                disabled={disableActions}
              />
              <TextField
                fullWidth
                label="Timezone"
                value={values.timezone}
                onChange={(event) => handleFieldChange(schedule.scheduleId, 'timezone', event.target.value)}
                helperText={`IANA identifier · Default: ${schedule.defaultTimezone}`}
                disabled={disableActions}
              />
            </Stack>

            <Typography variant="body2" color="text.secondary">
              {describeLastRun(schedule)}
            </Typography>

            {schedule.lastRunError && (
              <Alert severity="error">{schedule.lastRunError}</Alert>
            )}

            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
              <LoadingButton
                variant="outlined"
                onClick={() => handleResetDefaults(schedule)}
                disabled={disableActions}
              >
                Reset to defaults
              </LoadingButton>
              <LoadingButton
                variant="contained"
                onClick={() => handleSave(schedule)}
                loading={saving}
                disabled={!hasChanges(schedule) || disableActions}
              >
                Save changes
              </LoadingButton>
              <LoadingButton
                variant="contained"
                color="secondary"
                onClick={() => handleTrigger(schedule)}
                loading={triggering}
                disabled={!values.isActive || disableActions}
              >
                Run now
              </LoadingButton>
            </Stack>
          </Stack>
        </CardContent>
      </Card>
    );
  };

  return (
    <Box>
      <PageHeader
        title="Table Observability"
        subtitle="Control how Conversion Central captures table metrics across structural, schema, operational, and governance categories."
      />

      {pageError && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {pageError}
        </Alert>
      )}

      {isLoading ? (
        <Box display="flex" justifyContent="center" mt={6}>
          <CircularProgress />
        </Box>
      ) : (
        <Stack spacing={3}>
          {schedules.map((schedule) => renderScheduleCard(schedule))}
        </Stack>
      )}

      {!isLoading && schedules.length === 0 && !pageError && (
        <Alert severity="info" sx={{ mt: 3 }}>
          Table observability is not configured yet. Add JDBC table selections to generate schedules automatically.
        </Alert>
      )}

      {snackbar ? (
        <Snackbar
          open
          autoHideDuration={4000}
          onClose={() => setSnackbar(null)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        >
          <Alert onClose={() => setSnackbar(null)} severity={snackbar.severity} sx={{ width: '100%' }}>
            {snackbar.message}
          </Alert>
        </Snackbar>
      ) : null}
    </Box>
  );
};

export default TableObservabilitySettingsPage;
