import { MouseEvent, useCallback, useMemo, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { useMutation, useQueries, UseQueryResult, useQuery } from 'react-query';
import { Link as RouterLink } from 'react-router-dom';
import {
  fetchDataQualityProfileRuns,
  fetchDatasetHierarchy,
  startDataObjectProfileRuns
} from '@services/dataQualityService';
import {
  DataQualityDatasetApplication,
  DataQualityDatasetDefinition,
  DataQualityDatasetObject,
  DataQualityDatasetProductTeam,
  DataQualityDatasetTable,
  DataQualityProfileRunListResponse
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import PageHeader from '../components/common/PageHeader';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

const formatDescription = (value?: string | null) =>
  value && value.trim().length > 0 ? value : 'No description provided.';

const STATUS_RUNNING = ['running', 'queued', 'pending', 'in_progress', 'starting'];
const STATUS_FAILED = ['failed', 'error', 'errored', 'cancelled', 'canceled'];

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

const describeStatusTimestamps = (startedAt?: string | null, completedAt?: string | null) => {
  if (startedAt && completedAt) {
    return `Started ${formatDateTime(startedAt)} · Completed ${formatDateTime(completedAt)}`;
  }
  if (startedAt) {
    return `Started ${formatDateTime(startedAt)} · Waiting for completion`;
  }
  return 'Waiting for Databricks job to launch…';
};

const formatTablePrimary = (table: DataQualityDatasetTable) => {
  const schemaPrefix = table.schemaName ? `${table.schemaName}.` : '';
  return `${schemaPrefix}${table.tableName}`;
};

const formatTableSecondary = (table: DataQualityDatasetTable) => {
  const details: string[] = [];
  if (table.alias && table.alias !== table.tableName) {
    details.push(`Alias: ${table.alias}`);
  }
  if (table.description) {
    details.push(table.description);
  }
  if (table.tableType) {
    details.push(`Type: ${table.tableType}`);
  }
  if (table.isConstructed) {
    details.push('Constructed table');
  }
  if (table.loadOrder !== undefined && table.loadOrder !== null) {
    details.push(`Load order ${table.loadOrder}`);
  }
  return details.join(' • ');
};

const TablesList = ({ tables }: { tables: DataQualityDatasetTable[] }) => {
  if (!tables.length) {
    return <Typography color="text.secondary">No tables registered yet.</Typography>;
  }

  return (
    <List dense disablePadding>
      {tables.map((table) => (
        <ListItem key={table.dataDefinitionTableId} divider>
          <ListItemText
            primary={formatTablePrimary(table)}
            secondary={formatTableSecondary(table) || undefined}
          />
        </ListItem>
      ))}
    </List>
  );
};

const DataDefinitionSection = ({ definition }: { definition: DataQualityDatasetDefinition }) => (
  <Paper variant="outlined" sx={{ p: 2 }}>
    <Stack spacing={1.5}>
      <Stack spacing={0.5}>
        <Typography variant="subtitle1" fontWeight={600}>
          Data Definition
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {formatDescription(definition.description)}
        </Typography>
      </Stack>
      <TablesList tables={definition.tables} />
    </Stack>
  </Paper>
);

interface DataObjectAccordionProps {
  dataObject: DataQualityDatasetObject;
  onRunProfiling?: (dataObjectId: string) => void;
  isProfiling?: boolean;
}

const DataObjectAccordion = ({ dataObject, onRunProfiling, isProfiling }: DataObjectAccordionProps) => {
  const hasTables = dataObject.dataDefinitions.some((definition) => definition.tables.length > 0);

  const handleRunProfiling = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!hasTables || !onRunProfiling) {
      return;
    }
    onRunProfiling(dataObject.dataObjectId);
  };

  return (
    <Accordion>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" width="100%">
          <Stack spacing={0.5} pr={2}>
            <Typography fontWeight={600}>{dataObject.name}</Typography>
            <Typography variant="body2" color="text.secondary">
              {formatDescription(dataObject.description)}
            </Typography>
          </Stack>
          {onRunProfiling ? (
            <Button
              size="small"
              variant="contained"
              onClick={handleRunProfiling}
              disabled={!hasTables || isProfiling}
              startIcon={isProfiling ? <CircularProgress size={14} color="inherit" /> : undefined}
            >
              {isProfiling ? 'Running…' : 'Run Profiling'}
            </Button>
          ) : null}
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <Stack spacing={2}>
          {dataObject.dataDefinitions.length > 0 ? (
            dataObject.dataDefinitions.map((definition) => (
              <DataDefinitionSection key={definition.dataDefinitionId} definition={definition} />
            ))
          ) : (
            <Typography color="text.secondary">No data definitions registered.</Typography>
          )}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

interface ApplicationAccordionProps {
  application: DataQualityDatasetApplication;
  onRunProfiling?: (dataObjectId: string) => void;
  profilingState?: { activeId: string | null; isLoading: boolean };
}

const ApplicationAccordion = ({ application, onRunProfiling, profilingState }: ApplicationAccordionProps) => (
  <Accordion>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Stack spacing={0.5}>
        <Typography fontWeight={600}>{application.name}</Typography>
        <Typography variant="body2" color="text.secondary">
          {`Physical name: ${application.physicalName}`} ·{' '}
          {`${application.dataObjects.length} data object${application.dataObjects.length === 1 ? '' : 's'}`}
        </Typography>
        {application.description ? (
          <Typography variant="body2" color="text.secondary">
            {application.description}
          </Typography>
        ) : null}
      </Stack>
    </AccordionSummary>
    <AccordionDetails>
      <Stack spacing={1.5}>
        {application.dataObjects.length > 0 ? (
          application.dataObjects.map((dataObject) => (
            <DataObjectAccordion
              key={dataObject.dataObjectId}
              dataObject={dataObject}
              onRunProfiling={onRunProfiling}
              isProfiling={
                Boolean(profilingState?.isLoading && profilingState?.activeId === dataObject.dataObjectId)
              }
            />
          ))
        ) : (
          <Typography color="text.secondary">No data objects mapped yet.</Typography>
        )}
      </Stack>
    </AccordionDetails>
  </Accordion>
);

interface ProductTeamAccordionProps {
  productTeam: DataQualityDatasetProductTeam;
  onRunProfiling?: (dataObjectId: string) => void;
  profilingState?: { activeId: string | null; isLoading: boolean };
}

const ProductTeamAccordion = ({
  productTeam,
  onRunProfiling,
  profilingState
}: ProductTeamAccordionProps) => (
  <Accordion defaultExpanded>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Stack spacing={0.5}>
        <Typography fontWeight={600}>{productTeam.name}</Typography>
        <Typography variant="body2" color="text.secondary">
          {formatDescription(productTeam.description)}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {`${productTeam.applications.length} application${productTeam.applications.length === 1 ? '' : 's'}`}
        </Typography>
      </Stack>
    </AccordionSummary>
    <AccordionDetails>
      <Stack spacing={1.5}>
        {productTeam.applications.length > 0 ? (
          productTeam.applications.map((application) => (
            <ApplicationAccordion
              key={application.applicationId}
              application={application}
              onRunProfiling={onRunProfiling}
              profilingState={profilingState}
            />
          ))
        ) : (
          <Typography color="text.secondary">No applications configured yet.</Typography>
        )}
      </Stack>
    </AccordionDetails>
  </Accordion>
);

const DataQualityDatasetsPage = () => {
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();
  const [profilingTargetId, setProfilingTargetId] = useState<string | null>(null);
  const [monitoredRuns, setMonitoredRuns] = useState<
    { tableGroupId: string; profileRunId: string; status?: string | null }[]
  >([]);

  const monitorQueries = useQueries(
    monitoredRuns.map((run) => ({
      queryKey: ['data-quality', 'profiling-runs', 'monitor', run.tableGroupId],
      queryFn: () =>
        fetchDataQualityProfileRuns({
          tableGroupId: run.tableGroupId,
          limit: 5,
          includeGroups: false
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
      return {
        ...run,
        status: entry?.status ?? run.status ?? null,
        startedAt: entry?.startedAt ?? null,
        completedAt: entry?.completedAt ?? null,
        isLoading: query?.isFetching ?? query?.isLoading ?? false
      };
    });
  }, [monitorQueries, monitoredRuns]);

  const monitoredError = monitorQueries.find((query) => query?.isError);
  const monitorErrorMessage = monitoredError ? resolveErrorMessage(monitoredError.error) : null;
  const hasPendingMonitoredRuns = monitoredRunStatuses.some((run) => !isTerminalStatus(run.status));
  const monitorIsFetching = monitorQueries.some((query) => query?.isFetching);

  const runProfilingMutation = useMutation(startDataObjectProfileRuns, {
    onMutate: (dataObjectId: string) => {
      setProfilingTargetId(dataObjectId);
    },
    onSuccess: (result) => {
      setMonitoredRuns((previous) => {
        const dedup = new Map(previous.map((run) => [run.profileRunId, run]));
        result.profileRuns.forEach((run) => {
          dedup.set(run.profileRunId, { ...run, status: 'queued' });
        });
        const entries = Array.from(dedup.values());
        return entries.length > 10 ? entries.slice(entries.length - 10) : entries;
      });
      const started = result.profileRuns.length;
      const skipped = result.skippedTableIds.length;
      const messageParts = [
        started > 0
          ? `Profiling started for ${started} table group${started === 1 ? '' : 's'}.`
          : 'No profiling runs were started.'
      ];
      if (skipped > 0) {
        messageParts.push(
          `${skipped} table${skipped === 1 ? '' : 's'} skipped due to missing connections.`
        );
      }
      showSuccess(messageParts.join(' '));
    },
    onError: (error) => {
      showError(`Unable to start profiling: ${resolveErrorMessage(error)}`);
    },
    onSettled: () => {
      setProfilingTargetId(null);
    }
  });

  const handleClearMonitoredRuns = () => {
    setMonitoredRuns([]);
  };

  const handleRunProfiling = useCallback(
    (dataObjectId: string) => {
      runProfilingMutation.mutate(dataObjectId);
    },
    [runProfilingMutation]
  );

  const profilingState = {
    activeId: profilingTargetId,
    isLoading: runProfilingMutation.isLoading
  };

  const hierarchyQuery = useQuery<DataQualityDatasetProductTeam[]>(
    ['data-quality', 'dataset-hierarchy'],
    fetchDatasetHierarchy,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        showError(`Unable to load dataset hierarchy: ${resolveErrorMessage(error)}`);
      }
    }
  );

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Data Quality Datasets"
        subtitle="Explore product teams, applications, data objects, and their registered data definitions synchronized from Conversion Central."
        actions={
          <Button component={RouterLink} to="/data-quality/profiling-runs" variant="outlined">
            View Profiling Runs
          </Button>
        }
      />

      {monitoredRunStatuses.length > 0 ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
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
                    <Stack spacing={0.5}>
                      <Typography variant="body2" color="text.secondary">
                        Table Group
                      </Typography>
                      <Typography fontFamily="monospace" fontSize={14}>
                        {run.tableGroupId}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Run ID: {run.profileRunId}
                      </Typography>
                    </Stack>
                    <Stack direction="row" spacing={1} alignItems="center">
                      {run.isLoading ? <CircularProgress size={16} /> : null}
                      <Chip label={formatStatusLabel(run.status)} color={resolveStatusChipColor(run.status)} size="small" />
                    </Stack>
                    <Button
                      component={RouterLink}
                      to={`/data-quality/profiling-runs?tableGroupId=${encodeURIComponent(run.tableGroupId)}`}
                      variant="outlined"
                      size="small"
                    >
                      View runs
                    </Button>
                  </Stack>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                    {describeStatusTimestamps(run.startedAt, run.completedAt)}
                  </Typography>
                </Paper>
              ))}
            </Stack>
          </Stack>
        </Paper>
      ) : null}

      {hierarchyQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : hierarchyQuery.isError ? (
        <Alert severity="error">Unable to load the dataset hierarchy.</Alert>
      ) : hierarchyQuery.data && hierarchyQuery.data.length > 0 ? (
        <Stack spacing={2}>
          {hierarchyQuery.data.map((productTeam) => (
            <ProductTeamAccordion
              key={productTeam.productTeamId}
              productTeam={productTeam}
              onRunProfiling={handleRunProfiling}
              profilingState={profilingState}
            />
          ))}
        </Stack>
      ) : (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Typography color="text.secondary">
            No dataset metadata has been captured yet. Create data definitions to populate this view.
          </Typography>
        </Paper>
      )}
      {snackbar}
    </Stack>
  );
};

export default DataQualityDatasetsPage;
