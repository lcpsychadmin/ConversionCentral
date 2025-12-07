import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Stack,
  Typography
} from '@mui/material';
import { useQuery } from 'react-query';
import { Link as RouterLink } from 'react-router-dom';

import PageHeader from '@components/common/PageHeader';
import { fetchDataQualityProfileRuns } from '@services/dataQualityService';
import {
  DataQualityProfileRunEntry,
  DataQualityProfileRunListResponse,
  DataQualityProfileRunTableGroup
} from '@cc-types/data';
import { ProfileRunResultsContainer, formatDateTime } from '@pages/DataQualityProfileRunResultsPage';

const ALL_PROCESS_AREAS = 'all';
const UNASSIGNED_PROCESS_AREA = 'unassigned';

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

const buildGroupMetadata = (
  tableGroups: DataQualityProfileRunTableGroup[],
  runs: DataQualityProfileRunEntry[]
) => {
  const lookup = new Map<string, DataQualityProfileRunTableGroup>();
  tableGroups.forEach((group) => lookup.set(group.tableGroupId, group));
  runs.forEach((run) => {
    if (lookup.has(run.tableGroupId)) {
      return;
    }
    lookup.set(run.tableGroupId, {
      tableGroupId: run.tableGroupId,
      tableGroupName: run.tableGroupName ?? run.tableGroupId,
      connectionId: run.connectionId ?? undefined,
      connectionName: run.connectionName ?? undefined,
      catalog: run.catalog ?? undefined,
      schemaName: run.schemaName ?? undefined,
      dataObjectId: run.dataObjectId ?? undefined,
      dataObjectName: run.dataObjectName ?? undefined,
      applicationId: run.applicationId ?? undefined,
      applicationName: run.applicationName ?? undefined,
      applicationDescription: run.applicationDescription ?? undefined,
      productTeamId: run.productTeamId ?? undefined,
      productTeamName: run.productTeamName ?? undefined,
      tableCount: run.tableCount ?? undefined,
      fieldCount: run.fieldCount ?? undefined
    });
  });
  return lookup;
};

const describeProcessAreaOption = (group: DataQualityProfileRunTableGroup) => {
  const id = group.productTeamId ?? UNASSIGNED_PROCESS_AREA;
  const label = group.productTeamName ?? 'Unassigned process area';
  return { id, label };
};

const DataQualityDatasetsPage = () => {
  const [selectedProcessAreaId, setSelectedProcessAreaId] = useState<string>(ALL_PROCESS_AREAS);
  const [selectedTableGroupId, setSelectedTableGroupId] = useState<string | null>(null);

  const profileRunsQuery = useQuery<DataQualityProfileRunListResponse>(
    ['data-quality', 'profiling-runs', 'catalog-view'],
    () =>
      fetchDataQualityProfileRuns({
        limit: 500,
        includeGroups: true
      }),
    {
      staleTime: 30 * 1000
    }
  );

  const runs = useMemo(() => profileRunsQuery.data?.runs ?? [], [profileRunsQuery.data]);
  const tableGroups = useMemo(() => profileRunsQuery.data?.tableGroups ?? [], [profileRunsQuery.data]);

  const latestRunsByGroup = useMemo(() => {
    if (!runs.length) {
      return new Map<string, DataQualityProfileRunEntry>();
    }
    const sorted = [...runs].sort((a, b) => {
      const aTime = new Date(a.startedAt ?? a.completedAt ?? 0).getTime();
      const bTime = new Date(b.startedAt ?? b.completedAt ?? 0).getTime();
      return bTime - aTime;
    });
    const lookup = new Map<string, DataQualityProfileRunEntry>();
    sorted.forEach((run) => {
      const existing = lookup.get(run.tableGroupId);
      if (existing?.status?.toLowerCase() === 'completed') {
        return;
      }

      const isCompleted = run.status?.toLowerCase() === 'completed';
      if (isCompleted) {
        lookup.set(run.tableGroupId, run);
        return;
      }

      if (!existing) {
        lookup.set(run.tableGroupId, run);
      }
    });
    return lookup;
  }, [runs]);

  const groupMetadata = useMemo(
    () => buildGroupMetadata(tableGroups, runs),
    [tableGroups, runs]
  );

  const catalogGroups = useMemo(() => {
    return Array.from(groupMetadata.values()).filter((group) => latestRunsByGroup.has(group.tableGroupId));
  }, [groupMetadata, latestRunsByGroup]);

  const processAreaOptions = useMemo(() => {
    const unique = new Map<string, { id: string; label: string }>();
    catalogGroups.forEach((group) => {
      const option = describeProcessAreaOption(group);
      if (!unique.has(option.id)) {
        unique.set(option.id, option);
      }
    });
    return Array.from(unique.values()).sort((a, b) => a.label.localeCompare(b.label));
  }, [catalogGroups]);

  const filteredGroups = useMemo(() => {
    const normalizedSelection = selectedProcessAreaId;
    const filtered = catalogGroups.filter((group) => {
      if (normalizedSelection === ALL_PROCESS_AREAS) {
        return true;
      }
      const option = describeProcessAreaOption(group);
      return option.id === normalizedSelection;
    });
    return filtered.sort((a, b) => {
      const left = a.tableGroupName ?? a.tableGroupId;
      const right = b.tableGroupName ?? b.tableGroupId;
      return left.localeCompare(right);
    });
  }, [catalogGroups, selectedProcessAreaId]);

  useEffect(() => {
    if (!filteredGroups.length) {
      setSelectedTableGroupId(null);
      return;
    }
    setSelectedTableGroupId((current) => {
      if (current && filteredGroups.some((group) => group.tableGroupId === current)) {
        return current;
      }
      return filteredGroups[0].tableGroupId;
    });
  }, [filteredGroups]);

  const handleProcessAreaChange = (event: SelectChangeEvent<string>) => {
    setSelectedProcessAreaId(event.target.value);
  };

  const handleTableGroupChange = (event: SelectChangeEvent<string>) => {
    setSelectedTableGroupId(event.target.value);
  };

  const activeGroup = useMemo(() => {
    if (!selectedTableGroupId) {
      return null;
    }
    return catalogGroups.find((group) => group.tableGroupId === selectedTableGroupId) ?? null;
  }, [catalogGroups, selectedTableGroupId]);

  const activeRun = activeGroup ? latestRunsByGroup.get(activeGroup.tableGroupId) ?? null : null;
  const activeLabel = activeGroup ? formatTableGroupLabel(activeGroup) : null;

  const filtersDisabled = profileRunsQuery.isLoading || !catalogGroups.length;

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Data Catalog"
        subtitle="Review the latest profiling run for each table group and drill into column-level metrics."
        actions={
          <Button component={RouterLink} to="/data-quality/profiling-runs" variant="outlined">
            Manage Profiling Runs
          </Button>
        }
      />

      <Paper sx={{ p: 3 }}>
        <Stack spacing={2}>
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ md: 'flex-end' }}>
            <FormControl size="small" sx={{ minWidth: 220 }} disabled={filtersDisabled}>
              <InputLabel id="catalog-process-area-label">Process Area</InputLabel>
              <Select
                labelId="catalog-process-area-label"
                value={selectedProcessAreaId}
                label="Process Area"
                onChange={handleProcessAreaChange}
              >
                <MenuItem value={ALL_PROCESS_AREAS}>All process areas</MenuItem>
                {processAreaOptions.map((option) => (
                  <MenuItem key={option.id} value={option.id}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 240 }} disabled={filtersDisabled || !filteredGroups.length}>
              <InputLabel id="catalog-table-group-label">Table Group</InputLabel>
              <Select
                labelId="catalog-table-group-label"
                value={selectedTableGroupId ?? ''}
                label="Table Group"
                onChange={handleTableGroupChange}
                displayEmpty
              >
                {filteredGroups.length === 0 ? (
                  <MenuItem value="" disabled>
                    No table groups available
                  </MenuItem>
                ) : (
                  filteredGroups.map((group) => (
                    <MenuItem key={group.tableGroupId} value={group.tableGroupId}>
                      {group.tableGroupName ?? group.tableGroupId}
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>
          </Stack>

          {activeRun ? (
            <Typography variant="body2" color="text.secondary">
              Latest run started {formatDateTime(activeRun.startedAt)} · Status {activeRun.status.replace(/_/g, ' ')}
            </Typography>
          ) : (
            <Typography variant="body2" color="text.secondary">
              Select a process area and table group to load profiling results.
            </Typography>
          )}
        </Stack>
      </Paper>

      {profileRunsQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={6}>
          <CircularProgress />
        </Box>
      ) : null}

      {profileRunsQuery.isError ? (
        <Alert severity="error">Unable to load profiling data. Please try again later.</Alert>
      ) : null}

      {!profileRunsQuery.isLoading && !catalogGroups.length ? (
        <Alert severity="info">No profiling runs have completed yet. Launch a profiling run to populate the catalog.</Alert>
      ) : null}

      {activeGroup && activeRun ? (
        <Box sx={{ width: '100%' }}>
          <ProfileRunResultsContainer
            profileRunId={activeRun.profileRunId}
            tableGroupId={activeGroup.tableGroupId}
            tableGroupLabel={activeLabel}
            showBackButton={false}
          />
        </Box>
      ) : null}
    </Stack>
  );
};

export default DataQualityDatasetsPage;
