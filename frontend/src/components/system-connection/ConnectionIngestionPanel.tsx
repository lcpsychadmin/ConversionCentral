import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Stack,
  Typography
} from '@mui/material';

import {
  System,
  SystemConnection,
  ConnectionCatalogTable,
  IngestionScheduleInput,
  IngestionScheduleUpdateInput,
  IngestionSchedule,
  IngestionRun,
  DataWarehouseTarget
} from '../../types/data';
import { useIngestionSchedules } from '../../hooks/useIngestionSchedules';
import { useToast } from '../../hooks/useToast';
import IngestionScheduleForm, {
  FormValues as ScheduleFormValues,
  ScheduleSelectionOption,
  SapHanaOption,
  WarehouseOption
} from './IngestionScheduleForm';
import IngestionScheduleTable, { SelectionMetadata } from './IngestionScheduleTable';
import { buildIngestionTargetName } from '../../utils/ingestion';
import { fetchIngestionRuns } from '../../services/ingestionScheduleService';
import IngestionRunHistoryDialog from './IngestionRunHistoryDialog';
import { useDatabricksSettings } from '../../hooks/useDatabricksSettings';
import { useSapHanaSettings } from '../../hooks/useSapHanaSettings';

interface ConnectionIngestionPanelProps {
  connection: SystemConnection;
  system?: System | null;
  catalogRows: ConnectionCatalogTable[];
}

const trimOrUndefined = (value?: string | null) => {
  if (value === undefined || value === null) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

const ConnectionIngestionPanel = ({ connection, system, catalogRows }: ConnectionIngestionPanelProps) => {
  const {
    schedulesQuery,
    createSchedule,
    updateSchedule,
    triggerSchedule,
    deleteSchedule,
    abortRun,
    cleanupStuckRuns,
    creating,
    updating,
    triggering,
    deleting,
    aborting,
    cleaning
  } = useIngestionSchedules();
  const toast = useToast();

  const { settingsQuery: databricksSettingsQuery } = useDatabricksSettings();
  const { settingsQuery: sapHanaSettingsQuery } = useSapHanaSettings();

  const databricksSettings = databricksSettingsQuery.data;
  const sapHanaSetting = sapHanaSettingsQuery.data;

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<IngestionSchedule | null>(null);
  const [busyScheduleId, setBusyScheduleId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<IngestionSchedule | null>(null);
  const [abortTarget, setAbortTarget] = useState<IngestionSchedule | null>(null);
  const [runsTarget, setRunsTarget] = useState<IngestionSchedule | null>(null);
  const [runs, setRuns] = useState<IngestionRun[]>([]);
  const [runsLoading, setRunsLoading] = useState(false);

  const selectionLookup = useMemo(() => {
    const map = new Map<string, SelectionMetadata>();
    catalogRows.forEach((row) => {
      if (!row.selectionId) {
        return;
      }
      const targetPreview = buildIngestionTargetName(system ?? undefined, connection, row.schemaName, row.tableName);
      map.set(row.selectionId, {
        schemaName: row.schemaName,
        tableName: row.tableName,
        targetPreview
      });
    });
    return map;
  }, [catalogRows, connection, system]);

  const selectionOptions = useMemo<ScheduleSelectionOption[]>(() => {
    return catalogRows
      .filter((row) => row.selectionId)
      .map((row) => ({
        id: row.selectionId as string,
        label: `${row.schemaName}.${row.tableName}`,
        schemaName: row.schemaName,
        tableName: row.tableName,
        targetPreview: buildIngestionTargetName(system ?? undefined, connection, row.schemaName, row.tableName),
        disabled: !row.selected
      }));
  }, [catalogRows, connection, system]);

  const selectionIds = useMemo(() => new Set(selectionLookup.keys()), [selectionLookup]);

  const { data: schedules = [], isLoading: schedulesLoading } = schedulesQuery;

  const warehouseOptions = useMemo<WarehouseOption[]>(() => {
    const options: WarehouseOption[] = [
      {
        value: 'databricks_sql',
        label: databricksSettings?.displayName ?? 'Databricks SQL Warehouse',
        helperText: databricksSettings
          ? `Loads into ${databricksSettings.catalog ?? 'workspace'} catalog.`
          : 'Uses the default Databricks SQL warehouse configuration.'
      }
    ];
    options.push({
      value: 'sap_hana',
      label: sapHanaSetting?.displayName ?? 'SAP HANA Warehouse',
      disabled: !sapHanaSetting,
      helperText: sapHanaSetting ? 'Loads into the configured SAP HANA warehouse.' : 'Configure SAP HANA settings first.'
    });
    return options;
  }, [databricksSettings, sapHanaSetting]);

  const sapHanaOptions = useMemo<SapHanaOption[]>(() => {
    if (!sapHanaSetting) {
      return [];
    }
    return [
      {
        id: sapHanaSetting.id,
        label: sapHanaSetting.displayName,
        disabled: !sapHanaSetting.isActive
      }
    ];
  }, [sapHanaSetting]);

  const warehouseLabels = useMemo<Record<DataWarehouseTarget, string>>(() => ({
    databricks_sql: databricksSettings?.displayName ?? 'Databricks SQL',
    sap_hana: sapHanaSetting?.displayName ?? 'SAP HANA'
  }), [databricksSettings, sapHanaSetting]);

  const relevantSchedules = useMemo(
    () => schedules.filter((schedule) => selectionIds.has(schedule.connectionTableSelectionId)),
    [schedules, selectionIds]
  );

  const busyIds = useMemo(() => {
    if (!busyScheduleId) return undefined;
    return new Set<string>([busyScheduleId]);
  }, [busyScheduleId]);

  const handleCreateClick = () => {
    setEditing(null);
    setFormOpen(true);
  };

  const handleEdit = (schedule: IngestionSchedule) => {
    setEditing(schedule);
    setFormOpen(true);
  };

  const handleRun = async (schedule: IngestionSchedule) => {
    setBusyScheduleId(schedule.id);
    try {
      await triggerSchedule(schedule.id);
    } finally {
      setBusyScheduleId(null);
    }
  };

  const handleDeleteRequest = (schedule: IngestionSchedule) => {
    setDeleteTarget(schedule);
  };

  const handleConfirmDelete = async () => {
    if (!deleteTarget) {
      return;
    }
    setBusyScheduleId(deleteTarget.id);
    try {
      await deleteSchedule(deleteTarget.id);
    } finally {
      setDeleteTarget(null);
      setBusyScheduleId(null);
    }
  };

  const handleCancelDelete = () => {
    if (busyScheduleId && deleteTarget && busyScheduleId === deleteTarget.id) {
      return;
    }
    setDeleteTarget(null);
  };

  const handleAbortRequest = (schedule: IngestionSchedule) => {
    setAbortTarget(schedule);
  };

  const handleConfirmAbort = async () => {
    if (!abortTarget) {
      return;
    }
    setBusyScheduleId(abortTarget.id);
    let runId: string | null = null;
    try {
      const runs = await fetchIngestionRuns(abortTarget.id);
      const runningRun = runs.find((run) => run.status === 'running');
      if (!runningRun) {
        toast.showInfo('No active run found to abort.');
        return;
      }
      runId = runningRun.id;
      await abortRun({ scheduleId: abortTarget.id, runId });
    } catch (error) {
      if (runId === null) {
        const message = error instanceof Error ? error.message : 'Failed to abort run.';
        toast.showError(message);
      }
    } finally {
      setAbortTarget(null);
      setBusyScheduleId(null);
    }
  };

  const handleCancelAbort = () => {
    if (busyScheduleId && abortTarget && busyScheduleId === abortTarget.id) {
      return;
    }
    setAbortTarget(null);
  };

  const handleToggleActive = async (schedule: IngestionSchedule, isActive: boolean) => {
    setBusyScheduleId(schedule.id);
    try {
      const payload: IngestionScheduleUpdateInput = {
        isActive
      };
      await updateSchedule({ id: schedule.id, input: payload });
    } finally {
      setBusyScheduleId(null);
    }
  };

  const loadRuns = async (schedule: IngestionSchedule) => {
    setRunsLoading(true);
    try {
      const data = await fetchIngestionRuns(schedule.id);
      setRuns(data);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load run history.';
      toast.showError(message);
      setRuns([]);
    } finally {
      setRunsLoading(false);
    }
  };

  const handleViewRuns = async (schedule: IngestionSchedule) => {
    setRunsTarget(schedule);
    await loadRuns(schedule);
  };

  const handleRefreshRuns = async () => {
    if (!runsTarget) {
      return;
    }
    await loadRuns(runsTarget);
  };

  const handleCloseRuns = () => {
    if (runsLoading) {
      return;
    }
    setRunsTarget(null);
    setRuns([]);
  };

  const handleCleanupStuckRuns = async () => {
    await cleanupStuckRuns();
  };

  const handleFormSubmit = async (values: ScheduleFormValues) => {
    const normalizedSapHanaId = values.targetWarehouse === 'sap_hana'
      ? values.sapHanaSettingId && values.sapHanaSettingId.length > 0
        ? values.sapHanaSettingId
        : null
      : null;

    if (editing) {
      setBusyScheduleId(editing.id);
      try {
        const payload: IngestionScheduleUpdateInput = {
          scheduleExpression: values.scheduleExpression,
          timezone: trimOrUndefined(values.timezone),
          loadStrategy: values.loadStrategy,
          watermarkColumn: trimOrUndefined(values.watermarkColumn),
          primaryKeyColumn: trimOrUndefined(values.primaryKeyColumn),
          targetSchema: trimOrUndefined(values.targetSchema),
          targetWarehouse: values.targetWarehouse,
          sapHanaSettingId: normalizedSapHanaId,
          batchSize: values.batchSize,
          isActive: values.isActive
        };
        await updateSchedule({ id: editing.id, input: payload });
        setFormOpen(false);
        setEditing(null);
      } finally {
        setBusyScheduleId(null);
      }
      return;
    }

    try {
      const payload: IngestionScheduleInput = {
        connectionTableSelectionId: values.connectionTableSelectionId,
        scheduleExpression: values.scheduleExpression,
        timezone: trimOrUndefined(values.timezone) ?? null,
        loadStrategy: values.loadStrategy,
        watermarkColumn: trimOrUndefined(values.watermarkColumn) ?? null,
        primaryKeyColumn: trimOrUndefined(values.primaryKeyColumn) ?? null,
        targetSchema: trimOrUndefined(values.targetSchema) ?? null,
        targetTableName: null,
         targetWarehouse: values.targetWarehouse,
         sapHanaSettingId: normalizedSapHanaId,
        batchSize: values.batchSize,
        isActive: values.isActive
      };
      await createSchedule(payload);
      setFormOpen(false);
    } finally {
      setEditing(null);
      setBusyScheduleId(null);
    }
  };

  const canCreate = catalogRows.some((row) => row.selected && row.selectionId);
  const nothingSelected = catalogRows.filter((row) => row.selected).length === 0;

  return (
    <Box>
      <Stack direction={{ xs: 'column', sm: 'row' }} justifyContent="space-between" alignItems={{ xs: 'flex-start', sm: 'center' }} gap={2} mb={2.5}>
        <Box>
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            Ingestion Schedules
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Use cron expressions to ingest selected tables when this connection is enabled.
          </Typography>
        </Box>
        <Stack direction="row" spacing={1}>
          <Button
            variant="outlined"
            color="warning"
            onClick={handleCleanupStuckRuns}
            disabled={cleaning}
          >
            {cleaning ? 'Cleaning...' : 'Clean up runs'}
          </Button>
          <Button variant="contained" onClick={handleCreateClick} disabled={!canCreate || creating}>
            New Schedule
          </Button>
        </Stack>
      </Stack>

      {!canCreate && (
        <Alert severity="info" sx={{ mb: 2 }}>
          {nothingSelected
            ? 'Select one or more tables in the source catalog to enable scheduling.'
            : 'Save the catalog selection to continue. If you just added a table, refresh the catalog.'}
        </Alert>
      )}

      {relevantSchedules.length === 0 && schedulesLoading === false && canCreate && (
        <Alert severity="info" sx={{ mb: 2 }}>
          No schedules yet for this connection. Create one to begin automated ingestion.
        </Alert>
      )}

      <IngestionScheduleTable
        schedules={relevantSchedules}
        selectionLookup={selectionLookup}
        loading={schedulesLoading || updating || triggering || deleting || aborting || cleaning}
        busyIds={busyIds}
        warehouseLabels={warehouseLabels}
        onEdit={handleEdit}
        onRun={handleRun}
        onToggleActive={handleToggleActive}
        onDelete={handleDeleteRequest}
        onAbort={handleAbortRequest}
        onViewRuns={handleViewRuns}
      />

      <IngestionScheduleForm
        open={formOpen}
        title={editing ? 'Edit Schedule' : 'Create Schedule'}
        options={selectionOptions}
        warehouseOptions={warehouseOptions}
        sapHanaOptions={sapHanaOptions}
        initialValues={editing}
        disableSelectionChange={Boolean(editing)}
        loading={creating || updating}
        onClose={() => {
          setFormOpen(false);
          setEditing(null);
        }}
        onSubmit={handleFormSubmit}
        sapHanaSettingsPath="/data-configuration/data-warehouse"
      />

      <Dialog open={Boolean(deleteTarget)} onClose={handleCancelDelete} maxWidth="xs" fullWidth>
        <DialogTitle>Delete schedule</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete this ingestion schedule? This will remove its run history.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCancelDelete} disabled={Boolean(busyScheduleId)}>
            Cancel
          </Button>
          <Button color="error" onClick={handleConfirmDelete} disabled={Boolean(busyScheduleId)}>
            Delete
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={Boolean(abortTarget)} onClose={handleCancelAbort} maxWidth="xs" fullWidth>
        <DialogTitle>Abort ingestion run</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Stop the currently running ingestion job for this schedule?
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCancelAbort} disabled={Boolean(busyScheduleId)}>
            Cancel
          </Button>
          <Button color="warning" onClick={handleConfirmAbort} disabled={Boolean(busyScheduleId)}>
            Abort run
          </Button>
        </DialogActions>
      </Dialog>

      <IngestionRunHistoryDialog
        open={Boolean(runsTarget)}
        schedule={runsTarget}
        runs={runs}
        loading={runsLoading}
        onClose={handleCloseRuns}
        onRefresh={handleRefreshRuns}
      />
    </Box>
  );
};

export default ConnectionIngestionPanel;
