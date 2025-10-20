import { useMemo, useState } from 'react';
import { Alert, Box, Button, Stack, Typography } from '@mui/material';

import { System, SystemConnection, ConnectionCatalogTable, IngestionScheduleInput, IngestionScheduleUpdateInput, IngestionSchedule } from '../../types/data';
import { useIngestionSchedules } from '../../hooks/useIngestionSchedules';
import IngestionScheduleForm, { FormValues as ScheduleFormValues, ScheduleSelectionOption } from './IngestionScheduleForm';
import IngestionScheduleTable, { SelectionMetadata } from './IngestionScheduleTable';
import { buildIngestionTargetName } from '../../utils/ingestion';

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
    creating,
    updating,
    triggering
  } = useIngestionSchedules();

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<IngestionSchedule | null>(null);
  const [busyScheduleId, setBusyScheduleId] = useState<string | null>(null);

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

  const handleFormSubmit = async (values: ScheduleFormValues) => {
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
        <Button variant="contained" onClick={handleCreateClick} disabled={!canCreate || creating}>
          New Schedule
        </Button>
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
        loading={schedulesLoading || updating || triggering}
        busyIds={busyIds}
        onEdit={handleEdit}
        onRun={handleRun}
        onToggleActive={handleToggleActive}
      />

      <IngestionScheduleForm
        open={formOpen}
        title={editing ? 'Edit Schedule' : 'Create Schedule'}
        options={selectionOptions}
        initialValues={editing}
        disableSelectionChange={Boolean(editing)}
        loading={creating || updating}
        onClose={() => {
          setFormOpen(false);
          setEditing(null);
        }}
        onSubmit={handleFormSubmit}
      />
    </Box>
  );
};

export default ConnectionIngestionPanel;
