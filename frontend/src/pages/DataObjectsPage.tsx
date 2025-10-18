import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Divider,
  Grid,
  Paper,
  Stack,
  Typography
} from '@mui/material';

import DataObjectTable, { DataObjectRow } from '../components/data-object/DataObjectTable';
import DataObjectForm from '../components/data-object/DataObjectForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useDataObjects } from '../hooks/useDataObjects';
import { useProcessAreas } from '../hooks/useProcessAreas';
import { useSystems } from '../hooks/useSystems';
import { DataObjectFormValues, ProcessArea, System } from '../types/data';
import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';

const findProcessAreaName = (processAreas: ProcessArea[], processAreaId: string | null) => {
  if (!processAreaId) return null;
  return processAreas.find((area) => area.id === processAreaId)?.name ?? null;
};

const getErrorMessage = (error: unknown, fallback: string) => {
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

const InventoryPage = () => {
  const { hasRole } = useAuth();
  const toast = useToast();
  const canManage = hasRole('admin');

  const {
    dataObjectsQuery,
    createDataObject,
    updateDataObject,
    deleteDataObject,
    creating,
    updating,
    deleting
  } = useDataObjects();

  const { processAreasQuery } = useProcessAreas();
  const {
    data: processAreas = [],
    isLoading: processAreasLoading,
    isFetching: processAreasFetching,
    isError: processAreasError,
    error: processAreasErrorDetails
  } = processAreasQuery;

  const {
    data: dataObjects = [],
    isLoading: dataObjectsLoading,
    isError: dataObjectsError,
    error: dataObjectsErrorDetails
  } = dataObjectsQuery;

  const { systemsQuery } = useSystems();
  const {
    data: systems = [],
    isLoading: systemsLoading,
    isError: systemsError,
    error: systemsErrorDetails
  } = systemsQuery;

  const [selected, setSelected] = useState<DataObjectRow | null>(null);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [formOpen, setFormOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const rows = useMemo<DataObjectRow[]>(
    () =>
      dataObjects.map((item) => ({
        ...item,
        processAreaName: findProcessAreaName(processAreas, item.processAreaId),
        systems: item.systems ?? [],
        systemNames: (item.systems ?? []).map((system) => system.name)
      })),
    [dataObjects, processAreas]
  );

  const handleSelect = (dataObject: DataObjectRow | null) => {
    setSelected(dataObject);
  };

  const handleCreateClick = () => {
    if (!canManage) return;
    if (processAreasLoading || processAreasFetching || systemsLoading) {
      toast.showInfo('Process areas or systems are still loading. Try again in a moment.');
      return;
    }
    if (processAreasError) {
      const message = getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.');
      toast.showError(message);
      return;
    }
    if (systemsError) {
      const message = getErrorMessage(systemsErrorDetails, 'Unable to load systems.');
      toast.showError(message);
      return;
    }
    if (processAreas.length === 0) {
      toast.showInfo('Create a process area before adding data objects.');
      return;
    }

    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (dataObject: DataObjectRow) => {
    if (!canManage) return;
    setFormMode('edit');
    setSelected(dataObject);
    setFormOpen(true);
  };

  const handleDelete = (dataObject: DataObjectRow) => {
    if (!canManage) return;
    setSelected(dataObject);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: DataObjectFormValues) => {
    try {
      if (formMode === 'create') {
        await createDataObject({
          name: values.name,
          description: values.description ?? null,
          status: values.status,
          processAreaId: values.processAreaId,
          systemIds: values.systemIds
        });
      } else if (selected) {
        await updateDataObject({
          id: selected.id,
          input: {
            name: values.name,
            description: values.description ?? null,
            status: values.status,
            processAreaId: values.processAreaId,
            systemIds: values.systemIds
          }
        });
      }
      setFormOpen(false);
    } catch (error) {
      // handled by toast hook
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteDataObject(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (error) {
      // handled by toast hook
    }
  };

  const busy = creating || updating || deleting;
  const noProcessAreasAvailable = !processAreasLoading && !processAreasFetching && processAreas.length === 0;
  const dataObjectsErrorMessage = dataObjectsError
    ? getErrorMessage(dataObjectsErrorDetails, 'Unable to load data objects.')
    : null;
  const processAreasErrorMessage = processAreasError
    ? getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.')
    : null;
  const systemsErrorMessage = systemsError
    ? getErrorMessage(systemsErrorDetails, 'Unable to load systems.')
    : null;

  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Inventory
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Manage data object inventory, process area alignment, and system ownership.
          </Typography>
        </div>
        {canManage && (
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New Data Object
          </Button>
        )}
      </Stack>

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
        <DataObjectTable
          data={rows}
          loading={dataObjectsLoading}
          selectedId={selected?.id ?? null}
          canManage={canManage}
          onSelect={handleSelect}
          onEdit={canManage ? handleEdit : undefined}
          onDelete={canManage ? handleDelete : undefined}
        />
      </Paper>

      {dataObjectsErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {dataObjectsErrorMessage}
        </Alert>
      )}

      {processAreasErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {processAreasErrorMessage}
        </Alert>
      )}

      {systemsErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {systemsErrorMessage}
        </Alert>
      )}

      {canManage && noProcessAreasAvailable && (
        <Alert severity="info" sx={{ mb: 3 }}>
          No process areas are available yet. Create a process area to enable new data objects.
        </Alert>
      )}

      {selected && (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Details
              </Typography>
              <Stack spacing={1}>
                <DetailLine label="Name" value={selected.name} />
                <DetailLine label="Domain" value={selected.processAreaName ?? 'Unassigned'} />
                <DetailLine label="Description" value={selected.description ?? '—'} />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Status
              </Typography>
              <Chip label={selected.status} color={selected.status === 'active' ? 'success' : 'default'} />
              <Divider sx={{ my: 1.5 }} />
              <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                Systems
              </Typography>
              {(selected.systems?.length ?? 0) === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  No systems assigned.
                </Typography>
              ) : (
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  {selected.systems?.map((system: System) => (
                    <Chip key={system.id} label={system.name} size="small" />
                  ))}
                </Stack>
              )}
            </Grid>
          </Grid>
        </Paper>
      )}

      {canManage && (
        <DataObjectForm
          open={formOpen}
          title={formMode === 'create' ? 'Create Data Object' : 'Edit Data Object'}
          initialValues={formMode === 'edit' ? selected : null}
          loading={busy}
          onClose={handleFormClose}
          onSubmit={handleFormSubmit}
          processAreas={processAreas}
          systems={systems}
        />
      )}

      <ConfirmDialog
        open={confirmOpen}
        title="Delete Data Object"
        description={`Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`}
        confirmLabel="Delete"
        onClose={() => setConfirmOpen(false)}
        onConfirm={handleConfirmDelete}
      />
    </Box>
  );
};

interface DetailLineProps {
  label: string;
  value: string;
}

const DetailLine = ({ label, value }: DetailLineProps) => (
  <Box>
    <Typography variant="subtitle2" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="body1">{value}</Typography>
    <Divider sx={{ my: 1.5 }} />
  </Box>
);

export default InventoryPage;
