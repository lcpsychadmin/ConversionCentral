import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Grid,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

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
  const theme = useTheme();

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
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
          borderBottom: `3px solid ${theme.palette.primary.main}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
        }}
      >
        <Typography variant="h4" gutterBottom sx={{ color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }}>
          Inventory
        </Typography>
        <Typography variant="body2" sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}>
          Manage data object inventory, process area alignment, and system ownership.
        </Typography>
      </Box>

      {canManage && (
        <Box sx={{ mb: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New Data Object
          </Button>
        </Box>
      )}

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

      <Paper elevation={3} sx={{ 
        p: 3, 
        mb: 3,
        background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
      }}>
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

      {selected && (
        <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
          <Typography variant="h5" gutterBottom sx={{ color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }}>
            Data Object Details
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Stack spacing={2}>
                <DetailLine label="Name" value={selected.name} />
                <DetailLine label="Domain" value={selected.processAreaName ?? 'Unassigned'} />
                <DetailLine label="Description" value={selected.description ?? '—'} />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Box>
                <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
                  Status
                </Typography>
                <Chip label={selected.status} color={selected.status === 'active' ? 'success' : 'default'} sx={{ mb: 2 }} />
              </Box>
              <Box>
                <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 1 }}>
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
              </Box>
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
    <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
      {label}
    </Typography>
    <Typography variant="body1">{value}</Typography>
  </Box>
);

export default InventoryPage;
