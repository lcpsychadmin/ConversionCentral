import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Grid,
  List,
  ListItem,
  ListItemSecondaryAction,
  ListItemText,
  Paper,
  Stack,
  Typography
} from '@mui/material';

import ProcessAreaTable, { ProcessAreaRow } from '../components/process-area/ProcessAreaTable';
import ProcessAreaForm from '../components/process-area/ProcessAreaForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useProcessAreas } from '../hooks/useProcessAreas';
import { DataObject, ProcessAreaFormValues } from '../types/data';
import { useAuth } from '../context/AuthContext';
import { useDataObjects } from '../hooks/useDataObjects';

const getErrorMessage = (error: unknown, fallback: string) => {
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const getStatusColor = (status: string): 'default' | 'success' | 'warning' => {
  switch (status) {
    case 'active':
      return 'success';
    case 'draft':
      return 'warning';
    default:
      return 'default';
  }
};

const ProcessAreasPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    processAreasQuery,
    createProcessArea,
    updateProcessArea,
    deleteProcessArea,
    creating,
    updating,
    deleting
  } = useProcessAreas();

  const {
    data: processAreas = [],
    isLoading: processAreasLoading,
    isError: processAreasError,
    error: processAreasErrorDetails
  } = processAreasQuery;

  const { dataObjectsQuery } = useDataObjects();
  const {
    data: dataObjects = [],
    isLoading: dataObjectsLoading,
    isError: dataObjectsError,
    error: dataObjectsErrorDetails
  } = dataObjectsQuery;

  const [selected, setSelected] = useState<ProcessAreaRow | null>(null);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [formOpen, setFormOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const rows = useMemo<ProcessAreaRow[]>(() => [...processAreas], [processAreas]);

  const handleSelect = (processArea: ProcessAreaRow | null) => {
    setSelected(processArea);
  };

  const handleCreateClick = () => {
    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (processArea: ProcessAreaRow) => {
    setFormMode('edit');
    setSelected(processArea);
    setFormOpen(true);
  };

  const handleDelete = (processArea: ProcessAreaRow) => {
    setSelected(processArea);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: ProcessAreaFormValues) => {
    try {
      if (formMode === 'create') {
        await createProcessArea({
          name: values.name,
          description: values.description ?? null,
          status: values.status
        });
      } else if (selected) {
        const payload: Partial<ProcessAreaFormValues> = {
          name: values.name,
          description: values.description ?? null,
          status: values.status
        };
        await updateProcessArea({ id: selected.id, input: payload });
      }
      setFormOpen(false);
    } catch (error) {
      // handled by hook toast helpers
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteProcessArea(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (error) {
      // handled by hook toast helpers
    }
  };

  const busy = creating || updating || deleting;
  const tableLoading = processAreasLoading;
  const processAreasErrorMessage = processAreasError
    ? getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.')
    : null;
  const dataObjectsErrorMessage = dataObjectsError
    ? getErrorMessage(dataObjectsErrorDetails, 'Unable to load data objects.')
    : null;

  const assignedDataObjects = useMemo<DataObject[]>(() => {
    if (!selected) return [];
    return dataObjects.filter((obj) => obj.processAreaId === selected.id);
  }, [selected, dataObjects]);

  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Process Areas
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Manage process areas and review their related data objects.
          </Typography>
        </div>
        {canManage && (
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New Process Area
          </Button>
        )}
      </Stack>

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
        <ProcessAreaTable
          data={rows}
          loading={tableLoading}
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

      {selected && (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Details
              </Typography>
              <Stack spacing={1}>
                <DetailLine label="Name" value={selected.name} />
                <DetailLine label="Description" value={selected.description ?? '—'} />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Status
              </Typography>
              <Chip label={formatStatusLabel(selected.status)} color={getStatusColor(selected.status)} />
            </Grid>
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom>
                Assigned Data Objects
              </Typography>
              {dataObjectsLoading ? (
                <Box display="flex" alignItems="center" gap={1}>
                  <CircularProgress size={20} />
                  <Typography variant="body2" color="text.secondary">
                    Loading data objects…
                  </Typography>
                </Box>
              ) : assignedDataObjects.length === 0 ? (
                <Alert severity="info">No data objects are linked to this process area yet.</Alert>
              ) : (
                <List dense>
                  {assignedDataObjects.map((obj) => (
                    <ListItem key={obj.id} disablePadding>
                      <ListItemText
                        primary={obj.name}
                        secondary={obj.description ?? 'No description'}
                      />
                      <ListItemSecondaryAction>
                        <Chip size="small" label={obj.status} />
                      </ListItemSecondaryAction>
                    </ListItem>
                  ))}
                </List>
              )}
            </Grid>
          </Grid>
        </Paper>
      )}

      {canManage && (
        <ProcessAreaForm
          open={formOpen}
          title={formMode === 'create' ? 'Create Process Area' : 'Edit Process Area'}
          initialValues={formMode === 'edit' ? selected : null}
          loading={busy}
          onClose={handleFormClose}
          onSubmit={handleFormSubmit}
        />
      )}

      <ConfirmDialog
        open={confirmOpen}
        title="Delete Process Area"
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

export default ProcessAreasPage;
