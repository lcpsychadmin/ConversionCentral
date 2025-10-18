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

import SystemTable from '../components/system/SystemTable';
import SystemForm from '../components/system/SystemForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { System, SystemFormValues } from '../types/data';
import { useAuth } from '../context/AuthContext';

const getErrorMessage = (error: unknown, fallback: string) => {
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

const SystemsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    systemsQuery,
    createSystem,
    updateSystem,
    deleteSystem,
    creating,
    updating,
    deleting
  } = useSystems();

  const {
    data: systems = [],
    isLoading,
    isError,
    error
  } = systemsQuery;

  const [selected, setSelected] = useState<System | null>(null);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [formOpen, setFormOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const sortedSystems = useMemo(
    () => systems.slice().sort((a, b) => a.name.localeCompare(b.name)),
    [systems]
  );

  const handleSelect = (system: System | null) => {
    setSelected(system);
  };

  const handleCreateClick = () => {
    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (system: System) => {
    setFormMode('edit');
    setSelected(system);
    setFormOpen(true);
  };

  const handleDelete = (system: System) => {
    setSelected(system);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: SystemFormValues) => {
    try {
      if (formMode === 'create') {
        await createSystem({
          name: values.name,
          physicalName: values.physicalName,
          description: values.description ?? null,
          systemType: values.systemType ?? null,
          status: values.status,
          securityClassification: values.securityClassification ?? null
        });
      } else if (selected) {
        await updateSystem({
          id: selected.id,
          input: {
            name: values.name,
            physicalName: values.physicalName,
            description: values.description ?? null,
            systemType: values.systemType ?? null,
            status: values.status,
            securityClassification: values.securityClassification ?? null
          }
        });
      }
      setFormOpen(false);
    } catch (err) {
      // notifications handled in hook
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteSystem(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (err) {
      // notifications handled in hook
    }
  };

  const busy = creating || updating || deleting;
  const errorMessage = isError ? getErrorMessage(error, 'Unable to load systems.') : null;

  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Systems
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Maintain the catalog of applications and systems available for data objects.
          </Typography>
        </div>
        {canManage && (
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New System
          </Button>
        )}
      </Stack>

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
        <SystemTable
          data={sortedSystems}
          loading={isLoading}
          selectedId={selected?.id ?? null}
          canManage={canManage}
          onSelect={handleSelect}
          onEdit={canManage ? handleEdit : undefined}
          onDelete={canManage ? handleDelete : undefined}
        />
      </Paper>

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
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
                <DetailLine label="Physical Name" value={selected.physicalName} />
                <DetailLine label="Type" value={selected.systemType ?? '—'} />
                <DetailLine label="Description" value={selected.description ?? '—'} />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Status
              </Typography>
              <Chip label={selected.status} color={selected.status === 'active' ? 'success' : 'default'} />
              <Divider sx={{ my: 1.5 }} />
              <DetailLine
                label="Security Classification"
                value={selected.securityClassification ?? '—'}
              />
            </Grid>
          </Grid>
        </Paper>
      )}

      {canManage && (
        <SystemForm
          open={formOpen}
          title={formMode === 'create' ? 'Create System' : 'Edit System'}
          initialValues={formMode === 'edit' ? selected : null}
          loading={busy}
          onClose={handleFormClose}
          onSubmit={handleFormSubmit}
        />
      )}

      <ConfirmDialog
        open={confirmOpen}
        title="Delete System"
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

export default SystemsPage;
