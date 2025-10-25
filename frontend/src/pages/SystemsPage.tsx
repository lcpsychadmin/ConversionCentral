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
  const theme = useTheme();

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
    () => systems.slice().sort((a, b) => (a.name ?? '').localeCompare(b.name ?? '')),
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
          Systems
        </Typography>
        <Typography variant="body2" sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}>
          Maintain the catalog of applications and systems available for data objects.
        </Typography>
      </Box>

      {canManage && (
        <Box sx={{ mb: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New System
          </Button>
        </Box>
      )}

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      <Paper elevation={3} sx={{ 
        p: 3, 
        mb: 3,
        background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
      }}>
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

      {selected && (
        <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
          <Typography variant="h5" gutterBottom sx={{ color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }}>
            System Details
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Stack spacing={2}>
                <DetailLine label="Name" value={selected.name} />
                <DetailLine label="Physical Name" value={selected.physicalName} />
                <DetailLine label="Type" value={selected.systemType ?? '—'} />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Stack spacing={2}>
                <DetailLine label="Status" value={selected.status} isChip={true} chipColor={selected.status === 'active' ? 'success' : 'default'} />
                <DetailLine label="Description" value={selected.description ?? '—'} />
                <DetailLine label="Security Classification" value={selected.securityClassification ?? '—'} />
              </Stack>
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
  isChip?: boolean;
  chipColor?: 'default' | 'primary' | 'secondary' | 'error' | 'warning' | 'info' | 'success';
}

const DetailLine = ({ label, value, isChip = false, chipColor = 'default' }: DetailLineProps) => (
  <Box>
    <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
      {label}
    </Typography>
    {isChip ? (
      <Chip label={value} color={chipColor} size="small" />
    ) : (
      <Typography variant="body1">{value}</Typography>
    )}
  </Box>
);

export default SystemsPage;
