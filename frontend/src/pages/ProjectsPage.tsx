import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Divider,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useAuth } from '../context/AuthContext';
import ProjectTable, { ProjectRow } from '../components/project/ProjectTable';
import ProjectForm from '../components/project/ProjectForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useProjects } from '../hooks/useProjects';
import { ProjectFormValues } from '../types/data';

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const ProjectsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const theme = useTheme();

  const {
    projectsQuery,
    createProject,
    updateProject,
    deleteProject,
    creating,
    updating,
    deleting
  } = useProjects();

  const {
    data: projects = [],
    isLoading,
    isError,
    error
  } = projectsQuery;

  const [selected, setSelected] = useState<ProjectRow | null>(null);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [formOpen, setFormOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const rows = useMemo<ProjectRow[]>(() => [...projects], [projects]);

  const handleSelect = (project: ProjectRow | null) => {
    setSelected(project);
  };

  const handleCreateClick = () => {
    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (project: ProjectRow) => {
    setFormMode('edit');
    setSelected(project);
    setFormOpen(true);
  };

  const handleDelete = (project: ProjectRow) => {
    setSelected(project);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: ProjectFormValues) => {
    try {
      if (formMode === 'create') {
        await createProject(values);
      } else if (selected) {
        await updateProject({ id: selected.id, input: values });
      }
      setFormOpen(false);
    } catch (err) {
      // errors handled via hooks/toasts
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteProject(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (err) {
      // handled in hook
    }
  };

  const busy = creating || updating || deleting;
  const errorMessage = isError && error instanceof Error ? error.message : null;

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
          Projects
        </Typography>
        <Typography variant="body2" sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}>
          Create high-level projects to organize releases and implementation milestones.
        </Typography>
      </Box>

      {canManage && (
        <Box sx={{ mb: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New Project
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
        <ProjectTable
          data={rows}
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
            Project Details
          </Typography>
          <Stack spacing={2}>
            <Box>
              <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
                Status
              </Typography>
              <Chip label={formatStatusLabel(selected.status)} size="small" color="primary" />
            </Box>
            <Box>
              <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
                Description
              </Typography>
              <Typography variant="body1">
                {selected.description || 'No description provided.'}
              </Typography>
            </Box>
          </Stack>
        </Paper>
      )}

      <ProjectForm
        open={formOpen}
        title={formMode === 'create' ? 'Create Project' : 'Edit Project'}
        initialValues={formMode === 'edit' ? selected : null}
        loading={busy}
        onClose={handleFormClose}
        onSubmit={handleFormSubmit}
      />

      <ConfirmDialog
        open={confirmOpen}
        title="Delete Project"
        description={`Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`}
        confirmLabel="Delete"
        confirmColor="error"
        onClose={() => setConfirmOpen(false)}
        onConfirm={handleConfirmDelete}
      />
    </Box>
  );
};

export default ProjectsPage;
