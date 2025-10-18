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
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Projects
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Create high-level projects to organize releases and implementation milestones.
          </Typography>
        </div>
        {canManage && (
          <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
            New Project
          </Button>
        )}
      </Stack>

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
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

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {selected && (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Stack spacing={1}>
            <Typography variant="h6">Details</Typography>
            <Typography variant="subtitle2" color="text.secondary">
              Status
            </Typography>
            <Chip label={formatStatusLabel(selected.status)} size="small" color="primary" />
            <Divider sx={{ my: 1.5 }} />
            <Typography variant="subtitle2" color="text.secondary">
              Description
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {selected.description || 'No description provided.'}
            </Typography>
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
