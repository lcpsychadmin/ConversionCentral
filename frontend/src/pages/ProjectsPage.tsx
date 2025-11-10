import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import { useTheme } from '@mui/material/styles';

import { useAuth } from '../context/AuthContext';
import ProjectTable, { ProjectRow } from '../components/project/ProjectTable';
import ProjectForm from '../components/project/ProjectForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useProjects } from '../hooks/useProjects';
import { ProjectFormValues } from '../types/data';
import { getPanelSurface, getSectionSurface } from '../theme/surfaceStyles';
import PageHeader from '../components/common/PageHeader';

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const ProjectsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const sectionSurface = useMemo(() => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);
  const panelSurface = useMemo(() => getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);

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
      <PageHeader
        title="Projects"
        subtitle="Create high-level projects to organize releases and implementation milestones."
        actions={
          canManage ? (
            <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
              New Project
            </Button>
          ) : undefined
        }
      />

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      <Paper
        elevation={0}
        sx={{
          p: 3,
          mb: 3,
          borderRadius: 3,
          ...sectionSurface
        }}
      >
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
        <Paper
          elevation={0}
          sx={{
            p: 3,
            mb: 3,
            borderRadius: 3,
            ...panelSurface
          }}
        >
          <Typography
            variant="h5"
            gutterBottom
            sx={{ color: isDarkMode ? theme.palette.common.white : theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }}
          >
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
