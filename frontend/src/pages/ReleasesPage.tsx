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
import { useProjects } from '../hooks/useProjects';
import { useReleases } from '../hooks/useReleases';
import ReleaseTable, { ReleaseRow } from '../components/release/ReleaseTable';
import ReleaseForm from '../components/release/ReleaseForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { ReleaseFormValues } from '../types/data';
import { getPanelSurface, getSectionSurface } from '../theme/surfaceStyles';
import PageHeader from '../components/common/PageHeader';

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const ReleasesPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const sectionSurface = useMemo(() => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);
  const panelSurface = useMemo(() => getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);

  const { projectsQuery } = useProjects();
  const { data: projects = [], isLoading: projectsLoading } = projectsQuery;

  const {
    releasesQuery,
    createRelease,
    updateRelease,
    deleteRelease,
    creating,
    updating,
    deleting
  } = useReleases();

  const {
    data: releases = [],
    isLoading: releasesLoading,
    isError,
    error
  } = releasesQuery;

  const [selected, setSelected] = useState<ReleaseRow | null>(null);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [formOpen, setFormOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const projectLookup = useMemo(() => {
    return new Map(projects.map((project) => [project.id, project]));
  }, [projects]);

  const rows = useMemo<ReleaseRow[]>(
    () =>
      releases.map((release) => ({
        ...release,
        projectName: release.projectName ?? projectLookup.get(release.projectId)?.name ?? null
      })),
    [releases, projectLookup]
  );

  const handleSelect = (release: ReleaseRow | null) => {
    setSelected(release);
  };

  const handleCreateClick = () => {
    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (release: ReleaseRow) => {
    setFormMode('edit');
    setSelected(release);
    setFormOpen(true);
  };

  const handleDelete = (release: ReleaseRow) => {
    setSelected(release);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: ReleaseFormValues) => {
    try {
      if (formMode === 'create') {
        await createRelease(values);
      } else if (selected) {
        await updateRelease({ id: selected.id, input: values });
      }
      setFormOpen(false);
    } catch (err) {
      // handled in hook
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteRelease(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (err) {
      // handled in hook
    }
  };

  const busy = creating || updating || deleting;
  const errorMessage = isError && error instanceof Error ? error.message : null;
  const noProjectsAvailable = !projectsLoading && projects.length === 0;

  return (
    <Box>
      <PageHeader
        title="Releases"
        subtitle="Plan releases and assign them to projects to track go-live milestones."
        actions={
          canManage ? (
            <Button
              variant="contained"
              onClick={handleCreateClick}
              disabled={busy || noProjectsAvailable}
            >
              New Release
            </Button>
          ) : undefined
        }
      />

      {noProjectsAvailable && (
        <Alert severity="info" sx={{ mb: 3 }}>
          Create a project before adding releases.
        </Alert>
      )}

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
        <ReleaseTable
          data={rows}
          loading={releasesLoading}
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
            Release Details
          </Typography>
          <Stack spacing={2}>
            <Box>
              <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
                Project
              </Typography>
              <Typography variant="body1">
                {projectLookup.get(selected.projectId)?.name ?? 'Unassigned'}
              </Typography>
            </Box>
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

      <ReleaseForm
        open={formOpen}
        title={formMode === 'create' ? 'Create Release' : 'Edit Release'}
        initialValues={formMode === 'edit' ? selected : null}
        projectOptions={projects}
        loading={busy}
        onClose={handleFormClose}
        onSubmit={handleFormSubmit}
      />

      <ConfirmDialog
        open={confirmOpen}
        title="Delete Release"
        description={`Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`}
        confirmLabel="Delete"
        confirmColor="error"
        onClose={() => setConfirmOpen(false)}
        onConfirm={handleConfirmDelete}
      />
    </Box>
  );
};

export default ReleasesPage;
