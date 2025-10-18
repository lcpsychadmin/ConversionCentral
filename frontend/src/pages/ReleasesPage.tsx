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
import { useProjects } from '../hooks/useProjects';
import { useReleases } from '../hooks/useReleases';
import ReleaseTable, { ReleaseRow } from '../components/release/ReleaseTable';
import ReleaseForm from '../components/release/ReleaseForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { ReleaseFormValues } from '../types/data';

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const ReleasesPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

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
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Releases
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Plan releases and assign them to projects to track go-live milestones.
          </Typography>
        </div>
        {canManage && (
          <Button
            variant="contained"
            onClick={handleCreateClick}
            disabled={busy || noProjectsAvailable}
          >
            New Release
          </Button>
        )}
      </Stack>

      {noProjectsAvailable && (
        <Alert severity="info" sx={{ mb: 3 }}>
          Create a project before adding releases.
        </Alert>
      )}

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
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
              Project
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {projectLookup.get(selected.projectId)?.name ?? 'Unassigned'}
            </Typography>
            <Divider sx={{ my: 1.5 }} />
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
