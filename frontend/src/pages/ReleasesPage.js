import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Paper, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { useAuth } from '../context/AuthContext';
import { useProjects } from '../hooks/useProjects';
import { useReleases } from '../hooks/useReleases';
import ReleaseTable from '../components/release/ReleaseTable';
import ReleaseForm from '../components/release/ReleaseForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
const formatStatusLabel = (status) => status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
const ReleasesPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const theme = useTheme();
    const { projectsQuery } = useProjects();
    const { data: projects = [], isLoading: projectsLoading } = projectsQuery;
    const { releasesQuery, createRelease, updateRelease, deleteRelease, creating, updating, deleting } = useReleases();
    const { data: releases = [], isLoading: releasesLoading, isError, error } = releasesQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const projectLookup = useMemo(() => {
        return new Map(projects.map((project) => [project.id, project]));
    }, [projects]);
    const rows = useMemo(() => releases.map((release) => ({
        ...release,
        projectName: release.projectName ?? projectLookup.get(release.projectId)?.name ?? null
    })), [releases, projectLookup]);
    const handleSelect = (release) => {
        setSelected(release);
    };
    const handleCreateClick = () => {
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (release) => {
        setFormMode('edit');
        setSelected(release);
        setFormOpen(true);
    };
    const handleDelete = (release) => {
        setSelected(release);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
        try {
            if (formMode === 'create') {
                await createRelease(values);
            }
            else if (selected) {
                await updateRelease({ id: selected.id, input: values });
            }
            setFormOpen(false);
        }
        catch (err) {
            // handled in hook
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteRelease(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (err) {
            // handled in hook
        }
    };
    const busy = creating || updating || deleting;
    const errorMessage = isError && error instanceof Error ? error.message : null;
    const noProjectsAvailable = !projectsLoading && projects.length === 0;
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Releases" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Plan releases and assign them to projects to track go-live milestones." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy || noProjectsAvailable, children: "New Release" }) })), noProjectsAvailable && (_jsx(Alert, { severity: "info", sx: { mb: 3 }, children: "Create a project before adding releases." })), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(ReleaseTable, { data: rows, loading: releasesLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), selected && (_jsxs(Paper, { elevation: 3, sx: { p: 3, mb: 3 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }, children: "Release Details" }), _jsxs(Stack, { spacing: 2, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Project" }), _jsx(Typography, { variant: "body1", children: projectLookup.get(selected.projectId)?.name ?? 'Unassigned' })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), size: "small", color: "primary" })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Description" }), _jsx(Typography, { variant: "body1", children: selected.description || 'No description provided.' })] })] })] })), _jsx(ReleaseForm, { open: formOpen, title: formMode === 'create' ? 'Create Release' : 'Edit Release', initialValues: formMode === 'edit' ? selected : null, projectOptions: projects, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit }), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Release", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", confirmColor: "error", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
export default ReleasesPage;
