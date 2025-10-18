import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Divider, Paper, Stack, Typography } from '@mui/material';
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
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", mb: 3, children: [_jsxs("div", { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Releases" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Plan releases and assign them to projects to track go-live milestones." })] }), canManage && (_jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy || noProjectsAvailable, children: "New Release" }))] }), noProjectsAvailable && (_jsx(Alert, { severity: "info", sx: { mb: 3 }, children: "Create a project before adding releases." })), _jsx(Paper, { elevation: 1, sx: { p: 2, mb: 3 }, children: _jsx(ReleaseTable, { data: rows, loading: releasesLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), selected && (_jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "h6", children: "Details" }), _jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Project" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: projectLookup.get(selected.projectId)?.name ?? 'Unassigned' }), _jsx(Divider, { sx: { my: 1.5 } }), _jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), size: "small", color: "primary" }), _jsx(Divider, { sx: { my: 1.5 } }), _jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Description" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: selected.description || 'No description provided.' })] }) })), _jsx(ReleaseForm, { open: formOpen, title: formMode === 'create' ? 'Create Release' : 'Edit Release', initialValues: formMode === 'edit' ? selected : null, projectOptions: projects, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit }), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Release", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", confirmColor: "error", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
export default ReleasesPage;
