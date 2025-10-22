import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Paper, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { useAuth } from '../context/AuthContext';
import ProjectTable from '../components/project/ProjectTable';
import ProjectForm from '../components/project/ProjectForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useProjects } from '../hooks/useProjects';
const formatStatusLabel = (status) => status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
const ProjectsPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const theme = useTheme();
    const { projectsQuery, createProject, updateProject, deleteProject, creating, updating, deleting } = useProjects();
    const { data: projects = [], isLoading, isError, error } = projectsQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const rows = useMemo(() => [...projects], [projects]);
    const handleSelect = (project) => {
        setSelected(project);
    };
    const handleCreateClick = () => {
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (project) => {
        setFormMode('edit');
        setSelected(project);
        setFormOpen(true);
    };
    const handleDelete = (project) => {
        setSelected(project);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
        try {
            if (formMode === 'create') {
                await createProject(values);
            }
            else if (selected) {
                await updateProject({ id: selected.id, input: values });
            }
            setFormOpen(false);
        }
        catch (err) {
            // errors handled via hooks/toasts
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteProject(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (err) {
            // handled in hook
        }
    };
    const busy = creating || updating || deleting;
    const errorMessage = isError && error instanceof Error ? error.message : null;
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Projects" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Create high-level projects to organize releases and implementation milestones." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New Project" }) })), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(ProjectTable, { data: rows, loading: isLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), selected && (_jsxs(Paper, { elevation: 3, sx: { p: 3, mb: 3 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }, children: "Project Details" }), _jsxs(Stack, { spacing: 2, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), size: "small", color: "primary" })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Description" }), _jsx(Typography, { variant: "body1", children: selected.description || 'No description provided.' })] })] })] })), _jsx(ProjectForm, { open: formOpen, title: formMode === 'create' ? 'Create Project' : 'Edit Project', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit }), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Project", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", confirmColor: "error", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
export default ProjectsPage;
