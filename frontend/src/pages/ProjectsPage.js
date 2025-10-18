import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Divider, Paper, Stack, Typography } from '@mui/material';
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
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", mb: 3, children: [_jsxs("div", { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Projects" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Create high-level projects to organize releases and implementation milestones." })] }), canManage && (_jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New Project" }))] }), _jsx(Paper, { elevation: 1, sx: { p: 2, mb: 3 }, children: _jsx(ProjectTable, { data: rows, loading: isLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), selected && (_jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "h6", children: "Details" }), _jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), size: "small", color: "primary" }), _jsx(Divider, { sx: { my: 1.5 } }), _jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Description" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: selected.description || 'No description provided.' })] }) })), _jsx(ProjectForm, { open: formOpen, title: formMode === 'create' ? 'Create Project' : 'Edit Project', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit }), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Project", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", confirmColor: "error", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
export default ProjectsPage;
