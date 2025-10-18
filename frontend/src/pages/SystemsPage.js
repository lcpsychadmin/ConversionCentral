import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Divider, Grid, Paper, Stack, Typography } from '@mui/material';
import SystemTable from '../components/system/SystemTable';
import SystemForm from '../components/system/SystemForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { useAuth } from '../context/AuthContext';
const getErrorMessage = (error, fallback) => {
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
const SystemsPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const { systemsQuery, createSystem, updateSystem, deleteSystem, creating, updating, deleting } = useSystems();
    const { data: systems = [], isLoading, isError, error } = systemsQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const sortedSystems = useMemo(() => systems.slice().sort((a, b) => a.name.localeCompare(b.name)), [systems]);
    const handleSelect = (system) => {
        setSelected(system);
    };
    const handleCreateClick = () => {
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (system) => {
        setFormMode('edit');
        setSelected(system);
        setFormOpen(true);
    };
    const handleDelete = (system) => {
        setSelected(system);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
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
            }
            else if (selected) {
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
        }
        catch (err) {
            // notifications handled in hook
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteSystem(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (err) {
            // notifications handled in hook
        }
    };
    const busy = creating || updating || deleting;
    const errorMessage = isError ? getErrorMessage(error, 'Unable to load systems.') : null;
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", mb: 3, children: [_jsxs("div", { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Systems" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Maintain the catalog of applications and systems available for data objects." })] }), canManage && (_jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New System" }))] }), _jsx(Paper, { elevation: 1, sx: { p: 2, mb: 3 }, children: _jsx(SystemTable, { data: sortedSystems, loading: isLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), selected && (_jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsxs(Grid, { container: true, spacing: 2, children: [_jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Details" }), _jsxs(Stack, { spacing: 1, children: [_jsx(DetailLine, { label: "Name", value: selected.name }), _jsx(DetailLine, { label: "Physical Name", value: selected.physicalName }), _jsx(DetailLine, { label: "Type", value: selected.systemType ?? '—' }), _jsx(DetailLine, { label: "Description", value: selected.description ?? '—' })] })] }), _jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Status" }), _jsx(Chip, { label: selected.status, color: selected.status === 'active' ? 'success' : 'default' }), _jsx(Divider, { sx: { my: 1.5 } }), _jsx(DetailLine, { label: "Security Classification", value: selected.securityClassification ?? '—' })] })] }) })), canManage && (_jsx(SystemForm, { open: formOpen, title: formMode === 'create' ? 'Create System' : 'Edit System', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete System", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: label }), _jsx(Typography, { variant: "body1", children: value }), _jsx(Divider, { sx: { my: 1.5 } })] }));
export default SystemsPage;
