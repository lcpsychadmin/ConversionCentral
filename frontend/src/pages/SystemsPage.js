import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Grid, Paper, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
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
    const theme = useTheme();
    const { systemsQuery, createSystem, updateSystem, deleteSystem, creating, updating, deleting } = useSystems();
    const { data: systems = [], isLoading, isError, error } = systemsQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const sortedSystems = useMemo(() => systems.slice().sort((a, b) => (a.name ?? '').localeCompare(b.name ?? '')), [systems]);
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
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Systems" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Maintain the catalog of applications and systems available for data objects." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New System" }) })), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(SystemTable, { data: sortedSystems, loading: isLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), selected && (_jsxs(Paper, { elevation: 3, sx: { p: 3, mb: 3 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }, children: "System Details" }), _jsxs(Grid, { container: true, spacing: 3, children: [_jsx(Grid, { item: true, xs: 12, md: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "Name", value: selected.name }), _jsx(DetailLine, { label: "Physical Name", value: selected.physicalName }), _jsx(DetailLine, { label: "Type", value: selected.systemType ?? '—' })] }) }), _jsx(Grid, { item: true, xs: 12, md: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "Status", value: selected.status, isChip: true, chipColor: selected.status === 'active' ? 'success' : 'default' }), _jsx(DetailLine, { label: "Description", value: selected.description ?? '—' }), _jsx(DetailLine, { label: "Security Classification", value: selected.securityClassification ?? '—' })] }) })] })] })), canManage && (_jsx(SystemForm, { open: formOpen, title: formMode === 'create' ? 'Create System' : 'Edit System', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete System", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
const DetailLine = ({ label, value, isChip = false, chipColor = 'default' }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: label }), isChip ? (_jsx(Chip, { label: value, color: chipColor, size: "small" })) : (_jsx(Typography, { variant: "body1", children: value }))] }));
export default SystemsPage;
