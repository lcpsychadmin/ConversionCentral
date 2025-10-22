import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, CircularProgress, Grid, List, ListItem, ListItemSecondaryAction, ListItemText, Paper, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import ProcessAreaTable from '../components/process-area/ProcessAreaTable';
import ProcessAreaForm from '../components/process-area/ProcessAreaForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useProcessAreas } from '../hooks/useProcessAreas';
import { useAuth } from '../context/AuthContext';
import { useDataObjects } from '../hooks/useDataObjects';
const getErrorMessage = (error, fallback) => {
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
const formatStatusLabel = (status) => status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
const getStatusColor = (status) => {
    switch (status) {
        case 'active':
            return 'success';
        case 'draft':
            return 'warning';
        default:
            return 'default';
    }
};
const ProcessAreasPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const theme = useTheme();
    const { processAreasQuery, createProcessArea, updateProcessArea, deleteProcessArea, creating, updating, deleting } = useProcessAreas();
    const { data: processAreas = [], isLoading: processAreasLoading, isError: processAreasError, error: processAreasErrorDetails } = processAreasQuery;
    const { dataObjectsQuery } = useDataObjects();
    const { data: dataObjects = [], isLoading: dataObjectsLoading, isError: dataObjectsError, error: dataObjectsErrorDetails } = dataObjectsQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const rows = useMemo(() => [...processAreas], [processAreas]);
    const handleSelect = (processArea) => {
        setSelected(processArea);
    };
    const handleCreateClick = () => {
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (processArea) => {
        setFormMode('edit');
        setSelected(processArea);
        setFormOpen(true);
    };
    const handleDelete = (processArea) => {
        setSelected(processArea);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
        try {
            if (formMode === 'create') {
                await createProcessArea({
                    name: values.name,
                    description: values.description ?? null,
                    status: values.status
                });
            }
            else if (selected) {
                const payload = {
                    name: values.name,
                    description: values.description ?? null,
                    status: values.status
                };
                await updateProcessArea({ id: selected.id, input: payload });
            }
            setFormOpen(false);
        }
        catch (error) {
            // handled by hook toast helpers
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteProcessArea(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (error) {
            // handled by hook toast helpers
        }
    };
    const busy = creating || updating || deleting;
    const tableLoading = processAreasLoading;
    const processAreasErrorMessage = processAreasError
        ? getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.')
        : null;
    const dataObjectsErrorMessage = dataObjectsError
        ? getErrorMessage(dataObjectsErrorDetails, 'Unable to load data objects.')
        : null;
    const assignedDataObjects = useMemo(() => {
        if (!selected)
            return [];
        return dataObjects.filter((obj) => obj.processAreaId === selected.id);
    }, [selected, dataObjects]);
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Process Areas" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Manage process areas and review their related data objects." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New Process Area" }) })), dataObjectsErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: dataObjectsErrorMessage })), processAreasErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: processAreasErrorMessage })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(ProcessAreaTable, { data: rows, loading: tableLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), selected && (_jsxs(Paper, { elevation: 3, sx: { p: 3, mb: 3 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }, children: "Process Area Details" }), _jsxs(Grid, { container: true, spacing: 3, children: [_jsx(Grid, { item: true, xs: 12, md: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "Name", value: selected.name }), _jsx(DetailLine, { label: "Description", value: selected.description ?? '—' })] }) }), _jsx(Grid, { item: true, xs: 12, md: 6, children: _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), color: getStatusColor(selected.status) })] }) }), _jsxs(Grid, { item: true, xs: 12, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700 }, children: "Assigned Data Objects" }), dataObjectsLoading ? (_jsxs(Box, { display: "flex", alignItems: "center", gap: 1, children: [_jsx(CircularProgress, { size: 20 }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Loading data objects\u2026" })] })) : assignedDataObjects.length === 0 ? (_jsx(Alert, { severity: "info", children: "No data objects are linked to this process area yet." })) : (_jsx(List, { dense: true, children: assignedDataObjects.map((obj) => (_jsxs(ListItem, { disablePadding: true, children: [_jsx(ListItemText, { primary: obj.name, secondary: obj.description ?? 'No description' }), _jsx(ListItemSecondaryAction, { children: _jsx(Chip, { size: "small", label: obj.status }) })] }, obj.id))) }))] })] })] })), canManage && (_jsx(ProcessAreaForm, { open: formOpen, title: formMode === 'create' ? 'Create Process Area' : 'Edit Process Area', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Process Area", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: label }), _jsx(Typography, { variant: "body1", children: value })] }));
export default ProcessAreasPage;
