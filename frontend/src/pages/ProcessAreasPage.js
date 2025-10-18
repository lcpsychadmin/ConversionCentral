import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, CircularProgress, Divider, Grid, List, ListItem, ListItemSecondaryAction, ListItemText, Paper, Stack, Typography } from '@mui/material';
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
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", mb: 3, children: [_jsxs("div", { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Process Areas" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Manage process areas and review their related data objects." })] }), canManage && (_jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New Process Area" }))] }), _jsx(Paper, { elevation: 1, sx: { p: 2, mb: 3 }, children: _jsx(ProcessAreaTable, { data: rows, loading: tableLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), dataObjectsErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: dataObjectsErrorMessage })), processAreasErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: processAreasErrorMessage })), selected && (_jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsxs(Grid, { container: true, spacing: 2, children: [_jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Details" }), _jsxs(Stack, { spacing: 1, children: [_jsx(DetailLine, { label: "Name", value: selected.name }), _jsx(DetailLine, { label: "Description", value: selected.description ?? '—' })] })] }), _jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Status" }), _jsx(Chip, { label: formatStatusLabel(selected.status), color: getStatusColor(selected.status) })] }), _jsxs(Grid, { item: true, xs: 12, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Assigned Data Objects" }), dataObjectsLoading ? (_jsxs(Box, { display: "flex", alignItems: "center", gap: 1, children: [_jsx(CircularProgress, { size: 20 }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Loading data objects\u2026" })] })) : assignedDataObjects.length === 0 ? (_jsx(Alert, { severity: "info", children: "No data objects are linked to this process area yet." })) : (_jsx(List, { dense: true, children: assignedDataObjects.map((obj) => (_jsxs(ListItem, { disablePadding: true, children: [_jsx(ListItemText, { primary: obj.name, secondary: obj.description ?? 'No description' }), _jsx(ListItemSecondaryAction, { children: _jsx(Chip, { size: "small", label: obj.status }) })] }, obj.id))) }))] })] }) })), canManage && (_jsx(ProcessAreaForm, { open: formOpen, title: formMode === 'create' ? 'Create Process Area' : 'Edit Process Area', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Process Area", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: label }), _jsx(Typography, { variant: "body1", children: value }), _jsx(Divider, { sx: { my: 1.5 } })] }));
export default ProcessAreasPage;
