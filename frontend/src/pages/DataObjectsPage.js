import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Chip, Grid, Paper, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DataObjectTable from '../components/data-object/DataObjectTable';
import DataObjectForm from '../components/data-object/DataObjectForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useDataObjects } from '../hooks/useDataObjects';
import { useProcessAreas } from '../hooks/useProcessAreas';
import { useSystems } from '../hooks/useSystems';
import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';
const findProcessAreaName = (processAreas, processAreaId) => {
    if (!processAreaId)
        return null;
    return processAreas.find((area) => area.id === processAreaId)?.name ?? null;
};
const getErrorMessage = (error, fallback) => {
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
const InventoryPage = () => {
    const { hasRole } = useAuth();
    const toast = useToast();
    const canManage = hasRole('admin');
    const theme = useTheme();
    const { dataObjectsQuery, createDataObject, updateDataObject, deleteDataObject, creating, updating, deleting } = useDataObjects();
    const { processAreasQuery } = useProcessAreas();
    const { data: processAreas = [], isLoading: processAreasLoading, isFetching: processAreasFetching, isError: processAreasError, error: processAreasErrorDetails } = processAreasQuery;
    const { data: dataObjects = [], isLoading: dataObjectsLoading, isError: dataObjectsError, error: dataObjectsErrorDetails } = dataObjectsQuery;
    const { systemsQuery } = useSystems();
    const { data: systems = [], isLoading: systemsLoading, isError: systemsError, error: systemsErrorDetails } = systemsQuery;
    const [selected, setSelected] = useState(null);
    const [formMode, setFormMode] = useState('create');
    const [formOpen, setFormOpen] = useState(false);
    const [confirmOpen, setConfirmOpen] = useState(false);
    const rows = useMemo(() => dataObjects.map((item) => ({
        ...item,
        processAreaName: findProcessAreaName(processAreas, item.processAreaId),
        systems: item.systems ?? [],
        systemNames: (item.systems ?? []).map((system) => system.name)
    })), [dataObjects, processAreas]);
    const handleSelect = (dataObject) => {
        setSelected(dataObject);
    };
    const handleCreateClick = () => {
        if (!canManage)
            return;
        if (processAreasLoading || processAreasFetching || systemsLoading) {
            toast.showInfo('Process areas or systems are still loading. Try again in a moment.');
            return;
        }
        if (processAreasError) {
            const message = getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.');
            toast.showError(message);
            return;
        }
        if (systemsError) {
            const message = getErrorMessage(systemsErrorDetails, 'Unable to load systems.');
            toast.showError(message);
            return;
        }
        if (processAreas.length === 0) {
            toast.showInfo('Create a process area before adding data objects.');
            return;
        }
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (dataObject) => {
        if (!canManage)
            return;
        setFormMode('edit');
        setSelected(dataObject);
        setFormOpen(true);
    };
    const handleDelete = (dataObject) => {
        if (!canManage)
            return;
        setSelected(dataObject);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
        try {
            if (formMode === 'create') {
                await createDataObject({
                    name: values.name,
                    description: values.description ?? null,
                    status: values.status,
                    processAreaId: values.processAreaId,
                    systemIds: values.systemIds
                });
            }
            else if (selected) {
                await updateDataObject({
                    id: selected.id,
                    input: {
                        name: values.name,
                        description: values.description ?? null,
                        status: values.status,
                        processAreaId: values.processAreaId,
                        systemIds: values.systemIds
                    }
                });
            }
            setFormOpen(false);
        }
        catch (error) {
            // handled by toast hook
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteDataObject(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (error) {
            // handled by toast hook
        }
    };
    const busy = creating || updating || deleting;
    const noProcessAreasAvailable = !processAreasLoading && !processAreasFetching && processAreas.length === 0;
    const dataObjectsErrorMessage = dataObjectsError
        ? getErrorMessage(dataObjectsErrorDetails, 'Unable to load data objects.')
        : null;
    const processAreasErrorMessage = processAreasError
        ? getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.')
        : null;
    const systemsErrorMessage = systemsError
        ? getErrorMessage(systemsErrorDetails, 'Unable to load systems.')
        : null;
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Inventory" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Manage data object inventory, process area alignment, and system ownership." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy, children: "New Data Object" }) })), dataObjectsErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: dataObjectsErrorMessage })), processAreasErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: processAreasErrorMessage })), systemsErrorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: systemsErrorMessage })), canManage && noProcessAreasAvailable && (_jsx(Alert, { severity: "info", sx: { mb: 3 }, children: "No process areas are available yet. Create a process area to enable new data objects." })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(DataObjectTable, { data: rows, loading: dataObjectsLoading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined }) }), selected && (_jsxs(Paper, { elevation: 3, sx: { p: 3, mb: 3 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 700, mb: 2.5 }, children: "Data Object Details" }), _jsxs(Grid, { container: true, spacing: 3, children: [_jsx(Grid, { item: true, xs: 12, md: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "Name", value: selected.name }), _jsx(DetailLine, { label: "Domain", value: selected.processAreaName ?? 'Unassigned' }), _jsx(DetailLine, { label: "Description", value: selected.description ?? '—' })] }) }), _jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: "Status" }), _jsx(Chip, { label: selected.status, color: selected.status === 'active' ? 'success' : 'default', sx: { mb: 2 } })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 1 }, children: "Systems" }), (selected.systems?.length ?? 0) === 0 ? (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "No systems assigned." })) : (_jsx(Stack, { direction: "row", spacing: 1, flexWrap: "wrap", useFlexGap: true, children: selected.systems?.map((system) => (_jsx(Chip, { label: system.name, size: "small" }, system.id))) }))] })] })] })] })), canManage && (_jsx(DataObjectForm, { open: formOpen, title: formMode === 'create' ? 'Create Data Object' : 'Edit Data Object', initialValues: formMode === 'edit' ? selected : null, loading: busy, onClose: handleFormClose, onSubmit: handleFormSubmit, processAreas: processAreas, systems: systems })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Data Object", description: `Are you sure you want to delete "${selected?.name ?? ''}"? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete })] }));
};
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: label }), _jsx(Typography, { variant: "body1", children: value })] }));
export default InventoryPage;
