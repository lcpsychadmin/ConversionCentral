import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { LoadingButton } from '@mui/lab';
import { Alert, Autocomplete, Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, IconButton, Stack, TextField, Typography } from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import PostAddIcon from '@mui/icons-material/PostAdd';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { createTable, updateTable } from '../../services/tableService';
import { useToast } from '../../hooks/useToast';
import ConfirmDialog from '../common/ConfirmDialog';
import CreateTableDialog from './CreateTableDialog';
const generateId = () => Math.random().toString(36).slice(2, 11);
const getNextLoadOrderValue = (rows) => {
    const numericValues = rows
        .map((row) => Number(row.loadOrder))
        .filter((value) => Number.isInteger(value) && value > 0);
    const next = numericValues.length ? Math.max(...numericValues) + 1 : 1;
    return next.toString();
};
const getErrorMessage = (error, fallback) => {
    if (error instanceof AxiosError) {
        return error.response?.data?.detail ?? error.message;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
const sanitizeOptionalString = (value) => {
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
};
const snapshotFromDefinition = (definition) => ({
    description: definition?.description ?? '',
    tables: definition?.tables.map((table, index) => ({
        tableId: table.tableId,
        alias: table.alias ?? '',
        description: table.description ?? '',
        loadOrder: (table.loadOrder ?? index + 1).toString(),
        fields: table.fields.map((field) => ({
            fieldId: field.fieldId,
            notes: field.notes ?? ''
        }))
    })) ?? []
});
const normalizeSnapshot = (snapshot) => ({
    description: snapshot.description.trim(),
    tables: snapshot.tables.map((table) => ({
        tableId: table.tableId,
        alias: table.alias.trim(),
        description: table.description.trim(),
        loadOrder: table.loadOrder.trim(),
        fields: table.fields.map((field) => ({
            fieldId: field.fieldId,
            notes: field.notes.trim()
        }))
    }))
});
const buildRows = (tables) => tables.map((table) => ({
    id: generateId(),
    tableId: table.tableId,
    alias: table.alias,
    description: table.description,
    loadOrder: table.loadOrder,
    fields: table.fields.map((field) => ({
        id: generateId(),
        fieldId: field.fieldId,
        notes: field.notes
    }))
}));
const DataDefinitionForm = ({ open, mode, loading = false, onClose, onSubmit, initialDefinition, tables, systemId, onMetadataRefresh }) => {
    const toast = useToast();
    const initialSnapshot = useMemo(() => snapshotFromDefinition(initialDefinition), [initialDefinition]);
    const normalizedInitial = useMemo(() => normalizeSnapshot(initialSnapshot), [initialSnapshot]);
    const [description, setDescription] = useState(initialSnapshot.description);
    const [tableRows, setTableRows] = useState(buildRows(initialSnapshot.tables));
    const [formError, setFormError] = useState(null);
    const [localTables, setLocalTables] = useState([]);
    const [tableDialogOpen, setTableDialogOpen] = useState(false);
    const [tableDialogMode, setTableDialogMode] = useState('create');
    const [editingTable, setEditingTable] = useState(null);
    const [tableDialogLoading, setTableDialogLoading] = useState(false);
    const [existingTablePrompt, setExistingTablePrompt] = useState(null);
    useEffect(() => {
        if (!open)
            return;
        setDescription(initialSnapshot.description);
        setTableRows(buildRows(initialSnapshot.tables));
        setFormError(null);
    }, [initialSnapshot, open]);
    useEffect(() => {
        setLocalTables((prev) => prev.filter((table) => !tables.some((item) => item.id === table.id)));
    }, [tables]);
    const combinedTables = useMemo(() => {
        const map = new Map();
        [...tables, ...localTables].forEach((table) => {
            map.set(table.id, table);
        });
        return Array.from(map.values());
    }, [tables, localTables]);
    const normalizedCurrent = useMemo(() => ({
        description,
        tables: tableRows.map((table) => ({
            tableId: table.tableId,
            alias: table.alias,
            description: table.description,
            loadOrder: table.loadOrder,
            fields: table.fields.map((field) => ({
                fieldId: field.fieldId,
                notes: field.notes
            }))
        }))
    }), [description, tableRows]);
    const normalizedCurrentTrimmed = useMemo(() => normalizeSnapshot(normalizedCurrent), [normalizedCurrent]);
    const isDirty = useMemo(() => {
        return JSON.stringify(normalizedCurrentTrimmed) !== JSON.stringify(normalizedInitial);
    }, [normalizedCurrentTrimmed, normalizedInitial]);
    const handleClose = () => {
        setDescription(initialSnapshot.description);
        setTableRows(buildRows(initialSnapshot.tables));
        setFormError(null);
        setTableDialogOpen(false);
        setTableDialogMode('create');
        setEditingTable(null);
        onClose();
    };
    const addExistingTableToDefinition = (existingTable, sanitizedDescription) => {
        setTableRows((prev) => {
            const emptyIndex = prev.findIndex((row) => !row.tableId);
            const fallbackDescription = existingTable.description ?? sanitizedDescription ?? '';
            if (emptyIndex !== -1) {
                const updated = [...prev];
                const target = updated[emptyIndex];
                updated[emptyIndex] = {
                    ...target,
                    tableId: existingTable.id,
                    alias: target.alias || existingTable.name,
                    description: target.description || existingTable.description || fallbackDescription,
                    loadOrder: target.loadOrder,
                    fields: []
                };
                return updated;
            }
            return [
                ...prev,
                {
                    id: generateId(),
                    tableId: existingTable.id,
                    alias: existingTable.name,
                    description: existingTable.description ?? fallbackDescription,
                    loadOrder: getNextLoadOrderValue(prev),
                    fields: []
                }
            ];
        });
    };
    const handleExistingTableConfirm = () => {
        if (!existingTablePrompt)
            return;
        addExistingTableToDefinition(existingTablePrompt.table, existingTablePrompt.sanitized.description);
        toast.showSuccess('Existing table added to the data definition.');
        setExistingTablePrompt(null);
        setTableDialogOpen(false);
        setTableDialogMode('create');
        setEditingTable(null);
    };
    const handleExistingTableCancel = () => {
        setExistingTablePrompt(null);
    };
    const handleAddTable = () => {
        setTableRows((prev) => {
            const nextLoadOrder = getNextLoadOrderValue(prev);
            return [
                ...prev,
                {
                    id: generateId(),
                    tableId: '',
                    alias: '',
                    description: '',
                    loadOrder: nextLoadOrder,
                    fields: []
                }
            ];
        });
    };
    const handleRemoveTable = (id) => {
        setTableRows((prev) => prev.filter((table) => table.id !== id));
    };
    const handleTableChange = (rowId, table) => {
        setTableRows((prev) => prev.map((row) => {
            if (row.id !== rowId)
                return row;
            const nextAlias = row.alias || table?.name || '';
            return {
                ...row,
                tableId: table?.id ?? '',
                alias: table ? nextAlias : '',
                description: table ? row.description || table.description || '' : row.description,
                fields: [],
                loadOrder: row.loadOrder
            };
        }));
    };
    const handleOpenCreateTable = () => {
        setFormError(null);
        setTableDialogMode('create');
        setEditingTable(null);
        setTableDialogOpen(true);
    };
    const handleOpenEditTable = (row) => {
        if (!row.tableId) {
            toast.showError('Select a table before editing.');
            return;
        }
        const tableMeta = combinedTables.find((table) => table.id === row.tableId);
        if (!tableMeta) {
            toast.showError('Unable to load table metadata for editing.');
            return;
        }
        setFormError(null);
        setEditingTable(tableMeta);
        setTableDialogMode('edit');
        setTableDialogOpen(true);
    };
    const handleTableDialogSubmit = async (values) => {
        const sanitized = {
            name: values.name.trim(),
            physicalName: values.physicalName.trim(),
            schemaName: sanitizeOptionalString(values.schemaName),
            description: sanitizeOptionalString(values.description),
            tableType: sanitizeOptionalString(values.tableType),
            status: values.status.trim() || 'active'
        };
        if (tableDialogMode !== 'edit') {
            const existingTable = combinedTables.find((table) => table.systemId === systemId && (table.physicalName ?? '').toLowerCase() === sanitized.physicalName.toLowerCase());
            if (existingTable) {
                const alreadyLinked = tableRows.some((row) => row.tableId === existingTable.id);
                if (alreadyLinked) {
                    toast.showInfo('This table is already part of the data definition.');
                    setTableDialogOpen(false);
                    setTableDialogMode('create');
                    setEditingTable(null);
                }
                else {
                    setExistingTablePrompt({ table: existingTable, sanitized });
                }
                return;
            }
        }
        setTableDialogLoading(true);
        try {
            if (tableDialogMode === 'edit' && editingTable) {
                const updatedTable = await updateTable(editingTable.id, {
                    name: sanitized.name,
                    physicalName: sanitized.physicalName,
                    schemaName: sanitized.schemaName,
                    description: sanitized.description,
                    tableType: sanitized.tableType,
                    status: sanitized.status
                });
                toast.showSuccess('Table updated.');
                setLocalTables((prev) => [...prev.filter((table) => table.id !== updatedTable.id), updatedTable]);
                if (onMetadataRefresh) {
                    await onMetadataRefresh();
                }
                setTableDialogOpen(false);
                setTableDialogMode('create');
                setEditingTable(null);
                return;
            }
            const payload = {
                systemId,
                name: sanitized.name,
                physicalName: sanitized.physicalName,
                schemaName: sanitized.schemaName,
                description: sanitized.description,
                tableType: sanitized.tableType,
                status: sanitized.status
            };
            const newTable = await createTable(payload);
            toast.showSuccess('Table created.');
            setLocalTables((prev) => [...prev.filter((table) => table.id !== newTable.id), newTable]);
            if (onMetadataRefresh) {
                await onMetadataRefresh();
            }
            setTableRows((prev) => {
                const emptyIndex = prev.findIndex((row) => !row.tableId);
                if (emptyIndex !== -1) {
                    const updated = [...prev];
                    const target = updated[emptyIndex];
                    const fallbackDescription = sanitized.description ?? '';
                    updated[emptyIndex] = {
                        ...target,
                        tableId: newTable.id,
                        alias: target.alias || newTable.name,
                        description: target.description || newTable.description || fallbackDescription,
                        loadOrder: target.loadOrder,
                        fields: []
                    };
                    return updated;
                }
                return [
                    ...prev,
                    {
                        id: generateId(),
                        tableId: newTable.id,
                        alias: newTable.name,
                        description: newTable.description ?? sanitized.description ?? '',
                        loadOrder: getNextLoadOrderValue(prev),
                        fields: []
                    }
                ];
            });
            setTableDialogOpen(false);
            setTableDialogMode('create');
            setEditingTable(null);
        }
        catch (error) {
            toast.showError(getErrorMessage(error, tableDialogMode === 'edit' ? 'Unable to update table.' : 'Unable to create table.'));
        }
        finally {
            setTableDialogLoading(false);
        }
    };
    const handleTableDialogClose = () => {
        if (tableDialogLoading)
            return;
        setTableDialogOpen(false);
        setTableDialogMode('create');
        setEditingTable(null);
    };
    const validate = (tablesPayload) => {
        if (!tablesPayload.length) {
            return 'Add at least one table to the data definition.';
        }
        const seenTables = new Set();
        const seenLoadOrders = new Set();
        for (const table of tablesPayload) {
            if (!table.tableId) {
                return 'Every entry must select a table.';
            }
            if (seenTables.has(table.tableId)) {
                return 'Tables must be unique within the data definition.';
            }
            seenTables.add(table.tableId);
            const loadOrderValue = table.loadOrder.trim();
            if (!loadOrderValue) {
                return 'Each table must include a load order.';
            }
            const loadOrderNumber = Number(loadOrderValue);
            if (!Number.isInteger(loadOrderNumber) || loadOrderNumber <= 0) {
                return 'Table load order must be a positive whole number.';
            }
            if (seenLoadOrders.has(loadOrderNumber)) {
                return 'Table load order values must be unique.';
            }
            seenLoadOrders.add(loadOrderNumber);
        }
        return null;
    };
    const handleSubmit = (event) => {
        event.preventDefault();
        const validationError = validate(tableRows);
        if (validationError) {
            setFormError(validationError);
            return;
        }
        const payloadTables = tableRows.map((table) => ({
            tableId: table.tableId,
            alias: table.alias.trim() || null,
            description: table.description.trim() || null,
            loadOrder: Number(table.loadOrder.trim()),
            fields: table.fields.map((field) => ({
                fieldId: field.fieldId,
                notes: field.notes.trim() || null
            }))
        }));
        onSubmit({
            description: description.trim() || null,
            tables: payloadTables
        });
    };
    const tablesAvailable = combinedTables.length > 0;
    return (_jsxs(_Fragment, { children: [_jsx(Dialog, { open: open, onClose: handleClose, maxWidth: "md", fullWidth: true, children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: mode === 'create' ? 'Create Data Definition' : 'Edit Data Definition' }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 3, mt: 1, children: [!tablesAvailable && (_jsx(Alert, { severity: "warning", children: "No tables are available for the selected system yet. Use the Create Table action below to add one before building the data definition." })), _jsx(TextField, { label: "Description", value: description, onChange: (event) => setDescription(event.target.value), multiline: true, minRows: 2, fullWidth: true }), _jsxs(Stack, { spacing: 2, children: [tableRows.map((row) => {
                                                const selectedTable = combinedTables.find((table) => table.id === row.tableId) ?? null;
                                                return (_jsx(Box, { sx: { border: '1px solid', borderColor: 'divider', borderRadius: 1, p: 2 }, children: _jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", children: [_jsx(Autocomplete, { fullWidth: true, options: combinedTables, value: selectedTable, disabled: !tablesAvailable, onChange: (_, value) => handleTableChange(row.id, value), getOptionLabel: (option) => option.name, isOptionEqualToValue: (option, value) => option.id === value.id, renderInput: (params) => _jsx(TextField, { ...params, label: "Table", placeholder: "Select table", required: true }) }), _jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", children: [_jsx(IconButton, { "aria-label": "Edit table", onClick: () => handleOpenEditTable(row), disabled: loading || !row.tableId, children: _jsx(EditOutlinedIcon, {}) }), _jsx(IconButton, { "aria-label": "Remove table", onClick: () => handleRemoveTable(row.id), disabled: loading, children: _jsx(DeleteOutlineIcon, {}) })] })] }), _jsxs(Stack, { spacing: 2, direction: { xs: 'column', md: 'row' }, alignItems: { md: 'flex-start' }, children: [_jsx(TextField, { label: "Alias", value: row.alias, onChange: (event) => setTableRows((prev) => prev.map((table) => table.id === row.id ? { ...table, alias: event.target.value } : table)), fullWidth: true }), _jsx(TextField, { label: "Table Description", value: row.description, onChange: (event) => setTableRows((prev) => prev.map((table) => table.id === row.id ? { ...table, description: event.target.value } : table)), fullWidth: true }), _jsx(TextField, { label: "Load Order", value: row.loadOrder, onChange: (event) => setTableRows((prev) => prev.map((table) => table.id === row.id
                                                                            ? {
                                                                                ...table,
                                                                                loadOrder: event.target.value.replace(/[^0-9]/g, '')
                                                                            }
                                                                            : table)), type: "number", inputProps: { min: 1 }, required: true, sx: { width: { xs: '100%', md: 160 } } })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle1", gutterBottom: true, children: "Fields" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: row.fields.length > 0
                                                                            ? `${row.fields.length} field${row.fields.length === 1 ? '' : 's'} currently associated. Manage fields from the Data Definition screen.`
                                                                            : 'Fields can be added after saving from the Data Definition page.' })] })] }) }, row.id));
                                            }), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 1, children: [_jsx(Button, { variant: "outlined", startIcon: _jsx(AddCircleOutlineIcon, {}), onClick: handleAddTable, disabled: loading || !tablesAvailable, children: "Add Table" }), _jsx(Button, { variant: "contained", startIcon: _jsx(PostAddIcon, {}), onClick: handleOpenCreateTable, disabled: loading, children: "Create Table" })] })] }), formError && _jsx(Alert, { severity: "error", children: formError })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || !isDirty, children: "Save" })] })] }) }), _jsx(CreateTableDialog, { open: tableDialogOpen, loading: tableDialogLoading, mode: tableDialogMode, initialValues: editingTable
                    ? {
                        name: editingTable.name,
                        physicalName: editingTable.physicalName,
                        schemaName: editingTable.schemaName ?? null,
                        description: editingTable.description ?? null,
                        tableType: editingTable.tableType ?? null,
                        status: editingTable.status ?? 'active'
                    }
                    : undefined, onClose: handleTableDialogClose, onSubmit: handleTableDialogSubmit }), existingTablePrompt && (_jsx(ConfirmDialog, { open: Boolean(existingTablePrompt), title: "Use existing table?", description: `A table named "${existingTablePrompt.table.name}" (physical name "${existingTablePrompt.table.physicalName ?? existingTablePrompt.sanitized.physicalName}") already exists in this system. Would you like to add it to the data definition instead of creating a new table?`, confirmLabel: "Add Existing Table", cancelLabel: "Keep Editing", confirmColor: "primary", onClose: handleExistingTableCancel, onConfirm: handleExistingTableConfirm }))] }));
};
export default DataDefinitionForm;
