import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useEffect, useState } from 'react';
import { Box, Button, IconButton, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Tooltip, Dialog, DialogTitle, DialogContent, DialogActions, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import SaveIcon from '@mui/icons-material/Save';
import CancelIcon from '@mui/icons-material/Cancel';
import ErrorIcon from '@mui/icons-material/Error';
import { batchSaveConstructedData, deleteConstructedData, updateConstructedData } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';
const ConstructedDataGrid = ({ constructedTableId, fields, rows, onDataChange }) => {
    const theme = useTheme();
    const toast = useToast();
    // State
    const [rowStates, setRowStates] = useState(new Map(rows.map((row) => [
        row.id,
        {
            rowId: row.id,
            isDirty: false,
            isNew: false,
            data: row.payload,
            errors: []
        }
    ])));
    const [editingRowId, setEditingRowId] = useState(null);
    const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
    const [deleteRowId, setDeleteRowId] = useState(null);
    const [isSaving, setIsSaving] = useState(false);
    // Handlers
    useEffect(() => {
        setRowStates(new Map(rows.map((row) => [
            row.id,
            {
                rowId: row.id,
                isDirty: false,
                isNew: false,
                data: { ...row.payload },
                errors: []
            }
        ])));
        setEditingRowId(null);
    }, [rows]);
    const handleCellChange = useCallback((rowId, fieldName, value) => {
        setRowStates((prev) => {
            const newMap = new Map(prev);
            const state = newMap.get(rowId) || {
                rowId,
                isDirty: false,
                isNew: false,
                data: {},
                errors: []
            };
            state.data[fieldName] = value;
            state.isDirty = true;
            // Clear errors for this field when editing
            state.errors = state.errors.filter((e) => e.fieldName !== fieldName);
            newMap.set(rowId, state);
            return newMap;
        });
    }, []);
    const handleSaveRow = useCallback(async (rowId) => {
        const state = rowStates.get(rowId);
        if (!state)
            return;
        setIsSaving(true);
        try {
            // Validate the row using the batch validation pipeline
            const response = await batchSaveConstructedData(constructedTableId, {
                rows: [state.data],
                validateOnly: true
            });
            if (!response.success) {
                // Show validation errors
                setRowStates((prev) => {
                    const newMap = new Map(prev);
                    const updatedState = newMap.get(rowId);
                    if (updatedState) {
                        updatedState.errors = response.errors;
                    }
                    return newMap;
                });
                toast.showError('Validation failed. Please check errors below.');
                return;
            }
            await updateConstructedData(rowId, { payload: state.data });
            setRowStates((prev) => {
                const newMap = new Map(prev);
                const updatedState = newMap.get(rowId);
                if (updatedState) {
                    updatedState.isDirty = false;
                    updatedState.errors = [];
                }
                return newMap;
            });
            setEditingRowId(null);
            toast.showSuccess('Row saved successfully');
            onDataChange();
        }
        catch (error) {
            toast.showError(error.message || 'Failed to save row');
        }
        finally {
            setIsSaving(false);
        }
    }, [rowStates, toast, onDataChange]);
    const handleCancelEdit = useCallback((rowId) => {
        setRowStates((prev) => {
            const newMap = new Map(prev);
            const state = newMap.get(rowId);
            if (state) {
                // Reset to original data
                const originalRow = rows.find((r) => r.id === rowId);
                if (originalRow) {
                    state.data = originalRow.payload;
                    state.isDirty = false;
                    state.errors = [];
                }
            }
            return newMap;
        });
        setEditingRowId(null);
    }, [rows]);
    const handleDeleteClick = (rowId) => {
        setDeleteRowId(rowId);
        setDeleteConfirmOpen(true);
    };
    const handleDeleteConfirm = async () => {
        if (!deleteRowId)
            return;
        try {
            await deleteConstructedData(deleteRowId);
            setRowStates((prev) => {
                const newMap = new Map(prev);
                newMap.delete(deleteRowId);
                return newMap;
            });
            toast.showSuccess('Row deleted successfully');
            onDataChange();
        }
        catch (error) {
            toast.showError(error.message || 'Failed to delete row');
        }
        finally {
            setDeleteConfirmOpen(false);
            setDeleteRowId(null);
        }
    };
    const getFieldValue = (rowId, fieldName) => {
        const state = rowStates.get(rowId);
        return state?.data[fieldName] ?? '';
    };
    const getRowErrors = (rowId) => {
        return rowStates.get(rowId)?.errors || [];
    };
    const isRowDirty = (rowId) => {
        return rowStates.get(rowId)?.isDirty || false;
    };
    const isRowEditing = (rowId) => {
        return editingRowId === rowId;
    };
    return (_jsxs(_Fragment, { children: [_jsx(TableContainer, { sx: {
                    maxHeight: 'calc(100vh - 400px)',
                    border: 1,
                    borderColor: 'divider',
                    borderRadius: 1
                }, children: _jsxs(Table, { stickyHeader: true, children: [_jsx(TableHead, { children: _jsxs(TableRow, { sx: { backgroundColor: alpha(theme.palette.primary.main, 0.1) }, children: [_jsx(TableCell, { sx: { width: 100, fontWeight: 'bold' }, children: "Actions" }), fields.map((field) => (_jsxs(TableCell, { sx: { fontWeight: 'bold', minWidth: 150 }, children: [field.name, !field.isNullable && _jsx("span", { style: { color: 'red' }, children: "*" })] }, field.id)))] }) }), _jsx(TableBody, { children: Array.from(rowStates.values()).map((state) => {
                                const rowErrors = getRowErrors(state.rowId);
                                const hasErrors = rowErrors.length > 0;
                                return (_jsxs(TableRow, { sx: {
                                        backgroundColor: hasErrors
                                            ? alpha(theme.palette.error.main, 0.05)
                                            : state.isDirty
                                                ? alpha(theme.palette.warning.main, 0.05)
                                                : 'transparent',
                                        '&:hover': {
                                            backgroundColor: alpha(theme.palette.action.hover, 0.1)
                                        }
                                    }, children: [_jsx(TableCell, { sx: { width: 100 }, children: _jsx(Stack, { direction: "row", spacing: 0.5, children: isRowEditing(state.rowId) ? (_jsxs(_Fragment, { children: [_jsx(Tooltip, { title: "Save", children: _jsx(IconButton, { size: "small", color: "success", onClick: () => handleSaveRow(state.rowId), disabled: isSaving, children: _jsx(SaveIcon, { fontSize: "small" }) }) }), _jsx(Tooltip, { title: "Cancel", children: _jsx(IconButton, { size: "small", color: "inherit", onClick: () => handleCancelEdit(state.rowId), disabled: isSaving, children: _jsx(CancelIcon, { fontSize: "small" }) }) })] })) : (_jsxs(_Fragment, { children: [_jsx(Tooltip, { title: "Edit", children: _jsx(IconButton, { size: "small", color: "primary", onClick: () => setEditingRowId(state.rowId), children: hasErrors && _jsx(ErrorIcon, { fontSize: "small", sx: { color: 'error.main' } }) }) }), _jsx(Tooltip, { title: "Delete", children: _jsx(IconButton, { size: "small", color: "error", onClick: () => handleDeleteClick(state.rowId), children: _jsx(DeleteIcon, { fontSize: "small" }) }) })] })) }) }), fields.map((field) => {
                                            const fieldErrors = rowErrors.filter((e) => e.fieldName === field.name);
                                            const hasFieldError = fieldErrors.length > 0;
                                            return (_jsx(TableCell, { children: isRowEditing(state.rowId) ? (_jsxs(Box, { children: [_jsx(TextField, { size: "small", fullWidth: true, value: getFieldValue(state.rowId, field.name), onChange: (e) => handleCellChange(state.rowId, field.name, e.target.value), error: hasFieldError, type: field.dataType?.toLowerCase() === 'integer' ? 'number' : 'text', disabled: isSaving }), hasFieldError && (_jsx(Box, { sx: { mt: 0.5 }, children: fieldErrors.map((error) => (_jsxs(Box, { sx: {
                                                                    fontSize: '0.75rem',
                                                                    color: theme.palette.error.main,
                                                                    whiteSpace: 'normal'
                                                                }, children: ["\u2022 ", error.message] }, error.ruleId))) }))] })) : (_jsx(Box, { onClick: () => setEditingRowId(state.rowId), sx: {
                                                        cursor: 'pointer',
                                                        p: 1,
                                                        borderRadius: 1,
                                                        '&:hover': {
                                                            backgroundColor: alpha(theme.palette.action.hover, 0.1)
                                                        }
                                                    }, children: getFieldValue(state.rowId, field.name) })) }, `${state.rowId}-${field.id}`));
                                        })] }, state.rowId));
                            }) })] }) }), _jsxs(Dialog, { open: deleteConfirmOpen, onClose: () => setDeleteConfirmOpen(false), children: [_jsx(DialogTitle, { children: "Delete Row?" }), _jsx(DialogContent, { children: _jsx(Box, { sx: { mt: 2 }, children: _jsx(Typography, { children: "Are you sure you want to delete this row? This action cannot be undone." }) }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: () => setDeleteConfirmOpen(false), children: "Cancel" }), _jsx(Button, { onClick: handleDeleteConfirm, color: "error", variant: "contained", children: "Delete" })] })] })] }));
};
export default ConstructedDataGrid;
