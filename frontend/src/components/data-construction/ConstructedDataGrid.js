import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import { Box, IconButton, Tooltip, Dialog, DialogTitle, DialogContent, DialogActions, Typography, Button, Alert } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { DataGrid, GridCellEditStopReasons, useGridApiRef } from '@mui/x-data-grid';
import { batchSaveConstructedData, deleteConstructedData, createConstructedData, updateConstructedData } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';
const ConstructedDataGrid = ({ constructedTableId, fields, rows, onDataChange }) => {
    const theme = useTheme();
    const toast = useToast();
    const apiRef = useGridApiRef();
    // State
    const [rowStates, setRowStates] = useState(new Map(rows.map((row) => {
        const payloadCopy = { ...row.payload };
        const originalPayloadCopy = { ...row.payload };
        return [
            row.id,
            {
                rowId: row.id,
                isDirty: false,
                isNew: false,
                data: payloadCopy,
                errors: [],
                originalData: originalPayloadCopy
            }
        ];
    })));
    const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
    const [deleteRowId, setDeleteRowId] = useState(null);
    const [isSaving, setIsSaving] = useState(false);
    const pendingSaveRef = useRef(new Set());
    // Sync external rows with state
    useEffect(() => {
        setRowStates(new Map(rows.map((row) => [
            row.id,
            {
                rowId: row.id,
                isDirty: false,
                isNew: false,
                data: { ...row.payload },
                errors: [],
                originalData: { ...row.payload }
            }
        ])));
    }, [rows]);
    // Handle adding a new empty row
    const handleAddNewRow = useCallback(() => {
        const newRowId = `new-${Date.now()}`;
        const emptyData = {};
        fields.forEach((field) => {
            emptyData[field.name] = '';
        });
        setRowStates((prev) => {
            const newMap = new Map(prev);
            newMap.set(newRowId, {
                rowId: newRowId,
                isDirty: true,
                isNew: true,
                data: emptyData,
                errors: [],
                originalData: emptyData
            });
            return newMap;
        });
        // Scroll to the new row
        setTimeout(() => {
            apiRef.current?.scrollToIndexes({ rowIndex: rows.length });
        }, 100);
    }, [fields, rows.length, apiRef]);
    // Handle cell value changes (inline editing)
    // Handle cell edit stop (when user leaves cell)
    const handleCellEditStop = useCallback(async (params) => {
        const { id, field: fieldName, value, reason } = params;
        const rowId = String(id);
        const state = rowStates.get(rowId);
        if (!state)
            return;
        // Update the cell value
        setRowStates((prev) => {
            const newMap = new Map(prev);
            const updatedState = newMap.get(rowId);
            if (updatedState) {
                updatedState.data[fieldName] = value;
                updatedState.isDirty = true;
                // Clear errors for this field
                updatedState.errors = updatedState.errors.filter((e) => e.fieldName !== fieldName);
            }
            return newMap;
        });
        // Only auto-save on blur, not on escape
        if (reason === GridCellEditStopReasons.cellFocusOut) {
            const currentState = rowStates.get(rowId);
            if (!currentState)
                return;
            // Update data with new value
            const updatedData = { ...currentState.data, [fieldName]: value };
            pendingSaveRef.current.add(rowId);
            setIsSaving(true);
            try {
                // Validate the row
                const response = await batchSaveConstructedData(constructedTableId, {
                    rows: [updatedData],
                    validateOnly: true
                });
                if (!response.success) {
                    setRowStates((prev) => {
                        const newMap = new Map(prev);
                        const updatedState = newMap.get(rowId);
                        if (updatedState) {
                            updatedState.errors = response.errors;
                        }
                        return newMap;
                    });
                    toast.showToast('Row has validation issues. Please fix before saving.', 'warning');
                    return;
                }
                // If new row, create it first
                if (currentState.isNew) {
                    const createdRow = await createConstructedData({
                        constructedTableId,
                        payload: updatedData
                    });
                    setRowStates((prev) => {
                        const newMap = new Map(prev);
                        newMap.delete(rowId);
                        newMap.set(createdRow.id, {
                            rowId: createdRow.id,
                            isDirty: false,
                            isNew: false,
                            data: createdRow.payload,
                            errors: [],
                            originalData: createdRow.payload
                        });
                        return newMap;
                    });
                    toast.showSuccess('Row created successfully');
                }
                else {
                    // Update existing row
                    await updateConstructedData(rowId, { payload: updatedData });
                    setRowStates((prev) => {
                        const newMap = new Map(prev);
                        const updatedState = newMap.get(rowId);
                        if (updatedState) {
                            updatedState.isDirty = false;
                            updatedState.errors = [];
                            updatedState.originalData = { ...updatedData };
                            updatedState.data = updatedData;
                        }
                        return newMap;
                    });
                    toast.showSuccess('Row saved successfully');
                }
                onDataChange();
            }
            catch (error) {
                const message = error instanceof Error ? error.message : 'Failed to save row';
                toast.showError(message);
            }
            finally {
                pendingSaveRef.current.delete(rowId);
                setIsSaving(false);
            }
        }
    }, [rowStates, toast, onDataChange, constructedTableId]);
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
            const message = error instanceof Error ? error.message : 'Failed to delete row';
            toast.showError(message);
        }
        finally {
            setDeleteConfirmOpen(false);
            setDeleteRowId(null);
        }
    };
    // Build DataGrid columns with inline editing
    const columns = useMemo(() => {
        return [
            {
                field: 'actions',
                headerName: 'Actions',
                width: 80,
                sortable: false,
                filterable: false,
                disableColumnMenu: true,
                editable: false,
                renderCell: (params) => {
                    const rowId = String(params.id);
                    const state = rowStates.get(rowId);
                    const rowErrors = (state?.errors || []).length > 0;
                    return (_jsx(Tooltip, { title: rowErrors ? 'Has validation errors' : 'Delete row', children: _jsx(IconButton, { size: "small", color: "error", onClick: () => handleDeleteClick(rowId), disabled: rowErrors, children: _jsx(DeleteIcon, { fontSize: "small" }) }) }));
                }
            },
            ...fields.map((field) => ({
                field: field.name,
                headerName: `${field.name}${!field.isNullable ? ' *' : ''}`,
                width: Math.max(150, field.name.length * 10),
                editable: true,
                sortable: true,
                filterable: true,
                renderCell: (params) => {
                    const rowId = String(params.id);
                    const state = rowStates.get(rowId);
                    const fieldErrors = (state?.errors || []).filter((e) => e.fieldName === field.name);
                    const rawValue = params.value;
                    const displayValue = rawValue === null || rawValue === undefined ? '' : String(rawValue);
                    return (_jsx(Box, { sx: { width: '100%', py: 0.5 }, children: _jsxs(Box, { sx: {
                                display: 'flex',
                                flexDirection: 'column',
                                width: '100%'
                            }, children: [_jsx(Typography, { sx: {
                                        fontSize: '0.875rem',
                                        color: fieldErrors.length > 0 ? theme.palette.error.main : 'inherit'
                                    }, children: displayValue }), fieldErrors.length > 0 && (_jsxs(Typography, { sx: {
                                        fontSize: '0.65rem',
                                        color: theme.palette.error.main,
                                        fontStyle: 'italic',
                                        mt: 0.25
                                    }, children: ["\u26A0 ", fieldErrors[0].message] }))] }) }));
                }
            }))
        ];
    }, [fields, rowStates, theme]);
    // Build DataGrid rows
    const gridRows = useMemo(() => {
        return Array.from(rowStates.values()).map((state) => {
            const rowErrors = state.errors.length > 0;
            return {
                id: state.rowId,
                ...state.data,
                _hasErrors: rowErrors,
                _isDirty: state.isDirty,
                _isNew: state.isNew
            };
        });
    }, [rowStates]);
    return (_jsxs(_Fragment, { children: [_jsxs(Box, { sx: { display: 'flex', flexDirection: 'column', gap: 2, mb: 2 }, children: [_jsxs(Box, { sx: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' }, children: [_jsxs(Typography, { variant: "subtitle2", sx: { fontWeight: 600 }, children: ["Data Rows (", gridRows.filter(r => !r._isNew).length, ")"] }), _jsx(Button, { size: "small", startIcon: _jsx(AddIcon, {}), variant: "contained", onClick: handleAddNewRow, disabled: isSaving, children: "Add New Row" })] }), _jsx(Alert, { severity: "info", sx: { py: 1 }, children: "\uD83D\uDCA1 Click any cell to edit directly. Changes auto-save when you move to another cell." })] }), _jsx(Box, { sx: {
                    width: '100%',
                    height: 'calc(100vh - 550px)',
                    minHeight: 300,
                    backgroundColor: '#fff',
                    border: `1px solid ${theme.palette.divider}`,
                    borderRadius: 1,
                    '& .MuiDataGrid-root': {
                        border: 'none',
                        fontSize: '0.875rem',
                        '& .MuiDataGrid-columnHeaderTitle': {
                            fontWeight: 600
                        }
                    },
                    '& .MuiDataGrid-columnHeader': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.12),
                        borderBottom: `2px solid ${alpha(theme.palette.primary.main, 0.3)}`
                    },
                    '& .MuiDataGrid-cell': {
                        borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
                        paddingX: 2,
                        paddingY: 1,
                        '&:focus': {
                            outline: 'none'
                        },
                        '&:focus-within': {
                            outline: 'none'
                        }
                    },
                    '& .MuiDataGrid-row': {
                        borderBottom: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
                        '&:hover': {
                            backgroundColor: alpha(theme.palette.action.hover, 0.08)
                        },
                        '&.row-error': {
                            backgroundColor: alpha(theme.palette.error.main, 0.08),
                            '&:hover': {
                                backgroundColor: alpha(theme.palette.error.main, 0.12)
                            }
                        },
                        '&.row-new': {
                            backgroundColor: alpha(theme.palette.success.main, 0.05),
                            '&:hover': {
                                backgroundColor: alpha(theme.palette.success.main, 0.1)
                            }
                        }
                    },
                    '& .MuiDataGrid-editInputBase input': {
                        padding: '4px 8px',
                        fontSize: '0.875rem'
                    },
                    '& .MuiDataGrid-columnHeaderTitleContainer': {
                        overflow: 'visible'
                    }
                }, children: _jsx(DataGrid, { apiRef: apiRef, rows: gridRows, columns: columns, pageSizeOptions: [10, 25, 50, 100], initialState: {
                        pagination: {
                            paginationModel: { pageSize: 25, page: 0 }
                        }
                    }, disableRowSelectionOnClick: true, processRowUpdate: (updatedRow) => updatedRow, onCellEditStop: handleCellEditStop, getRowClassName: (params) => {
                        if (params.row._hasErrors)
                            return 'row-error';
                        if (params.row._isNew)
                            return 'row-new';
                        return '';
                    }, sx: {
                        '& .MuiDataGrid-virtualScroller': {
                            minHeight: 300
                        },
                        '& .MuiDataGrid-columnHeaders': {
                            backgroundColor: alpha(theme.palette.primary.main, 0.1)
                        }
                    } }) }), _jsxs(Dialog, { open: deleteConfirmOpen, onClose: () => setDeleteConfirmOpen(false), children: [_jsx(DialogTitle, { children: "Delete Row?" }), _jsx(DialogContent, { children: _jsx(Box, { sx: { mt: 2 }, children: _jsx(Typography, { children: "Are you sure you want to delete this row? This action cannot be undone." }) }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: () => setDeleteConfirmOpen(false), children: "Cancel" }), _jsx(Button, { onClick: handleDeleteConfirm, color: "error", variant: "contained", children: "Delete" })] })] })] }));
};
export default ConstructedDataGrid;
