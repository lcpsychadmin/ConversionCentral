import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Autocomplete, Button, Dialog, DialogActions, DialogContent, DialogTitle, Stack, TextField, Typography, CircularProgress, Alert, Checkbox, FormControlLabel, Box, List, ListItem, ListItemIcon, ListItemText } from '@mui/material';
import { fetchSourceTableColumns } from '../../services/dataDefinitionService';
const AddExistingSourceTableDialog = ({ dataObjectId, open, tables, loading = false, error, onClose, onSubmit }) => {
    const [selectedTable, setSelectedTable] = useState(null);
    const [columns, setColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState(new Set());
    const [localError, setLocalError] = useState(null);
    const [columnsLoading, setColumnsLoading] = useState(false);
    useEffect(() => {
        if (!open) {
            return;
        }
        setSelectedTable(null);
        setColumns([]);
        setSelectedColumns(new Set());
        setLocalError(null);
        setColumnsLoading(false);
    }, [open, tables]);
    // Fetch columns when table is selected
    useEffect(() => {
        if (!selectedTable || !dataObjectId) {
            return;
        }
        const fetchColumns = async () => {
            setColumnsLoading(true);
            setLocalError(null);
            try {
                const cols = await fetchSourceTableColumns(dataObjectId, selectedTable.schemaName, selectedTable.tableName);
                setColumns(cols);
                // Auto-select all columns by default
                setSelectedColumns(new Set(cols.map((c) => c.name)));
            }
            catch (err) {
                const message = err instanceof Error ? err.message : 'Failed to load columns';
                setLocalError(message);
                setColumns([]);
                setSelectedColumns(new Set());
            }
            finally {
                setColumnsLoading(false);
            }
        };
        fetchColumns();
    }, [selectedTable, dataObjectId]);
    const tableOptions = useMemo(() => tables
        .slice()
        .sort((a, b) => {
        const schemaCompare = (a.schemaName ?? '').localeCompare(b.schemaName ?? '');
        return schemaCompare !== 0 ? schemaCompare : (a.tableName ?? '').localeCompare(b.tableName ?? '');
    }), [tables]);
    const handleClose = () => {
        if (loading || columnsLoading) {
            return;
        }
        onClose();
    };
    const handleTableChange = (_, value) => {
        setSelectedTable(value);
        setColumns([]);
        setSelectedColumns(new Set());
        setLocalError(null);
    };
    const handleColumnToggle = (columnName) => {
        setSelectedColumns((prev) => {
            const next = new Set(prev);
            if (next.has(columnName)) {
                next.delete(columnName);
            }
            else {
                next.add(columnName);
            }
            return next;
        });
    };
    const handleSelectAll = () => {
        if (selectedColumns.size === columns.length) {
            setSelectedColumns(new Set());
        }
        else {
            setSelectedColumns(new Set(columns.map((c) => c.name)));
        }
    };
    const handleSubmit = async () => {
        if (!selectedTable) {
            setLocalError('Select a table to add.');
            return;
        }
        if (selectedColumns.size === 0) {
            setLocalError('Select at least one column to add.');
            return;
        }
        setLocalError(null);
        const columnsToAdd = columns.filter((c) => selectedColumns.has(c.name));
        await onSubmit({
            schemaName: selectedTable.schemaName,
            tableName: selectedTable.tableName,
            tableType: selectedTable.tableType,
            columnCount: selectedTable.columnCount,
            estimatedRows: selectedTable.estimatedRows,
            selectedColumns: columnsToAdd
        });
    };
    return (_jsxs(Dialog, { open: open, onClose: handleClose, maxWidth: "sm", fullWidth: true, children: [_jsx(DialogTitle, { children: "Add Source Table to Definition" }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, mt: 1, children: [error && (_jsx(Alert, { severity: "error", children: error })), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Available source tables from your system connections" }), _jsx(Autocomplete, { options: tableOptions, value: selectedTable, onChange: handleTableChange, getOptionLabel: (option) => `${option.schemaName}.${option.tableName}`, isOptionEqualToValue: (option, value) => option.schemaName === value.schemaName && option.tableName === value.tableName, renderInput: (params) => (_jsx(TextField, { ...params, label: "Source Table", placeholder: tableOptions.length ? 'Select table' : 'No tables available', required: true, error: Boolean(localError && !selectedTable), helperText: localError && !selectedTable ? localError : '' })), disabled: loading || !tableOptions.length }), selectedTable && (_jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "caption", color: "text.secondary", children: _jsx("strong", { children: "Table Details:" }) }), selectedTable.tableType && (_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Type: ", selectedTable.tableType] })), selectedTable.columnCount !== null && selectedTable.columnCount !== undefined && (_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Columns: ", selectedTable.columnCount] })), selectedTable.estimatedRows !== null && selectedTable.estimatedRows !== undefined && (_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Est. Rows: ", new Intl.NumberFormat().format(selectedTable.estimatedRows)] }))] }), _jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "caption", color: "text.secondary", children: _jsx("strong", { children: "Columns to Import:" }) }), columnsLoading ? (_jsx(Box, { display: "flex", justifyContent: "center", py: 2, children: _jsx(CircularProgress, { size: 24 }) })) : columns.length > 0 ? (_jsxs(Box, { children: [_jsx(FormControlLabel, { control: _jsx(Checkbox, { checked: selectedColumns.size === columns.length && columns.length > 0, indeterminate: selectedColumns.size > 0 && selectedColumns.size < columns.length, onChange: handleSelectAll }), label: `Select All (${selectedColumns.size}/${columns.length})` }), _jsx(List, { dense: true, sx: {
                                                        maxHeight: 300,
                                                        overflow: 'auto',
                                                        border: '1px solid',
                                                        borderColor: 'divider',
                                                        borderRadius: 1
                                                    }, children: columns.map((col) => (_jsxs(ListItem, { dense: true, onClick: () => handleColumnToggle(col.name), sx: { cursor: 'pointer' }, children: [_jsx(ListItemIcon, { children: _jsx(Checkbox, { edge: "start", checked: selectedColumns.has(col.name), tabIndex: -1, disableRipple: true }) }), _jsx(ListItemText, { primary: col.name, secondary: col.typeName, primaryTypographyProps: { variant: 'body2' }, secondaryTypographyProps: { variant: 'caption' } })] }, col.name))) })] })) : (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "No columns available" }))] })] }))] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading || columnsLoading, children: "Cancel" }), _jsx(LoadingButton, { onClick: handleSubmit, variant: "contained", loading: loading, disabled: loading || columnsLoading || !selectedTable || selectedColumns.size === 0, children: "Add Table with Fields" })] })] }));
};
export default AddExistingSourceTableDialog;
