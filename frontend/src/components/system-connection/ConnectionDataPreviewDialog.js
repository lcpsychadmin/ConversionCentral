import { jsxs as _jsxs, jsx as _jsx } from "react/jsx-runtime";
import { Alert, Box, Button, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from '@mui/material';
import { useMemo } from 'react';
const formatValue = (value) => {
    if (value === null || value === undefined) {
        return '—';
    }
    if (typeof value === 'string') {
        return value;
    }
    if (value instanceof Date) {
        return value.toISOString();
    }
    if (typeof value === 'object') {
        try {
            return JSON.stringify(value);
        }
        catch (error) {
            return String(value);
        }
    }
    return String(value);
};
const ConnectionDataPreviewDialog = ({ open, schemaName, tableName, loading, error, preview, onClose, onRefresh }) => {
    const columns = preview?.columns ?? [];
    const rows = preview?.rows ?? [];
    const title = useMemo(() => {
        if (!schemaName || schemaName.length === 0) {
            return tableName;
        }
        return `${schemaName}.${tableName}`;
    }, [schemaName, tableName]);
    return (_jsxs(Dialog, { open: open, onClose: onClose, fullWidth: true, maxWidth: "lg", children: [_jsxs(DialogTitle, { children: ["Preview \u2014 ", title] }), _jsxs(DialogContent, { dividers: true, sx: { minHeight: 320 }, children: [loading && (_jsx(Box, { display: "flex", justifyContent: "center", alignItems: "center", height: "100%", children: _jsx(CircularProgress, {}) })), !loading && error && (_jsx(Alert, { severity: "error", action: _jsx(Button, { color: "inherit", size: "small", onClick: onRefresh, children: "Retry" }), children: error })), !loading && !error && columns.length === 0 && (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "No data returned. The table may be empty or inaccessible." })), !loading && !error && columns.length > 0 && (_jsx(TableContainer, { sx: { maxHeight: 420 }, children: _jsxs(Table, { size: "small", stickyHeader: true, children: [_jsx(TableHead, { children: _jsx(TableRow, { children: columns.map((column) => (_jsx(TableCell, { sx: { fontWeight: 600 }, children: column }, column))) }) }), _jsx(TableBody, { children: rows.map((row, index) => (_jsx(TableRow, { hover: true, children: columns.map((column) => (_jsx(TableCell, { sx: { whiteSpace: 'nowrap' }, children: formatValue(row[column]) }, column))) }, index))) })] }) }))] }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: onRefresh, disabled: loading, variant: "outlined", children: "Refresh" }), _jsx(Button, { onClick: onClose, variant: "contained", children: "Close" })] })] }));
};
export default ConnectionDataPreviewDialog;
