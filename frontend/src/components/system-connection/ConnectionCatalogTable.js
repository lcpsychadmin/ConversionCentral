import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { Box, Button, Chip, CircularProgress, Stack, Typography, Alert, Accordion, AccordionSummary, AccordionDetails, Checkbox, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
const buildRowId = (row) => `${row.schemaName}.${row.tableName}`;
const formatNumber = (value) => {
    if (value === null || value === undefined) {
        return '—';
    }
    return new Intl.NumberFormat().format(value);
};
const ConnectionCatalogTable = ({ rows, loading = false, saving = false, error, onRefresh, onSelectionChange, onPreview }) => {
    const theme = useTheme();
    const [selectionModel, setSelectionModel] = useState([]);
    const gridRows = useMemo(() => rows.map((row) => ({
        id: buildRowId(row),
        schema: row.schemaName,
        table: row.tableName,
        type: row.tableType,
        columnCount: row.columnCount,
        estimatedRows: row.estimatedRows,
        available: row.available,
        selected: row.selected,
        original: row
    })), [rows]);
    // Group rows by schema
    const groupedBySchema = useMemo(() => {
        const groups = {};
        gridRows.forEach((row) => {
            if (!groups[row.schema]) {
                groups[row.schema] = [];
            }
            groups[row.schema].push(row);
        });
        // Sort schemas and tables alphabetically
        return Object.keys(groups)
            .sort()
            .reduce((acc, schema) => {
            acc[schema] = groups[schema].sort((a, b) => (a.table ?? '').localeCompare(b.table ?? ''));
            return acc;
        }, {});
    }, [gridRows]);
    useEffect(() => {
        setSelectionModel(gridRows.filter((row) => row.selected).map((row) => row.id));
    }, [gridRows]);
    const handleRowCheckChange = (rowId, checked) => {
        const newSelection = checked
            ? [...selectionModel, rowId]
            : selectionModel.filter((id) => id !== rowId);
        setSelectionModel(newSelection);
        onSelectionChange?.(newSelection.map((id) => id.toString()));
    };
    const handleSchemaCheckChange = (schema, checked) => {
        const schemaRows = groupedBySchema[schema];
        const schemaRowIds = schemaRows.map((row) => row.id);
        let newSelection = [...selectionModel];
        if (checked) {
            newSelection = [...new Set([...newSelection, ...schemaRowIds])];
        }
        else {
            newSelection = newSelection.filter((id) => !schemaRowIds.includes(id));
        }
        setSelectionModel(newSelection);
        onSelectionChange?.(newSelection.map((id) => id.toString()));
    };
    const isSchemaSelected = (schema) => {
        const schemaRows = groupedBySchema[schema];
        return schemaRows.every((row) => selectionModel.includes(row.id));
    };
    const isSchemaIndeterminate = (schema) => {
        const schemaRows = groupedBySchema[schema];
        const selectedCount = schemaRows.filter((row) => selectionModel.includes(row.id)).length;
        return selectedCount > 0 && selectedCount < schemaRows.length;
    };
    const getSchemaSelectedCount = (schema) => {
        const schemaRows = groupedBySchema[schema];
        return schemaRows.filter((row) => selectionModel.includes(row.id)).length;
    };
    return (_jsxs(Stack, { spacing: 2, sx: { position: 'relative' }, children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", children: [_jsx(Typography, { variant: "h6", sx: { fontWeight: 700, letterSpacing: 0.3 }, children: "Source Catalog" }), _jsxs(Box, { display: "flex", alignItems: "center", gap: 1.5, children: [saving && _jsx(CircularProgress, { size: 22, thickness: 5 }), _jsx(Button, { variant: "outlined", onClick: onRefresh, disabled: loading || saving, children: "Refresh Catalog" })] })] }), error && (_jsx(Alert, { severity: "error", sx: { mb: 1 }, children: error })), loading ? (_jsx(Box, { sx: { display: 'flex', justifyContent: 'center', py: 4 }, children: _jsx(CircularProgress, {}) })) : (_jsx(Box, { sx: { maxHeight: 600, overflow: 'auto', border: `1px solid ${theme.palette.divider}`, borderRadius: 1 }, children: Object.entries(groupedBySchema).map(([schema, tables]) => (_jsxs(Accordion, { defaultExpanded: false, children: [_jsxs(AccordionSummary, { expandIcon: _jsx(ExpandMoreIcon, {}), children: [_jsx(Checkbox, { checked: isSchemaSelected(schema), indeterminate: isSchemaIndeterminate(schema), onChange: (e) => handleSchemaCheckChange(schema, e.target.checked), onClick: (e) => e.stopPropagation(), sx: { mr: 1 } }), _jsxs(Typography, { sx: { fontWeight: 600, flex: 1 }, children: [schema, " (", getSchemaSelectedCount(schema), "/", tables.length, ")"] })] }), _jsx(AccordionDetails, { sx: { p: 0 }, children: _jsx(TableContainer, { component: Paper, elevation: 0, children: _jsxs(Table, { size: "small", children: [_jsx(TableHead, { children: _jsxs(TableRow, { sx: { backgroundColor: alpha(theme.palette.primary.main, 0.05) }, children: [_jsx(TableCell, { padding: "checkbox", sx: { width: 44 } }), _jsx(TableCell, { sx: { fontWeight: 600 }, children: "Table" }), _jsx(TableCell, { sx: { fontWeight: 600, width: 120 }, children: "Type" }), _jsx(TableCell, { align: "center", sx: { fontWeight: 600, width: 100 }, children: "Columns" }), _jsx(TableCell, { align: "right", sx: { fontWeight: 600, width: 130 }, children: "Rows (est.)" }), _jsx(TableCell, { align: "center", sx: { fontWeight: 600, width: 110 }, children: "Status" }), onPreview && _jsx(TableCell, { align: "center", sx: { fontWeight: 600, width: 100 }, children: "Preview" })] }) }), _jsx(TableBody, { children: tables.map((row) => (_jsxs(TableRow, { sx: {
                                                    backgroundColor: !row.available ? alpha(theme.palette.warning.light, 0.08) : undefined,
                                                    '&:hover': {
                                                        backgroundColor: alpha(theme.palette.action.hover, 0.5)
                                                    }
                                                }, children: [_jsx(TableCell, { padding: "checkbox", children: _jsx(Checkbox, { checked: selectionModel.includes(row.id), onChange: (e) => handleRowCheckChange(row.id, e.target.checked), disabled: saving }) }), _jsx(TableCell, { sx: { color: !row.available ? theme.palette.text.disabled : undefined }, children: row.table }), _jsx(TableCell, { sx: { color: !row.available ? theme.palette.text.disabled : undefined }, children: row.type?.replace('_', ' ') ?? '—' }), _jsx(TableCell, { align: "center", sx: { color: !row.available ? theme.palette.text.disabled : undefined }, children: row.columnCount ?? '—' }), _jsx(TableCell, { align: "right", sx: { color: !row.available ? theme.palette.text.disabled : undefined }, children: formatNumber(row.estimatedRows) }), _jsx(TableCell, { align: "center", children: _jsx(Chip, { label: row.available ? 'Available' : 'Missing', color: row.available ? 'success' : 'warning', size: "small", variant: row.available ? 'outlined' : 'filled' }) }), onPreview && _jsx(TableCell, { align: "center", children: _jsx(Button, { variant: "text", size: "small", onClick: () => onPreview?.(row.original), disabled: !row.available || saving, children: "Preview" }) })] }, row.id))) })] }) }) })] }, schema))) }))] }));
};
export default ConnectionCatalogTable;
