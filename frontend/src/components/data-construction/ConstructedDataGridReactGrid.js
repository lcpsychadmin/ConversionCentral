import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Box, Typography, IconButton, Tooltip } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { alpha, useTheme } from '@mui/material/styles';
import { batchSaveConstructedData, createConstructedData, updateConstructedData, deleteConstructedData } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';
const emptyRowIdPrefix = 'new-';
const ConstructedDataGridReactGrid = ({ constructedTableId, fields, rows, onDataChange }) => {
    const theme = useTheme();
    const toast = useToast();
    const [localRows, setLocalRows] = useState(() => rows.map((r) => ({ ...r })));
    const editingRef = useRef(null);
    useEffect(() => {
        setLocalRows(rows.map((r) => ({ ...r })));
    }, [rows]);
    // Ensure there's always an extra blank row at the end for spreadsheet-style entry
    const rowsWithBlank = useMemo(() => {
        const last = localRows[localRows.length - 1];
        const needsBlank = !last || Object.values(last.payload).some(v => v !== '' && v !== null && v !== undefined);
        if (needsBlank) {
            const payload = fields.reduce((acc, field) => {
                acc[field.name] = '';
                return acc;
            }, {});
            const blank = {
                id: `${emptyRowIdPrefix}${Date.now()}`,
                constructedTableId,
                payload,
                isNew: true,
            };
            return [...localRows, blank];
        }
        return localRows;
    }, [localRows, fields, constructedTableId]);
    const updateCellLocal = useCallback((rowIndex, field, value) => {
        setLocalRows((prev) => {
            const copy = prev.map((r) => ({ ...r, payload: { ...r.payload } }));
            if (rowIndex >= copy.length)
                return copy;
            copy[rowIndex].payload[field] = value;
            return copy;
        });
    }, []);
    const saveRow = useCallback(async (row) => {
        try {
            // Run server-side validation first
            const validation = await batchSaveConstructedData(constructedTableId, { rows: [row.payload], validateOnly: true });
            if (!validation.success) {
                toast.showToast('Validation failed for row. Fix errors before continuing.', 'warning');
                return { success: false, errors: validation.errors };
            }
            if (String(row.id).startsWith(emptyRowIdPrefix)) {
                const created = await createConstructedData({ constructedTableId, payload: row.payload });
                // replace local id with created id
                setLocalRows((prev) => prev.map((r) => (r.id === row.id ? { ...created, isNew: false } : r)));
                toast.showToast('Row created', 'success');
                return { success: true, row: { ...created, isNew: false } };
            }
            else {
                await updateConstructedData(row.id, { payload: row.payload });
                setLocalRows((prev) => prev.map((r) => (r.id === row.id ? { ...r, payload: { ...row.payload }, isNew: false } : r)));
                toast.showToast('Row saved', 'success');
                return { success: true, row: { ...row, isNew: false } };
            }
        }
        catch (rawError) {
            const message = rawError instanceof Error ? rawError.message : 'Failed to save row';
            toast.showToast(message, 'error');
            return { success: false, error: message };
        }
    }, [constructedTableId, toast]);
    const onCellBlur = useCallback(async (rowIndex) => {
        const row = rowsWithBlank[rowIndex];
        if (!row)
            return;
        // If the row is blank and untouched, skip
        const allEmpty = Object.values(row.payload).every(v => v === '' || v === null || v === undefined);
        if (allEmpty)
            return;
        const result = await saveRow(row);
        if (result.success) {
            onDataChange();
        }
    }, [rowsWithBlank, saveRow, onDataChange]);
    const handleDelete = useCallback(async (rowId) => {
        if (String(rowId).startsWith(emptyRowIdPrefix)) {
            setLocalRows((prev) => prev.filter((r) => r.id !== rowId));
            return;
        }
        try {
            await deleteConstructedData(rowId);
            setLocalRows((prev) => prev.filter((r) => r.id !== rowId));
            toast.showToast('Row deleted', 'success');
            onDataChange();
        }
        catch (rawError) {
            const message = rawError instanceof Error ? rawError.message : 'Failed to delete row';
            toast.showToast(message, 'error');
        }
    }, [onDataChange, toast]);
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: { display: 'flex', gap: 1, mb: 1, alignItems: 'center' }, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600 }, children: "Data Rows" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "(start typing into any cell \u2014 new rows are created automatically)" })] }), _jsx(Box, { sx: { overflow: 'auto', border: `1px solid ${alpha(theme.palette.divider, 0.6)}`, borderRadius: 1 }, children: _jsxs("table", { style: { borderCollapse: 'collapse', width: '100%' }, children: [_jsx("thead", { children: _jsxs("tr", { children: [_jsx("th", { style: { textAlign: 'left', padding: '8px 12px', borderBottom: `1px solid ${alpha(theme.palette.divider, 0.6)}`, width: 60 }, children: "Actions" }), fields.map(f => (_jsxs("th", { style: { textAlign: 'left', padding: '8px 12px', borderBottom: `1px solid ${alpha(theme.palette.divider, 0.6)}` }, children: [f.name, !f.isNullable && _jsx("span", { style: { color: 'red' }, children: " *" })] }, f.id)))] }) }), _jsx("tbody", { children: rowsWithBlank.map((r, rowIndex) => (_jsxs("tr", { style: { background: r.id.toString().startsWith(emptyRowIdPrefix) ? alpha(theme.palette.success.main, 0.05) : 'transparent' }, children: [_jsx("td", { style: { padding: 6, borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}` }, children: _jsx(Tooltip, { title: "Delete row", children: _jsx(IconButton, { size: "small", onClick: () => handleDelete(r.id), children: _jsx(DeleteIcon, { fontSize: "small" }) }) }) }), fields.map((f) => {
                                        const rawValue = r.payload?.[f.name];
                                        const displayValue = rawValue == null ? '' : String(rawValue);
                                        return (_jsx("td", { style: { padding: 6, borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}` }, children: _jsx("div", { contentEditable: true, suppressContentEditableWarning: true, style: { minHeight: 28, padding: '4px 8px', outline: 'none', cursor: 'text' }, role: "textbox", "aria-label": `${f.name} value`, "aria-multiline": "false", tabIndex: 0, onFocus: () => { editingRef.current = { rowIndex, fieldName: f.name }; }, onBlur: (e) => {
                                                    const newValue = e.currentTarget.textContent ?? '';
                                                    updateCellLocal(rowIndex, f.name, newValue);
                                                    void onCellBlur(rowIndex);
                                                }, onKeyDown: (e) => {
                                                    if (e.key === 'Enter') {
                                                        e.preventDefault();
                                                        // Move focus to next cell (simple behavior: blur)
                                                        e.currentTarget.blur();
                                                    }
                                                }, children: displayValue }) }, `${r.id}-${f.name}`));
                                    })] }, r.id))) })] }) })] }));
};
export default ConstructedDataGridReactGrid;
