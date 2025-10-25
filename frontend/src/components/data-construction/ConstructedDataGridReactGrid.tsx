import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Box, Typography, IconButton, Tooltip } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { alpha, useTheme } from '@mui/material/styles';
import { ConstructedField, ConstructedData, batchSaveConstructedData, createConstructedData, updateConstructedData, deleteConstructedData, ValidationError, ConstructedRowPayload } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

type EditableConstructedRow = ConstructedData & { isNew?: boolean };

interface SaveRowResult {
  success: boolean;
  row?: EditableConstructedRow;
  errors?: ValidationError[];
  error?: string;
}

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
}

const emptyRowIdPrefix = 'new-';

const ConstructedDataGridReactGrid: React.FC<Props> = ({ constructedTableId, fields, rows, onDataChange }) => {
  const theme = useTheme();
  const toast = useToast();
  const [localRows, setLocalRows] = useState<EditableConstructedRow[]>(() => rows.map((r) => ({ ...r })));
  const editingRef = useRef<{ rowIndex: number; fieldName: string } | null>(null);

  useEffect(() => {
    setLocalRows(rows.map((r) => ({ ...r })));
  }, [rows]);

  // Ensure there's always an extra blank row at the end for spreadsheet-style entry
  const rowsWithBlank = useMemo(() => {
  const last = localRows[localRows.length - 1];
  const needsBlank = !last || Object.values(last.payload).some(v => v !== '' && v !== null && v !== undefined);
    if (needsBlank) {
      const payload = fields.reduce<ConstructedRowPayload>((acc, field) => {
        acc[field.name] = '';
        return acc;
      }, {});
      const blank: EditableConstructedRow = {
        id: `${emptyRowIdPrefix}${Date.now()}`,
        constructedTableId,
        payload,
        isNew: true,
      };
      return [...localRows, blank];
    }
    return localRows;
  }, [localRows, fields, constructedTableId]);

  const updateCellLocal = useCallback((rowIndex: number, field: string, value: string) => {
    setLocalRows((prev) => {
  const copy = prev.map((r) => ({ ...r, payload: { ...r.payload } }));
      if (rowIndex >= copy.length) return copy;
      copy[rowIndex].payload[field] = value;
      return copy;
    });
  }, []);

  const saveRow = useCallback(async (row: EditableConstructedRow): Promise<SaveRowResult> => {
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
      } else {
        await updateConstructedData(row.id, { payload: row.payload });
        setLocalRows((prev) => prev.map((r) => (r.id === row.id ? { ...r, payload: { ...row.payload }, isNew: false } : r)));
        toast.showToast('Row saved', 'success');
        return { success: true, row: { ...row, isNew: false } };
      }
    } catch (rawError) {
      const message = rawError instanceof Error ? rawError.message : 'Failed to save row';
      toast.showToast(message, 'error');
      return { success: false, error: message };
    }
  }, [constructedTableId, toast]);

  const onCellBlur = useCallback(async (rowIndex: number) => {
    const row = rowsWithBlank[rowIndex];
    if (!row) return;
    // If the row is blank and untouched, skip
    const allEmpty = Object.values(row.payload).every(v => v === '' || v === null || v === undefined);
    if (allEmpty) return;

    const result = await saveRow(row);
    if (result.success) {
      onDataChange();
    }
  }, [rowsWithBlank, saveRow, onDataChange]);

  const handleDelete = useCallback(async (rowId: string) => {
    if (String(rowId).startsWith(emptyRowIdPrefix)) {
      setLocalRows((prev) => prev.filter((r) => r.id !== rowId));
      return;
    }
    try {
      await deleteConstructedData(rowId);
      setLocalRows((prev) => prev.filter((r) => r.id !== rowId));
      toast.showToast('Row deleted', 'success');
      onDataChange();
    } catch (rawError) {
      const message = rawError instanceof Error ? rawError.message : 'Failed to delete row';
      toast.showToast(message, 'error');
    }
  }, [onDataChange, toast]);

  return (
    <Box>
      <Box sx={{ display: 'flex', gap: 1, mb: 1, alignItems: 'center' }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Data Rows</Typography>
        <Typography variant="body2" color="text.secondary">(start typing into any cell — new rows are created automatically)</Typography>
      </Box>

      <Box sx={{ overflow: 'auto', border: `1px solid ${alpha(theme.palette.divider, 0.6)}`, borderRadius: 1 }}>
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              <th style={{ textAlign: 'left', padding: '8px 12px', borderBottom: `1px solid ${alpha(theme.palette.divider, 0.6)}`, width: 60 }}>Actions</th>
              {fields.map(f => (
                <th key={f.id} style={{ textAlign: 'left', padding: '8px 12px', borderBottom: `1px solid ${alpha(theme.palette.divider, 0.6)}` }}>
                  {f.name}{!f.isNullable && <span style={{ color: 'red' }}> *</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rowsWithBlank.map((r, rowIndex) => (
              <tr key={r.id} style={{ background: r.id.toString().startsWith(emptyRowIdPrefix) ? alpha(theme.palette.success.main, 0.05) : 'transparent' }}>
                <td style={{ padding: 6, borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}` }}>
                  <Tooltip title="Delete row">
                    <IconButton size="small" onClick={() => handleDelete(r.id)}>
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </td>
                {fields.map((f) => {
                  const rawValue = r.payload?.[f.name];
                  const displayValue = rawValue == null ? '' : String(rawValue);

                  return (
                    <td
                      key={`${r.id}-${f.name}`}
                      style={{ padding: 6, borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}` }}
                    >
                      <div
                        contentEditable
                        suppressContentEditableWarning
                        style={{ minHeight: 28, padding: '4px 8px', outline: 'none', cursor: 'text' }}
                        role="textbox"
                        aria-label={`${f.name} value`}
                        aria-multiline="false"
                        tabIndex={0}
                        onFocus={() => { editingRef.current = { rowIndex, fieldName: f.name }; }}
                        onBlur={(e) => {
                          const newValue = e.currentTarget.textContent ?? '';
                          updateCellLocal(rowIndex, f.name, newValue);
                          void onCellBlur(rowIndex);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') {
                            e.preventDefault();
                            // Move focus to next cell (simple behavior: blur)
                            (e.currentTarget as HTMLElement).blur();
                          }
                        }}
                      >
                        {displayValue}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    </Box>
  );
};

export default ConstructedDataGridReactGrid;
