import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Box, Typography, Button, Stack, CircularProgress, Tooltip, Dialog, DialogTitle, DialogContent, DialogActions, Snackbar } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, GridApi, GridReadyEvent, RowValueChangedEvent, SelectionChangedEvent, ICellRendererParams } from 'ag-grid-community';
import { alpha, useTheme } from '@mui/material/styles';

import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css';

import { ConstructedField, ConstructedData, batchSaveConstructedData, createConstructedData, updateConstructedData, deleteConstructedData, ConstructedRowPayload } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

type EditableConstructedRow = ConstructedData & { isNew?: boolean };

type PayloadFieldKey = `payload.${string}`;

type GridRowData = EditableConstructedRow & Record<PayloadFieldKey, unknown>;

interface DeleteRendererParams extends ICellRendererParams<GridRowData> {
  onClick?: (params: ICellRendererParams<GridRowData>) => void;
}

interface SaveRowOptions {
  skipRefresh?: boolean;
  suppressSuccessToast?: boolean;
  suppressErrorToast?: boolean;
}

interface SaveRowResult {
  success: boolean;
  error?: string;
}

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
}

const emptyRowIdPrefix = 'new-';

// Custom renderer for delete button
const DeleteButtonRenderer: React.FC<DeleteRendererParams> = (props) => {
  const handleClick = () => {
    props.onClick?.(props);
  };

  return (
    <Tooltip title="Delete row" arrow>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100%',
          cursor: 'pointer',
          width: '100%',
        }}
        onClick={handleClick}
      >
        <DeleteIcon fontSize="small" />
      </Box>
    </Tooltip>
  );
};

const ConstructedDataGridAgGrid: React.FC<Props> = ({ constructedTableId, fields, rows, onDataChange }) => {
  const theme = useTheme();
  const toast = useToast();
  const [gridApi, setGridApi] = useState<GridApi<GridRowData> | null>(null);
  const [localRows, setLocalRows] = useState<EditableConstructedRow[]>(() => rows.map((r) => ({ ...r })));
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<GridRowData | null>(null);
  const [bulkDeleteTargets, setBulkDeleteTargets] = useState<GridRowData[]>([]);
  const [isBulkDeleting, setIsBulkDeleting] = useState(false);
  const [selectedRowCount, setSelectedRowCount] = useState(0);
  const [undoState, setUndoState] = useState<{ rows: EditableConstructedRow[] } | null>(null);
  const [isRestoring, setIsRestoring] = useState(false);
  const gridContainerRef = useRef<HTMLDivElement | null>(null);
  const dataRowCount = rows.length;

  useEffect(() => {
    setLocalRows(rows.map(r => ({ ...r })));
  }, [rows]);

  // Ensure there's always a blank row at the end
  const createEmptyRow = useCallback((): EditableConstructedRow => {
    const uniqueSuffix = Math.random().toString(16).slice(2, 10);
    const payload = fields.reduce<ConstructedRowPayload>((acc, field) => {
      acc[field.name] = '';
      return acc;
    }, {});
    return {
      id: `${emptyRowIdPrefix}${Date.now()}-${uniqueSuffix}`,
      constructedTableId,
      payload,
      rowIdentifier: null,
      isNew: true,
    };
  }, [constructedTableId, fields]);

  const toUndoRow = useCallback(
    (row: GridRowData | EditableConstructedRow): EditableConstructedRow => ({
      id: row.id,
      constructedTableId: row.constructedTableId ?? constructedTableId,
      payload: { ...row.payload },
      rowIdentifier: row.rowIdentifier ?? null,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      isNew: row.isNew ?? String(row.id).startsWith(emptyRowIdPrefix),
    }),
    [constructedTableId]
  );

  const dataColumnIds = useMemo(() => fields.map(f => `payload.${f.name}`), [fields]);

  const toGridRow = useCallback(
    (row: EditableConstructedRow): GridRowData => {
      const flattened: GridRowData = {
        ...row,
        payload: { ...row.payload },
        isNew: row.isNew ?? String(row.id).startsWith(emptyRowIdPrefix)
      };
      Object.entries(flattened.payload).forEach(([key, value]) => {
        flattened[`payload.${key}` as const] = value ?? '';
      });
      return flattened;
    },
    []
  );

  const rowsWithBlank = useMemo(() => {
  const last = localRows[localRows.length - 1];
  const needsBlank = !last || Object.values(last.payload).some(v => v !== '' && v !== null && v !== undefined);
    if (needsBlank) {
      return [...localRows, createEmptyRow()];
    }
    return localRows;
  }, [localRows, createEmptyRow]);

  // Transform rows for ag-grid (flatten payload)
  const gridRows = useMemo<GridRowData[]>(() => rowsWithBlank.map(toGridRow), [rowsWithBlank, toGridRow]);

  const onGridReady = useCallback((event: GridReadyEvent<GridRowData>) => {
    setGridApi(event.api);

    requestAnimationFrame(() => {
      event.api.sizeColumnsToFit();
    });
  }, []);

  useEffect(() => {
    if (!gridApi) {
      return;
    }

    requestAnimationFrame(() => {
      gridApi.sizeColumnsToFit();
    });
  }, [gridApi, fields.length, rowsWithBlank.length]);

  useEffect(() => {
    if (!gridApi || typeof window === 'undefined' || !('ResizeObserver' in window)) {
      return;
    }

    const observer = new ResizeObserver(() => {
      requestAnimationFrame(() => {
        gridApi.sizeColumnsToFit();
      });
    });

    if (gridContainerRef.current) {
      observer.observe(gridContainerRef.current);
    }

    return () => {
      observer.disconnect();
    };
  }, [gridApi]);

  const saveRow = useCallback(
    async (rowData: GridRowData, options: SaveRowOptions = {}): Promise<SaveRowResult> => {
      const { skipRefresh = false, suppressSuccessToast = false, suppressErrorToast = false } = options;
      try {
        setIsSaving(true);
        const payload = fields.reduce<ConstructedRowPayload>((acc, field) => {
          const flattenedKey = `payload.${field.name}` as const;
          const value = rowData.payload?.[field.name] ?? rowData[flattenedKey] ?? '';
          acc[field.name] = value;
          return acc;
        }, {});

        // Validate on server
        const validation = await batchSaveConstructedData(constructedTableId, { rows: [payload], validateOnly: true });
        if (!validation.success) {
          if (!suppressErrorToast) {
            toast.showToast('Validation failed for row. Fix errors before continuing.', 'warning');
          }
          return { success: false, error: 'Validation failed for row. Fix errors before continuing.' };
        }

        const isNewRow = rowData.isNew ?? String(rowData.id).startsWith(emptyRowIdPrefix);
        const targetTableId = rowData.constructedTableId ?? constructedTableId;

        if (isNewRow) {
          const created = await createConstructedData({
            constructedTableId: targetTableId,
            payload,
            rowIdentifier: rowData.rowIdentifier ?? undefined,
          });
          setLocalRows((prev) => prev.map((r) => (r.id === rowData.id ? { ...created, isNew: false } : r)));
          if (!suppressSuccessToast) {
            toast.showToast('Row created', 'success');
          }
        } else {
          await updateConstructedData(rowData.id, { payload });
          setLocalRows((prev) => prev.map((r) => (r.id === rowData.id ? { ...r, payload, isNew: false } : r)));
          if (!suppressSuccessToast) {
            toast.showToast('Row saved', 'success');
          }
        }

        if (!skipRefresh) {
          onDataChange();
        }
        return { success: true };
      } catch (rawError) {
        const errorMessage =
          (rawError as { response?: { data?: { detail?: string } } }).response?.data?.detail ??
          (rawError instanceof Error ? rawError.message : 'Failed to save row');
        if (!suppressErrorToast) {
          toast.showToast(errorMessage, 'error');
        }
        return { success: false, error: errorMessage };
      } finally {
        setIsSaving(false);
      }
    },
    [constructedTableId, fields, toast, onDataChange]
  );

  const onRowValueChanged = useCallback(async (event: RowValueChangedEvent<GridRowData>) => {
    if (!event.data) {
      return;
    }
    await saveRow(event.data);
  }, [saveRow]);

  const handleClipboardPaste = useCallback((event: ClipboardEvent) => {
    if (!gridApi) {
      return;
    }

    const clipboardText = event.clipboardData?.getData('text/plain');
    if (!clipboardText || !clipboardText.trim() || dataColumnIds.length === 0) {
      return;
    }

    const focusedCell = gridApi.getFocusedCell();
    if (!focusedCell) {
      return;
    }

    event.preventDefault();

    const startRowIndex = focusedCell.rowIndex ?? 0;
    let startColIndex = dataColumnIds.indexOf(focusedCell.column.getColId());
    if (startColIndex < 0) {
      startColIndex = 0;
    }

    const parsedRows = clipboardText
      .split(/\r?\n/)
      .map(row => row.trimEnd())
      .filter(row => row.length > 0)
      .map(row => row.split('\t'));

    if (parsedRows.length === 0) {
      return;
    }

    type PastedRow = {
      data: GridRowData;
      label: number;
      targetId: string;
      previous: EditableConstructedRow;
    };
    const rowsToPersist: PastedRow[] = [];

    setLocalRows((prev) => {
      const next = [...prev];

      const ensureRowAtIndex = (index: number): EditableConstructedRow => {
        if (index < next.length) {
          return next[index];
        }

        while (next.length <= index) {
          next.push(createEmptyRow());
        }
        return next[index];
      };

      parsedRows.forEach((pastedRow, rowOffset) => {
        const targetIndex = startRowIndex + rowOffset;
        const targetRow = ensureRowAtIndex(targetIndex);
        const previousSnapshot = toUndoRow(targetRow);
  const updatedPayload: ConstructedRowPayload = { ...targetRow.payload };
        let hasChanges = false;

        pastedRow.forEach((cellValue, colOffset) => {
          const columnId = dataColumnIds[startColIndex + colOffset];
          if (!columnId) {
            return;
          }

          const fieldName = columnId.slice('payload.'.length);
          if (updatedPayload[fieldName] !== cellValue) {
            updatedPayload[fieldName] = cellValue;
            hasChanges = true;
          }
        });

        if (!hasChanges) {
          return;
        }

        const updatedRow: EditableConstructedRow = {
          ...targetRow,
          payload: updatedPayload,
        };

        next[targetIndex] = updatedRow;
        rowsToPersist.push({
          data: toGridRow(updatedRow),
          label: rowOffset + 1,
          targetId: updatedRow.id,
          previous: previousSnapshot,
        });
      });

      return next;
    });

    if (rowsToPersist.length > 0) {
      (async () => {
        let successCount = 0;
        const failures: string[] = [];

        for (const item of rowsToPersist) {
          const result = await saveRow(item.data, {
            skipRefresh: true,
            suppressSuccessToast: true,
            suppressErrorToast: true,
          });

          if (result.success) {
            successCount += 1;
          } else {
            failures.push(result.error ? `Row ${item.label}: ${result.error}` : `Row ${item.label} failed to paste`);
            setLocalRows((prev) => prev.map((r) => (r.id === item.targetId ? item.previous : r)));
          }
        }

        if (successCount > 0) {
          onDataChange();
          toast.showToast(
            successCount === 1 ? '1 row pasted' : `${successCount} rows pasted`,
            'success'
          );
        }

        if (failures.length > 0) {
          const summary = failures.length === 1 ? failures[0] : `${failures.length} rows failed to paste.`;
          toast.showToast(summary, 'warning');
        }
      })();
    }
  }, [gridApi, dataColumnIds, createEmptyRow, saveRow, onDataChange, toast, toGridRow, toUndoRow]);

  useEffect(() => {
    const container = gridContainerRef.current;
    if (!container) {
      return;
    }

    const listener = (event: ClipboardEvent) => {
      handleClipboardPaste(event);
    };

    container.addEventListener('paste', listener);
    return () => {
      container.removeEventListener('paste', listener);
    };
  }, [handleClipboardPaste]);

  // Build column definitions
  const selectionColumn: ColDef<GridRowData> = useMemo(() => ({
    colId: 'select',
    headerName: '',
    width: 48,
    minWidth: 40,
    maxWidth: 56,
    checkboxSelection: true,
    headerCheckboxSelection: true,
    sortable: false,
    filter: false,
    resizable: false,
    pinned: 'left',
    suppressMenu: true,
    suppressSizeToFit: true,
    suppressAutoSize: true,
  }), []);

  const handleDeleteClick = useCallback(async (params: DeleteRendererParams) => {
    const rowData = params.data;
    if (!rowData) {
      return;
    }
    if (rowData.isNew) {
      const undoRow = toUndoRow(rowData);
      setLocalRows((prev) => prev.filter((r) => r.id !== rowData.id));
      if (gridApi) {
        gridApi.applyTransaction({ remove: [rowData] });
      }
      setUndoState({ rows: [undoRow] });
      return;
    }
    setDeleteTarget(rowData);
  }, [gridApi, toUndoRow]);

  const handleConfirmDelete = useCallback(async () => {
    if (!deleteTarget) {
      return;
    }

    const undoRow = toUndoRow(deleteTarget);

    try {
      setIsDeleting(true);
      await deleteConstructedData(deleteTarget.id);
      setLocalRows((prev) => prev.filter((r) => r.id !== deleteTarget.id));
      if (gridApi) {
        gridApi.applyTransaction({ remove: [deleteTarget] });
      }
      setUndoState({ rows: [undoRow] });
      toast.showToast('Row deleted', 'success');
      onDataChange();
    } catch (rawError) {
      const message = rawError instanceof Error ? rawError.message : 'Failed to delete row';
      toast.showToast(message, 'error');
    } finally {
      setIsDeleting(false);
      setDeleteTarget(null);
    }
  }, [deleteTarget, gridApi, onDataChange, toast, toUndoRow]);

  const handleCancelDelete = useCallback(() => {
    if (isDeleting) {
      return;
    }
    setDeleteTarget(null);
  }, [isDeleting]);

  const onSelectionChanged = useCallback((event: SelectionChangedEvent<GridRowData>) => {
    setSelectedRowCount(event.api.getSelectedNodes().length);
  }, []);

  const handleBulkDeleteRequest = useCallback(() => {
    if (!gridApi) {
      return;
    }
    const selectedNodes = gridApi.getSelectedNodes();
    if (!selectedNodes.length) {
      return;
    }
    const targets = selectedNodes
      .map((node) => node.data)
      .filter((row): row is GridRowData => Boolean(row));
    if (targets.length === 0) {
      return;
    }
    setBulkDeleteTargets(targets);
  }, [gridApi]);

  const handleConfirmBulkDelete = useCallback(async () => {
    if (!bulkDeleteTargets.length) {
      return;
    }

    const targets = [...bulkDeleteTargets];
    const undoRows = targets.map((target) => toUndoRow(target));
    const idsToRemove = new Set(targets.map((target) => target.id));
    const persistedTargets = targets.filter((target) => !String(target.id).startsWith(emptyRowIdPrefix));
    const unsavedUndoRows = targets
      .filter((target) => String(target.id).startsWith(emptyRowIdPrefix))
      .map((target) => toUndoRow(target));

    try {
      setIsBulkDeleting(true);
      setLocalRows((prev) => prev.filter((row) => !idsToRemove.has(row.id)));
      if (gridApi) {
        gridApi.applyTransaction({ remove: targets });
        gridApi.deselectAll();
      }
      setSelectedRowCount(0);

      if (persistedTargets.length > 0) {
        for (const row of persistedTargets) {
          await deleteConstructedData(row.id);
        }
        onDataChange();
      }

      const message = persistedTargets.length > 1
        ? `${persistedTargets.length} rows deleted`
        : persistedTargets.length === 1
          ? 'Row deleted'
          : 'Rows removed';
      toast.showToast(message, 'success');
      setUndoState({ rows: undoRows });
    } catch (rawError) {
      const message = rawError instanceof Error ? rawError.message : 'Failed to delete selected rows';
      toast.showToast(message, 'error');
      if (unsavedUndoRows.length) {
        setLocalRows((prev) => [...prev, ...unsavedUndoRows]);
      }
      onDataChange();
    } finally {
      setIsBulkDeleting(false);
      setBulkDeleteTargets([]);
    }
  }, [bulkDeleteTargets, gridApi, onDataChange, toast, toUndoRow]);

  const handleCancelBulkDelete = useCallback(() => {
    if (isBulkDeleting) {
      return;
    }
    setBulkDeleteTargets([]);
  }, [isBulkDeleting]);

  const handleUndoRestore = useCallback(async () => {
    if (!undoState || isRestoring) {
      return;
    }

    try {
      setIsRestoring(true);
      const rowsToRestore = undoState.rows;
      if (!rowsToRestore.length) {
        return;
      }

      const unsavedRows = rowsToRestore.filter(row => String(row.id).startsWith(emptyRowIdPrefix));
      const persistedRows = rowsToRestore.filter(row => !String(row.id).startsWith(emptyRowIdPrefix));

      if (unsavedRows.length > 0) {
        setLocalRows((prev) => {
          const existingIds = new Set(prev.map((r) => r.id));
          const restored: EditableConstructedRow[] = unsavedRows.map((row) => {
            let rowId = row.id;
            if (existingIds.has(rowId)) {
              rowId = `${emptyRowIdPrefix}${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
            }
            return {
              ...row,
              id: rowId,
              isNew: true,
            };
          });
          return [...prev, ...restored];
        });
      }

      let restoredPersisted = 0;
      let failedPersisted = 0;
      for (const row of persistedRows) {
        try {
          await createConstructedData({
            constructedTableId: row.constructedTableId,
            payload: row.payload,
            rowIdentifier: row.rowIdentifier ?? undefined,
          });
          restoredPersisted += 1;
        } catch (error) {
          failedPersisted += 1;
        }
      }

      if (restoredPersisted > 0) {
        onDataChange();
      }

      const restoredCount = unsavedRows.length + restoredPersisted;
      if (restoredCount > 0) {
        toast.showToast(
          restoredCount === 1 ? 'Row restored' : `${restoredCount} rows restored`,
          'success'
        );
      }

      if (failedPersisted > 0) {
        toast.showToast(
          failedPersisted === 1 ? 'Failed to restore 1 row' : `Failed to restore ${failedPersisted} rows`,
          'error'
        );
      }
    } finally {
      setUndoState(null);
      setIsRestoring(false);
    }
  }, [undoState, isRestoring, onDataChange, toast]);

  const handleSnackbarClose = useCallback((_: Event | React.SyntheticEvent | undefined, reason?: string) => {
    if (reason === 'clickaway' || isRestoring) {
      return;
    }
    setUndoState(null);
  }, [isRestoring]);

  const columnDefs = useMemo<ColDef<GridRowData>[]>(() => {
    const cols: ColDef<GridRowData>[] = [
      selectionColumn,
      {
        colId: 'actions',
        headerName: 'Actions',
        width: 72,
        minWidth: 60,
        maxWidth: 90,
        sortable: false,
        filter: false,
        resizable: false,
        suppressMenu: true,
        cellRenderer: DeleteButtonRenderer,
        cellRendererParams: {
          onClick: handleDeleteClick,
        },
        pinned: 'left',
        cellClass: 'actions-cell',
        suppressSizeToFit: true,
        suppressAutoSize: true,
      },
    ];

    fields.forEach(f => {
      cols.push({
        field: `payload.${f.name}`,
        headerName: f.name + (f.isNullable ? '' : ' *'),
        flex: 1,
        minWidth: 160,
        editable: true,
        cellDataType: 'text',
        resizable: true,
        sortable: true,
        filter: 'agTextColumnFilter',
        enableRowGroup: true,
        enablePivot: true,
        enableValue: true,
        menuTabs: ['generalMenuTab', 'filterMenuTab', 'columnsMenuTab'],
      });
    });

    return cols;
  }, [fields, handleDeleteClick, selectionColumn]);

  const handleAddRow = useCallback(() => {
    const newRow = createEmptyRow();
    setLocalRows((prev) => [...prev, newRow]);
    if (gridApi) {
      const rowData = toGridRow(newRow);
      gridApi.applyTransaction({ add: [rowData], addIndex: gridRows.length });
      setTimeout(() => {
        const node = gridApi.getRowNode(String(newRow.id));
        if (node) {
          gridApi.ensureNodeVisible(node);
        }
      }, 100);
    }
  }, [createEmptyRow, gridApi, gridRows.length, toGridRow]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2 }}>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Data Rows</Typography>
          <Typography variant="body2" color="text.secondary">({dataRowCount} rows)</Typography>
          {dataRowCount === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ ml: 1 }}>
              No rows yet. Use Add Row to get started.
            </Typography>
          )}
        </Box>
        <Stack direction="row" spacing={1}>
          <Button
            size="small"
            variant="outlined"
            color="error"
            startIcon={<DeleteSweepIcon />}
            onClick={handleBulkDeleteRequest}
            disabled={selectedRowCount === 0 || isSaving || isDeleting || isBulkDeleting}
          >
            Delete Selected
          </Button>
          <Button
            size="small"
            variant="outlined"
            startIcon={<AddIcon />}
            onClick={handleAddRow}
            disabled={isSaving}
          >
            Add Row
          </Button>
        </Stack>
      </Box>

      <Box
        sx={{
          flex: 1,
          width: '100%',
          height: '60vh',
          minHeight: 320,
          maxHeight: '70vh',
          mb: 2,
          borderRadius: 3,
          boxShadow: theme.shadows[1],
          border: `1px solid ${alpha(theme.palette.divider, 0.25)}`,
          backgroundColor: alpha(theme.palette.background.default, 0.4),
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        <Box
          ref={gridContainerRef}
          className="ag-theme-quartz"
          sx={{
            flex: 1,
            overflow: 'auto',
            px: 2.5,
            pb: 2.5,
            '& .ag-root': {
              fontFamily: theme.typography.fontFamily,
              fontSize: '0.875rem',
              '--ag-background-color': theme.palette.background.paper,
              '--ag-foreground-color': theme.palette.text.primary,
              '--ag-border-color': alpha(theme.palette.divider, 0.4),
              '--ag-header-background-color': alpha(theme.palette.primary.main, 0.06),
              '--ag-header-foreground-color': theme.palette.text.primary,
              '--ag-row-hover-color': alpha(theme.palette.action.hover, 0.12),
              '--ag-selected-row-background-color': alpha(theme.palette.primary.main, 0.08),
              '--ag-header-cell-text-color': theme.palette.text.primary,
              '--ag-odd-row-background-color': theme.palette.background.paper,
              '--ag-header-height': '48px',
              '--ag-row-height': '40px',
              '--ag-wrapper-border-radius': theme.shape.borderRadius,
              '--ag-font-size': '0.9rem',
              minHeight: '100%',
            },
            '& .ag-root-wrapper': {
              height: '100%',
            },
            '& .ag-root-wrapper-body': {
              height: '100%',
            },
            '& .ag-center-cols-viewport': {
              overflowX: 'auto',
              overflowY: 'auto',
            },
            '& .ag-header': {
              position: 'sticky',
              top: 0,
              zIndex: theme.zIndex.appBar,
              borderBottom: `2px solid ${alpha(theme.palette.divider, 0.5)}`,
            },
            '& .ag-header-cell': {
              color: theme.palette.text.primary,
              fontWeight: 600,
              padding: '12px',
              display: 'flex',
              alignItems: 'center',
              backgroundColor: alpha(theme.palette.primary.main, 0.06),
              borderRight: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
              '&:hover': {
                backgroundColor: alpha(theme.palette.primary.main, 0.12),
              },
            },
            '& .ag-row': {
              borderBottom: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
              '&:hover': {
                backgroundColor: alpha(theme.palette.action.hover, 0.12),
              },
              '&.ag-row-selected': {
                backgroundColor: alpha(theme.palette.primary.main, 0.08),
                '& .ag-cell': {
                  backgroundColor: alpha(theme.palette.primary.main, 0.08),
                },
              },
            },
            '& .ag-cell': {
              padding: '10px 12px',
              display: 'flex',
              alignItems: 'center',
              borderRight: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
              '&.actions-cell': {
                justifyContent: 'center',
              },
            },
            '& .ag-cell-focus': {
              outline: 'none',
            },
            '& .ag-input-field-input': {
              padding: '6px 8px',
              fontSize: '0.875rem',
              border: `1px solid ${alpha(theme.palette.primary.main, 0.3)}`,
              borderRadius: '4px',
              fontFamily: theme.typography.fontFamily,
              '&:focus': {
                outline: 'none',
                borderColor: theme.palette.primary.main,
                boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.1)}`,
              },
            },
          }}
        >
          <AgGridReact<GridRowData>
            columnDefs={columnDefs}
            rowData={gridRows}
            onGridReady={onGridReady}
            onRowValueChanged={onRowValueChanged}
            onSelectionChanged={onSelectionChanged}
            domLayout="normal"
          editType="fullRow"
          stopEditingWhenCellsLoseFocus={true}
          undoRedoCellEditing={true}
          undoRedoCellEditingLimit={20}
          rowSelection="multiple"
          rowMultiSelectWithClick={true}
          suppressRowClickSelection={true}
          animateRows={true}
          suppressColumnVirtualisation={false}
          suppressRowVirtualisation={false}
          columnHoverHighlight={true}
          enableRangeSelection={true}
          enableCellTextSelection={true}
          enableCharts={true}
          allowContextMenuWithControlKey={true}
          rowGroupPanelShow="always"
          pivotPanelShow="always"
          sideBar={{
            toolPanels: [
              {
                id: 'columns',
                labelDefault: 'Columns',
                labelKey: 'columns',
                iconKey: 'columns',
                toolPanel: 'agColumnsToolPanel',
              },
              {
                id: 'filters',
                labelDefault: 'Filters',
                labelKey: 'filters',
                iconKey: 'filter',
                toolPanel: 'agFiltersToolPanel',
              },
            ],
            defaultToolPanel: 'columns',
          }}
          statusBar={{
            statusPanels: [
              { statusPanel: 'agTotalRowCountComponent', align: 'left' },
              { statusPanel: 'agFilteredRowCountComponent' },
              { statusPanel: 'agAggregationComponent', align: 'right' },
            ]
          }}
          defaultColDef={{
            resizable: true,
            sortable: true,
            filter: 'agTextColumnFilter',
            floatingFilter: true,
            flex: 1,
            enableRowGroup: true,
            enablePivot: true,
            enableValue: true,
            filterParams: {
              buttons: ['apply', 'reset'],
              debounceMs: 200,
            },
          }}
          headerHeight={48}
          rowHeight={40}
          suppressMovableColumns={false}
          suppressDragLeaveHidesColumns={false}
          rowBuffer={10}
          paginationPageSize={50}
        />
      </Box>

      </Box>

      {isSaving && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'info.main' }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Saving...</Typography>
        </Box>
      )}

      <Dialog
        open={Boolean(deleteTarget)}
        onClose={handleCancelDelete}
        maxWidth="xs"
        fullWidth
      >
        <DialogTitle sx={{ fontWeight: 600 }}>Delete Row</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary">
            This row will be permanently removed. This action cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelDelete} disabled={isDeleting} variant="outlined" size="small">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmDelete}
            color="error"
            variant="contained"
            size="small"
            disabled={isDeleting}
          >
            {isDeleting ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={bulkDeleteTargets.length > 0}
        onClose={handleCancelBulkDelete}
        maxWidth="xs"
        fullWidth
      >
        <DialogTitle sx={{ fontWeight: 600 }}>Delete Selected Rows</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary">
            {`You are about to delete ${bulkDeleteTargets.length} selected row${bulkDeleteTargets.length === 1 ? '' : 's'}. This action cannot be undone.`}
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelBulkDelete} disabled={isBulkDeleting} variant="outlined" size="small">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmBulkDelete}
            color="error"
            variant="contained"
            size="small"
            disabled={isBulkDeleting}
          >
            {isBulkDeleting ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={Boolean(undoState)}
        autoHideDuration={6000}
        onClose={handleSnackbarClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        message={undoState ? (undoState.rows.length === 1 ? 'Row deleted' : `${undoState.rows.length} rows deleted`) : undefined}
        action={
          <Button color="secondary" size="small" onClick={handleUndoRestore} disabled={isRestoring}>
            {isRestoring ? 'Restoring…' : 'Undo'}
          </Button>
        }
      />
    </Box>
  );
};

export default ConstructedDataGridAgGrid;
