import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Box, Typography, Button, Stack, CircularProgress } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, GridApi, GridReadyEvent, CellEditingStoppedEvent, RowClickedEvent } from 'ag-grid-community';
import { alpha, useTheme } from '@mui/material/styles';

import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css';

import { ConstructedField, ConstructedData, batchSaveConstructedData, createConstructedData, updateConstructedData, deleteConstructedData } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
}

const emptyRowIdPrefix = 'new-';

// Custom renderer for delete button
const DeleteButtonRenderer = (props: any) => {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', height: '100%', cursor: 'pointer' }}>
      <DeleteIcon fontSize="small" />
    </Box>
  );
};

const ConstructedDataGridAgGrid: React.FC<Props> = ({ constructedTableId, fields, rows, onDataChange }) => {
  const theme = useTheme();
  const toast = useToast();
  const [gridApi, setGridApi] = useState<GridApi | null>(null);
  const [localRows, setLocalRows] = useState<ConstructedData[]>(() => rows.map(r => ({ ...r })));
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    setLocalRows(rows.map(r => ({ ...r })));
  }, [rows]);

  // Ensure there's always a blank row at the end
  const rowsWithBlank = useMemo(() => {
    const last = localRows[localRows.length - 1];
    const needsBlank = !last || Object.values(last.payload || {}).some(v => v !== '' && v !== null && v !== undefined);
    if (needsBlank) {
      const blank: ConstructedData = {
        id: `${emptyRowIdPrefix}${Date.now()}`,
        constructedTableId,
        payload: fields.reduce((acc, f) => ({ ...acc, [f.name]: '' }), {}),
        isNew: true,
      } as ConstructedData;
      return [...localRows, blank];
    }
    return localRows;
  }, [localRows, fields, constructedTableId]);

  // Build column definitions
  const columnDefs = useMemo<ColDef[]>(() => {
    const cols: ColDef[] = [
      {
        field: 'actions',
        headerName: 'Actions',
        width: 50,
        sortable: false,
        filter: false,
        resizable: false,
        cellRenderer: DeleteButtonRenderer,
        cellRendererParams: {
          onClick: handleDeleteClick,
        },
        pinned: 'left',
      },
    ];

    fields.forEach(f => {
      cols.push({
        field: `payload.${f.name}`,
        headerName: f.name + (f.isNullable ? '' : ' *'),
        flex: 1,
        minWidth: 150,
        editable: true,
        cellDataType: 'text',
        resizable: true,
        sortable: true,
        filter: true,
      });
    });

    return cols;
  }, [fields]);

  // Transform rows for ag-grid (flatten payload)
  const gridRows = useMemo(() => {
    return rowsWithBlank.map(r => ({
      id: r.id,
      payload: r.payload || {},
      ...Object.keys(r.payload || {}).reduce((acc, key) => ({
        ...acc,
        [`payload.${key}`]: r.payload?.[key] ?? '',
      }), {}),
      isNew: String(r.id).startsWith(emptyRowIdPrefix),
    }));
  }, [rowsWithBlank]);

  const onGridReady = useCallback((event: GridReadyEvent) => {
    setGridApi(event.api);
  }, []);

  const saveRow = useCallback(async (rowData: any) => {
    try {
      setIsSaving(true);
      const payload = fields.reduce((acc, f) => ({
        ...acc,
        [f.name]: rowData[`payload.${f.name}`] ?? '',
      }), {});

      // Validate on server
      const validation = await batchSaveConstructedData(constructedTableId, { rows: [payload], validateOnly: true });
      if (!validation.success) {
        toast.showToast('Validation failed for row. Fix errors before continuing.', 'warning');
        return { success: false };
      }

      if (rowData.isNew) {
        const created = await createConstructedData({ constructedTableId, payload });
        setLocalRows(prev => prev.map(r => (r.id === rowData.id ? created : r)));
        toast.showToast('Row created', 'success');
      } else {
        await updateConstructedData(rowData.id, { payload });
        toast.showToast('Row saved', 'success');
      }

      onDataChange();
      return { success: true };
    } catch (e: any) {
      toast.showToast(e?.message || 'Failed to save row', 'error');
      return { success: false };
    } finally {
      setIsSaving(false);
    }
  }, [constructedTableId, fields, toast, onDataChange]);

  const onCellEditingStopped = useCallback(async (event: CellEditingStoppedEvent) => {
    const rowData = event.data;
    await saveRow(rowData);
  }, [saveRow]);

  function handleDeleteClick(params: any) {
    const rowData = params.data;
    if (rowData.isNew) {
      setLocalRows(prev => prev.filter(r => r.id !== rowData.id));
      if (gridApi) {
        gridApi.applyTransaction({ remove: [rowData] });
      }
      return;
    }

    if (window.confirm('Are you sure you want to delete this row?')) {
      deleteConstructedData(rowData.id)
        .then(() => {
          setLocalRows(prev => prev.filter(r => r.id !== rowData.id));
          if (gridApi) {
            gridApi.applyTransaction({ remove: [rowData] });
          }
          toast.showToast('Row deleted', 'success');
          onDataChange();
        })
        .catch((e: any) => {
          toast.showToast(e?.message || 'Failed to delete row', 'error');
        });
    }
  }

  const handleAddRow = useCallback(() => {
    const newRow: ConstructedData = {
      id: `${emptyRowIdPrefix}${Date.now()}`,
      constructedTableId,
      payload: fields.reduce((acc, f) => ({ ...acc, [f.name]: '' }), {}),
    } as ConstructedData;
    setLocalRows(prev => [...prev, newRow]);
    if (gridApi) {
      const rowData = {
        id: newRow.id,
        payload: newRow.payload || {},
        ...Object.keys(newRow.payload || {}).reduce((acc, key) => ({
          ...acc,
          [`payload.${key}`]: '',
        }), {}),
        isNew: true,
      };
      gridApi.applyTransaction({ add: [rowData], addIndex: gridRows.length });
      setTimeout(() => {
        const node = gridApi.getRowNode(String(newRow.id));
        if (node) {
          gridApi.ensureNodeVisible(node);
        }
      }, 100);
    }
  }, [constructedTableId, fields, gridApi, gridRows.length]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2 }}>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Data Rows</Typography>
          <Typography variant="body2" color="text.secondary">({localRows.length} rows)</Typography>
        </Box>
        <Button
          size="small"
          variant="outlined"
          startIcon={<AddIcon />}
          onClick={handleAddRow}
          disabled={isSaving}
        >
          Add Row
        </Button>
      </Box>

      <Box
        className="ag-theme-quartz"
        sx={{
          flex: 1,
          width: '100%',
          minHeight: 500,
          mb: 2,
          border: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
          borderRadius: '4px',
          overflow: 'hidden',
          '& .ag-root': {
            fontFamily: theme.typography.fontFamily,
            fontSize: '0.875rem',
            '--ag-background-color': theme.palette.background.paper,
            '--ag-foreground-color': theme.palette.text.primary,
            '--ag-border-color': alpha(theme.palette.divider, 0.4),
            '--ag-header-background-color': alpha(theme.palette.primary.main, 0.06),
            '--ag-header-foreground-color': theme.palette.text.primary,
            '--ag-row-hover-color': alpha(theme.palette.action.hover, 0.6),
            '--ag-selected-row-background-color': alpha(theme.palette.primary.main, 0.08),
            '--ag-header-cell-text-color': theme.palette.text.primary,
            '--ag-odd-row-background-color': theme.palette.background.paper,
          },
          '& .ag-header': {
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
          '& .ag-header-cell-sortable': {
            cursor: 'pointer',
          },
          '& .ag-row': {
            borderBottom: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
            '&:hover': {
              backgroundColor: alpha(theme.palette.action.hover, 0.6),
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
          },
          '& .ag-cell-focus': {
            outline: 'none',
          },
          '& .ag-cell-value': {
            color: theme.palette.text.primary,
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
          '& .ag-cell-inline-editing': {
            padding: '0',
            '& .ag-input-field-input': {
              height: '100%',
              margin: 0,
            },
          },
          '& .ag-checkbox-input-wrapper input': {
            accentColor: theme.palette.primary.main,
          },
          '& .ag-paging-panel': {
            borderTop: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
            backgroundColor: alpha(theme.palette.primary.main, 0.02),
            padding: '8px 12px',
          },
          '& .ag-status-bar': {
            borderTop: `1px solid ${alpha(theme.palette.divider, 0.3)}`,
            backgroundColor: alpha(theme.palette.primary.main, 0.02),
          },
        }}
      >
        <AgGridReact
          columnDefs={columnDefs}
          rowData={gridRows}
          onGridReady={onGridReady}
          onCellEditingStopped={onCellEditingStopped}
          domLayout="normal"
          editType="fullRow"
          stopEditingWhenCellsLoseFocus={true}
          undoRedoCellEditing={true}
          undoRedoCellEditingLimit={20}
          rowSelection="multiple"
          animateRows={true}
          suppressColumnVirtualisation={false}
          suppressRowVirtualisation={false}
          columnHoverHighlight={true}
          enableRangeSelection={true}
          enableCellTextSelection={true}
          allowContextMenuWithControlKey={true}
          statusBar={{
            statusPanels: [
              { statusPanel: 'agTotalRowCountComponent', align: 'left' },
              { statusPanel: 'agFilteredRowCountComponent' },
            ]
          }}
          defaultColDef={{
            resizable: true,
            sortable: true,
            filter: 'agTextColumnFilter',
            floatingFilter: true,
          }}
          headerHeight={40}
          rowHeight={36}
          suppressMovableColumns={false}
          suppressDragLeaveHidesColumns={false}
          rowBuffer={10}
          paginationPageSize={50}
        />
      </Box>

      {isSaving && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'info.main' }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Saving...</Typography>
        </Box>
      )}
    </Box>
  );
};

export default ConstructedDataGridAgGrid;
