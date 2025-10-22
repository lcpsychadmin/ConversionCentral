import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Typography,
  Button,
  Alert
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import {
  DataGrid,
  GridColDef,
  GridRenderCellParams,
  GridRowClassNameParams,
  GridCellEditStopReasons,
  useGridApiRef
} from '@mui/x-data-grid';

import { ConstructedField, ConstructedData, batchSaveConstructedData, ValidationError, deleteConstructedData, createConstructedData, updateConstructedData } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
}

interface RowData {
  [key: string]: any;
}

interface RowState {
  rowId: string;
  isDirty: boolean;
  isNew: boolean;
  data: RowData;
  errors: ValidationError[];
  originalData: RowData;
}

const ConstructedDataGrid: React.FC<Props> = ({
  constructedTableId,
  fields,
  rows,
  onDataChange
}) => {
  const theme = useTheme();
  const toast = useToast();
  const apiRef = useGridApiRef();

  // State
  const [rowStates, setRowStates] = useState<Map<string, RowState>>(
    new Map(
      rows.map((row) => [
        row.id,
        {
          rowId: row.id,
          isDirty: false,
          isNew: false,
          data: row.payload,
          errors: [],
          originalData: row.payload
        }
      ])
    )
  );
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [deleteRowId, setDeleteRowId] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const pendingSaveRef = useRef<Set<string>>(new Set());

  // Sync external rows with state
  useEffect(() => {
    setRowStates(
      new Map(
        rows.map((row) => [
          row.id,
          {
            rowId: row.id,
            isDirty: false,
            isNew: false,
            data: { ...row.payload },
            errors: [],
            originalData: { ...row.payload }
          }
        ])
      )
    );
  }, [rows]);

  // Handle adding a new empty row
  const handleAddNewRow = useCallback(() => {
    const newRowId = `new-${Date.now()}`;
    const emptyData: RowData = {};
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
  const handleCellEditStop = useCallback(
    async (params: any) => {
      const { id: rowId, field: fieldName, value, reason } = params;
      const state = rowStates.get(rowId as string);

      if (!state) return;

      // Update the cell value
      setRowStates((prev) => {
        const newMap = new Map(prev);
        const updatedState = newMap.get(rowId as string);
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
        const currentState = rowStates.get(rowId as string);
        if (!currentState) return;

        // Update data with new value
        const updatedData = { ...currentState.data, [fieldName]: value };

        pendingSaveRef.current.add(rowId as string);
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
              const updatedState = newMap.get(rowId as string);
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
              newMap.delete(rowId as string);
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
          } else {
            // Update existing row
            await updateConstructedData(rowId as string, { payload: updatedData });
            setRowStates((prev) => {
              const newMap = new Map(prev);
              const updatedState = newMap.get(rowId as string);
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
        } catch (error: any) {
          toast.showError(error.message || 'Failed to save row');
        } finally {
          pendingSaveRef.current.delete(rowId as string);
          setIsSaving(false);
        }
      }
    },
    [rowStates, toast, onDataChange, constructedTableId]
  );

  const handleDeleteClick = (rowId: string) => {
    setDeleteRowId(rowId);
    setDeleteConfirmOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteRowId) return;

    try {
      await deleteConstructedData(deleteRowId);
      setRowStates((prev) => {
        const newMap = new Map(prev);
        newMap.delete(deleteRowId);
        return newMap;
      });
      toast.showSuccess('Row deleted successfully');
      onDataChange();
    } catch (error: any) {
      toast.showError(error.message || 'Failed to delete row');
    } finally {
      setDeleteConfirmOpen(false);
      setDeleteRowId(null);
    }
  };

  // Build DataGrid columns with inline editing
  const columns = useMemo<GridColDef[]>(() => {
    return [
      {
        field: 'actions',
        headerName: 'Actions',
        width: 80,
        sortable: false,
        filterable: false,
        disableColumnMenu: true,
        editable: false,
        renderCell: (params: GridRenderCellParams) => {
          const rowId = params.id as string;
          const state = rowStates.get(rowId);
          const rowErrors = (state?.errors || []).length > 0;

          return (
            <Tooltip title={rowErrors ? 'Has validation errors' : 'Delete row'}>
              <IconButton
                size="small"
                color="error"
                onClick={() => handleDeleteClick(rowId)}
                disabled={rowErrors}
              >
                <DeleteIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          );
        }
      },
      ...fields.map((field) => ({
        field: field.name,
        headerName: `${field.name}${!field.isNullable ? ' *' : ''}`,
        width: Math.max(150, field.name.length * 10),
        editable: true,
        sortable: true,
        filterable: true,
        renderCell: (params: GridRenderCellParams) => {
          const rowId = params.id as string;
          const state = rowStates.get(rowId);
          const fieldErrors = (state?.errors || []).filter((e) => e.fieldName === field.name);

          return (
            <Box sx={{ width: '100%', py: 0.5 }}>
              <Box
                sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  width: '100%'
                }}
              >
                <Typography
                  sx={{
                    fontSize: '0.875rem',
                    color: fieldErrors.length > 0 ? theme.palette.error.main : 'inherit'
                  }}
                >
                  {params.value || ''}
                </Typography>
                {fieldErrors.length > 0 && (
                  <Typography
                    sx={{
                      fontSize: '0.65rem',
                      color: theme.palette.error.main,
                      fontStyle: 'italic',
                      mt: 0.25
                    }}
                  >
                    ⚠ {fieldErrors[0].message}
                  </Typography>
                )}
              </Box>
            </Box>
          );
        }
      } as GridColDef))
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

  return (
    <>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Data Rows ({gridRows.filter(r => !r._isNew).length})
          </Typography>
          <Button
            size="small"
            startIcon={<AddIcon />}
            variant="contained"
            onClick={handleAddNewRow}
            disabled={isSaving}
          >
            Add New Row
          </Button>
        </Box>
        <Alert severity="info" sx={{ py: 1 }}>
          💡 Click any cell to edit directly. Changes auto-save when you move to another cell.
        </Alert>
      </Box>

      <Box
        sx={{
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
        }}
      >
        <DataGrid
          apiRef={apiRef}
          rows={gridRows}
          columns={columns}
          pageSizeOptions={[10, 25, 50, 100]}
          initialState={{
            pagination: {
              paginationModel: { pageSize: 25, page: 0 }
            }
          }}
          disableRowSelectionOnClick
          processRowUpdate={(updatedRow) => updatedRow}
          onCellEditStop={handleCellEditStop}
          getRowClassName={(params: GridRowClassNameParams) => {
            if (params.row._hasErrors) return 'row-error';
            if (params.row._isNew) return 'row-new';
            return '';
          }}
          sx={{
            '& .MuiDataGrid-virtualScroller': {
              minHeight: 300
            },
            '& .MuiDataGrid-columnHeaders': {
              backgroundColor: alpha(theme.palette.primary.main, 0.1)
            }
          }}
        />
      </Box>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteConfirmOpen} onClose={() => setDeleteConfirmOpen(false)}>
        <DialogTitle>Delete Row?</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <Typography>Are you sure you want to delete this row? This action cannot be undone.</Typography>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteConfirmOpen(false)}>Cancel</Button>
          <Button onClick={handleDeleteConfirm} color="error" variant="contained">
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default ConstructedDataGrid;
