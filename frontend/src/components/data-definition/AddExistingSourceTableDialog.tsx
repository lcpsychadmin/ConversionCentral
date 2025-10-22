import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Autocomplete,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
  CircularProgress,
  Alert,
  Checkbox,
  FormControlLabel,
  Box,
  List,
  ListItem,
  ListItemIcon,
  ListItemText
} from '@mui/material';

import { AvailableSourceTable, fetchSourceTableColumns, SourceTableColumn } from '../../services/dataDefinitionService';

interface AddExistingSourceTableDialogProps {
  dataObjectId?: string;
  open: boolean;
  tables: AvailableSourceTable[];
  loading?: boolean;
  error?: string | null;
  onClose: () => void;
  onSubmit: (payload: {
    schemaName: string;
    tableName: string;
    tableType?: string | null;
    columnCount?: number | null;
    estimatedRows?: number | null;
    selectedColumns: SourceTableColumn[];
  }) => Promise<void> | void;
}

const AddExistingSourceTableDialog = ({
  dataObjectId,
  open,
  tables,
  loading = false,
  error,
  onClose,
  onSubmit
}: AddExistingSourceTableDialogProps) => {
  const [selectedTable, setSelectedTable] = useState<AvailableSourceTable | null>(null);
  const [columns, setColumns] = useState<SourceTableColumn[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());
  const [localError, setLocalError] = useState<string | null>(null);
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
        const cols = await fetchSourceTableColumns(
          dataObjectId,
          selectedTable.schemaName,
          selectedTable.tableName
        );
        setColumns(cols);
        // Auto-select all columns by default
        setSelectedColumns(new Set(cols.map((c) => c.name)));
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to load columns';
        setLocalError(message);
        setColumns([]);
        setSelectedColumns(new Set());
      } finally {
        setColumnsLoading(false);
      }
    };

    fetchColumns();
  }, [selectedTable, dataObjectId]);

  const tableOptions = useMemo(
    () =>
      tables
        .slice()
        .sort((a, b) => {
          const schemaCompare = (a.schemaName ?? '').localeCompare(b.schemaName ?? '');
          return schemaCompare !== 0 ? schemaCompare : (a.tableName ?? '').localeCompare(b.tableName ?? '');
        }),
    [tables]
  );

  const handleClose = () => {
    if (loading || columnsLoading) {
      return;
    }
    onClose();
  };

  const handleTableChange = (_: unknown, value: AvailableSourceTable | null) => {
    setSelectedTable(value);
    setColumns([]);
    setSelectedColumns(new Set());
    setLocalError(null);
  };

  const handleColumnToggle = (columnName: string) => {
    setSelectedColumns((prev) => {
      const next = new Set(prev);
      if (next.has(columnName)) {
        next.delete(columnName);
      } else {
        next.add(columnName);
      }
      return next;
    });
  };

  const handleSelectAll = () => {
    if (selectedColumns.size === columns.length) {
      setSelectedColumns(new Set());
    } else {
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

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Add Source Table to Definition</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2} mt={1}>
          {error && (
            <Alert severity="error">{error}</Alert>
          )}
          <Typography variant="body2" color="text.secondary">
            Available source tables from your system connections
          </Typography>
          <Autocomplete
            options={tableOptions}
            value={selectedTable}
            onChange={handleTableChange}
            getOptionLabel={(option) => `${option.schemaName}.${option.tableName}`}
            isOptionEqualToValue={(option, value) =>
              option.schemaName === value.schemaName && option.tableName === value.tableName
            }
            renderInput={(params) => (
              <TextField
                {...params}
                label="Source Table"
                placeholder={tableOptions.length ? 'Select table' : 'No tables available'}
                required
                error={Boolean(localError && !selectedTable)}
                helperText={localError && !selectedTable ? localError : ''}
              />
            )}
            disabled={loading || !tableOptions.length}
          />

          {selectedTable && (
            <Stack spacing={2}>
              <Stack spacing={1}>
                <Typography variant="caption" color="text.secondary">
                  <strong>Table Details:</strong>
                </Typography>
                {selectedTable.tableType && (
                  <Typography variant="body2" color="text.secondary">
                    Type: {selectedTable.tableType}
                  </Typography>
                )}
                {selectedTable.columnCount !== null && selectedTable.columnCount !== undefined && (
                  <Typography variant="body2" color="text.secondary">
                    Columns: {selectedTable.columnCount}
                  </Typography>
                )}
                {selectedTable.estimatedRows !== null && selectedTable.estimatedRows !== undefined && (
                  <Typography variant="body2" color="text.secondary">
                    Est. Rows: {new Intl.NumberFormat().format(selectedTable.estimatedRows)}
                  </Typography>
                )}
              </Stack>

              <Stack spacing={1}>
                <Typography variant="caption" color="text.secondary">
                  <strong>Columns to Import:</strong>
                </Typography>
                {columnsLoading ? (
                  <Box display="flex" justifyContent="center" py={2}>
                    <CircularProgress size={24} />
                  </Box>
                ) : columns.length > 0 ? (
                  <Box>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={selectedColumns.size === columns.length && columns.length > 0}
                          indeterminate={
                            selectedColumns.size > 0 && selectedColumns.size < columns.length
                          }
                          onChange={handleSelectAll}
                        />
                      }
                      label={`Select All (${selectedColumns.size}/${columns.length})`}
                    />
                    <List
                      dense
                      sx={{
                        maxHeight: 300,
                        overflow: 'auto',
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 1
                      }}
                    >
                      {columns.map((col) => (
                        <ListItem
                          key={col.name}
                          dense
                          onClick={() => handleColumnToggle(col.name)}
                          sx={{ cursor: 'pointer' }}
                        >
                          <ListItemIcon>
                            <Checkbox
                              edge="start"
                              checked={selectedColumns.has(col.name)}
                              tabIndex={-1}
                              disableRipple
                            />
                          </ListItemIcon>
                          <ListItemText
                            primary={col.name}
                            secondary={col.typeName}
                            primaryTypographyProps={{ variant: 'body2' }}
                            secondaryTypographyProps={{ variant: 'caption' }}
                          />
                        </ListItem>
                      ))}
                    </List>
                  </Box>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No columns available
                  </Typography>
                )}
              </Stack>
            </Stack>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={loading || columnsLoading}>
          Cancel
        </Button>
        <LoadingButton
          onClick={handleSubmit}
          variant="contained"
          loading={loading}
          disabled={loading || columnsLoading || !selectedTable || selectedColumns.size === 0}
        >
          Add Table with Fields
        </LoadingButton>
      </DialogActions>
    </Dialog>
  );
};

export default AddExistingSourceTableDialog;
