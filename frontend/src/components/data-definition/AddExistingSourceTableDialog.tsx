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
  Alert
} from '@mui/material';

import { AvailableSourceTable } from '../../services/dataDefinitionService';

interface AddExistingSourceTableDialogProps {
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
  }) => Promise<void> | void;
}

const AddExistingSourceTableDialog = ({
  open,
  tables,
  loading = false,
  error,
  onClose,
  onSubmit
}: AddExistingSourceTableDialogProps) => {
  const [selectedTable, setSelectedTable] = useState<AvailableSourceTable | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    setSelectedTable(null);
    setLocalError(null);
  }, [open, tables]);

  const tableOptions = useMemo(
    () =>
      tables
        .slice()
        .sort((a, b) => {
          const schemaCompare = a.schemaName.localeCompare(b.schemaName);
          return schemaCompare !== 0 ? schemaCompare : a.tableName.localeCompare(b.tableName);
        }),
    [tables]
  );

  const handleClose = () => {
    if (loading) {
      return;
    }
    onClose();
  };

  const handleSubmit = async () => {
    if (!selectedTable) {
      setLocalError('Select a table to add.');
      return;
    }

    setLocalError(null);
    await onSubmit({
      schemaName: selectedTable.schemaName,
      tableName: selectedTable.tableName,
      tableType: selectedTable.tableType,
      columnCount: selectedTable.columnCount,
      estimatedRows: selectedTable.estimatedRows
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
          {loading ? (
            <CircularProgress />
          ) : (
            <Autocomplete
              options={tableOptions}
              value={selectedTable}
              onChange={(_, value) => {
                setSelectedTable(value);
                setLocalError(null);
              }}
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
                  error={Boolean(localError)}
                  helperText={localError}
                />
              )}
              disabled={loading || !tableOptions.length}
            />
          )}
          {selectedTable && (
            <Stack spacing={1}>
              <Typography variant="caption" color="text.secondary">
                <strong>Details:</strong>
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
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={loading}>
          Cancel
        </Button>
        <LoadingButton
          onClick={handleSubmit}
          variant="contained"
          loading={loading}
          disabled={loading || !tableOptions.length}
        >
          Add Table
        </LoadingButton>
      </DialogActions>
    </Dialog>
  );
};

export default AddExistingSourceTableDialog;
