import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography
} from '@mui/material';
import { useMemo } from 'react';

import { ConnectionTablePreview } from '../../types/data';

interface ConnectionDataPreviewDialogProps {
  open: boolean;
  schemaName: string | null;
  tableName: string;
  loading: boolean;
  error?: string | null;
  preview: ConnectionTablePreview | null;
  onClose: () => void;
  onRefresh: () => void;
}

const formatValue = (value: unknown): string => {
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
    } catch (error) {
      return String(value);
    }
  }
  return String(value);
};

const ConnectionDataPreviewDialog = ({
  open,
  schemaName,
  tableName,
  loading,
  error,
  preview,
  onClose,
  onRefresh
}: ConnectionDataPreviewDialogProps) => {
  const columns = preview?.columns ?? [];
  const rows = preview?.rows ?? [];

  const title = useMemo(() => {
    if (!schemaName || schemaName.length === 0) {
      return tableName;
    }
    return `${schemaName}.${tableName}`;
  }, [schemaName, tableName]);

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="lg">
      <DialogTitle>Preview — {title}</DialogTitle>
      <DialogContent dividers sx={{ minHeight: 320 }}>
        {loading && (
          <Box display="flex" justifyContent="center" alignItems="center" height="100%">
            <CircularProgress />
          </Box>
        )}

        {!loading && error && (
          <Alert severity="error" action={<Button color="inherit" size="small" onClick={onRefresh}>Retry</Button>}>
            {error}
          </Alert>
        )}

        {!loading && !error && columns.length === 0 && (
          <Typography variant="body2" color="text.secondary">
            No data returned. The table may be empty or inaccessible.
          </Typography>
        )}

        {!loading && !error && columns.length > 0 && (
          <TableContainer sx={{ maxHeight: 420 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  {columns.map((column) => (
                    <TableCell key={column} sx={{ fontWeight: 600 }}>
                      {column}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {rows.map((row, index) => (
                  <TableRow key={index} hover>
                    {columns.map((column) => (
                      <TableCell key={column} sx={{ whiteSpace: 'nowrap' }}>
                        {formatValue((row as Record<string, unknown>)[column])}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onRefresh} disabled={loading} variant="outlined">
          Refresh
        </Button>
        <Button onClick={onClose} variant="contained">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ConnectionDataPreviewDialog;
