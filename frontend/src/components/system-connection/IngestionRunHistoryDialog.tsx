import React from 'react';
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';

import { IngestionRun, IngestionSchedule } from '../../types/data';
import { useToast } from '../../hooks/useToast';

interface IngestionRunHistoryDialogProps {
  open: boolean;
  schedule: IngestionSchedule | null;
  runs: IngestionRun[];
  loading: boolean;
  onClose: () => void;
  onRefresh: () => void;
}

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '—';
  }
  return date.toLocaleString();
};

const getStatusColor = (status: IngestionRun['status']) => {
  switch (status) {
    case 'completed':
      return 'success';
    case 'failed':
      return 'error';
    case 'running':
      return 'warning';
    default:
      return 'default';
  }
};

const formatRowsDisplay = (run: IngestionRun) => {
  const { rowsLoaded, rowsExpected } = run;
  if (rowsExpected === undefined || rowsExpected === null) {
    return rowsLoaded ?? '—';
  }
  const loaded = rowsLoaded ?? 0;
  return `${loaded} / ${rowsExpected}`;
};

const IngestionRunHistoryDialog: React.FC<IngestionRunHistoryDialogProps> = ({
  open,
  schedule,
  runs,
  loading,
  onClose,
  onRefresh
}) => {
  const toast = useToast();

  const handleCopyQuery = (query?: string | null) => {
    if (!query) {
      toast.showInfo('No query captured for this run.');
      return;
    }
    void navigator.clipboard.writeText(query).then(
      () => toast.showSuccess('Query copied to clipboard.'),
      () => toast.showError('Unable to copy query to clipboard.')
    );
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Run history
            </Typography>
            {schedule && (
              <Typography variant="body2" color="text.secondary">
                {schedule.targetTableName ?? 'Target table not set'} — cron: {schedule.scheduleExpression}
              </Typography>
            )}
          </Box>
          <Tooltip title="Refresh runs" arrow>
            <span>
              <IconButton onClick={onRefresh} disabled={loading} size="small">
                <RefreshIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </Box>
      </DialogTitle>
      <DialogContent dividers>
        {runs.length === 0 && !loading ? (
          <Typography variant="body2" color="text.secondary">
            No runs recorded yet for this schedule.
          </Typography>
        ) : (
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Status</TableCell>
                  <TableCell>Started</TableCell>
                  <TableCell>Completed</TableCell>
                  <TableCell>Rows (loaded / expected)</TableCell>
                  <TableCell>Watermark</TableCell>
                  <TableCell>Query</TableCell>
                  <TableCell>Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {runs.map((run) => (
                  <TableRow key={run.id} hover>
                    <TableCell>
                      <Chip label={run.status} size="small" color={getStatusColor(run.status)} />
                    </TableCell>
                    <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                    <TableCell>{formatDateTime(run.completedAt)}</TableCell>
                    <TableCell>{formatRowsDisplay(run)}</TableCell>
                    <TableCell>
                      {run.watermarkTimestampAfter
                        ? formatDateTime(run.watermarkTimestampAfter)
                        : run.watermarkIdAfter ?? '—'}
                    </TableCell>
                    <TableCell>
                      {run.queryText ? (
                        <Tooltip title="Copy query" arrow>
                          <IconButton size="small" onClick={() => handleCopyQuery(run.queryText)}>
                            <ContentCopyIcon fontSize="inherit" />
                          </IconButton>
                        </Tooltip>
                      ) : (
                        <Typography variant="caption" color="text.secondary">
                          —
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      {run.errorMessage ? (
                        <Typography variant="body2" color="error" sx={{ maxWidth: 260 }}>
                          {run.errorMessage}
                        </Typography>
                      ) : (
                        <Typography variant="caption" color="text.secondary">
                          —
                        </Typography>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default IngestionRunHistoryDialog;
