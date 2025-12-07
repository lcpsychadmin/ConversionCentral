import {
  Alert,
  Box,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  LinearProgress,
  Stack,
  Typography,
  Button
} from '@mui/material';
import { formatDateTime } from '@pages/DataQualityProfileRunResultsPage';
import type { TableObservabilityMetric } from '@cc-types/data';

interface TableObservabilityMetricsDialogProps {
  open: boolean;
  tableLabel: string;
  metrics: TableObservabilityMetric[];
  loading: boolean;
  error: string | null;
  onClose: () => void;
  onRefresh: () => void;
}

const normalizeStatus = (status?: string | null): string => {
  if (!status) return 'not_configured';
  return status.replace(/_/g, ' ');
};

const pickStatusColor = (status?: string | null): 'default' | 'success' | 'warning' | 'error' | 'info' => {
  if (!status) {
    return 'default';
  }
  const normalized = status.toLowerCase();
  if (normalized === 'ok' || normalized === 'baseline') {
    return 'success';
  }
  if (normalized === 'warning') {
    return 'warning';
  }
  if (normalized === 'missing_data' || normalized === 'failed') {
    return 'error';
  }
  return 'info';
};

const formatMetricValue = (metric: TableObservabilityMetric): string => {
  if (metric.metricValueNumber !== undefined && metric.metricValueNumber !== null) {
    return metric.metricValueNumber.toLocaleString();
  }
  if (metric.metricValueText) {
    return metric.metricValueText;
  }
  if (metric.metricPayload && Object.keys(metric.metricPayload).length > 0) {
    return JSON.stringify(metric.metricPayload, null, 2);
  }
  return '—';
};

const TableObservabilityMetricsDialog = ({
  open,
  tableLabel,
  metrics,
  loading,
  error,
  onClose,
  onRefresh
}: TableObservabilityMetricsDialogProps) => (
  <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
    <DialogTitle>Table Observability Metrics</DialogTitle>
    <DialogContent dividers>
      <Stack spacing={2}>
        <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
          {tableLabel}
        </Typography>
        {loading && <LinearProgress />}
        {error && <Alert severity="error">{error}</Alert>}
        {!loading && !error && metrics.length === 0 && (
          <Alert severity="info">No observability metrics have been recorded for this table yet.</Alert>
        )}
        {!loading && !error && metrics.length > 0 && (
          <Stack spacing={2}>
            {metrics.map((metric) => {
              const statusLabel = normalizeStatus(metric.metricStatus);
              const payloadValue = formatMetricValue(metric);
              const isJsonPayload = payloadValue.startsWith('{') || payloadValue.startsWith('[');
              return (
                <Box
                  key={metric.metricId}
                  sx={{
                    borderRadius: 2,
                    border: (theme) => `1px solid ${theme.palette.divider}`,
                    p: 2
                  }}
                >
                  <Stack spacing={1.5}>
                    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ xs: 'flex-start', sm: 'center' }} justifyContent="space-between">
                      <Typography variant="h6">{metric.metricName}</Typography>
                      <Chip
                        label={statusLabel}
                        color={pickStatusColor(metric.metricStatus)}
                        size="small"
                        sx={{ textTransform: 'capitalize' }}
                      />
                    </Stack>
                    <Typography variant="body2" color="text.secondary">
                      Category: {metric.metricCategory.replace(/_/g, ' ')} · Recorded {formatDateTime(metric.recordedAt)}
                    </Typography>
                    <Typography variant="body2">
                      Value:
                    </Typography>
                    <Box
                      component="pre"
                      sx={{
                        m: 0,
                        fontFamily: isJsonPayload ? 'Menlo, Monaco, Consolas, monospace' : 'inherit',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                        backgroundColor: (theme) => theme.palette.action.hover,
                        borderRadius: 1,
                        p: 1.5,
                        fontSize: 14
                      }}
                    >
                      {payloadValue}
                    </Box>
                  </Stack>
                </Box>
              );
            })}
          </Stack>
        )}
      </Stack>
    </DialogContent>
    <DialogActions>
      <Button onClick={onRefresh} disabled={loading}>
        Refresh
      </Button>
      <Button onClick={onClose} variant="contained">
        Close
      </Button>
    </DialogActions>
  </Dialog>
);

export default TableObservabilityMetricsDialog;
