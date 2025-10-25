import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Grid,
  Stack,
  Typography
} from '@mui/material';
import { useTheme } from '@mui/material/styles';

import { System, SystemConnection } from '../../types/data';
import { formatConnectionSummary, parseJdbcConnectionString } from '../../utils/connectionString';

interface SystemConnectionDetailModalProps {
  open: boolean;
  connection: SystemConnection | null;
  system?: System | null;
  onClose: () => void;
}

interface DetailLineProps {
  label: string;
  value: string;
}

const DetailLine = ({ label, value }: DetailLineProps) => (
  <Box>
    <Typography variant="subtitle2" sx={{ color: 'text.secondary', fontWeight: 600, mb: 0.5 }}>
      {label}
    </Typography>
    <Typography variant="body2">{value}</Typography>
  </Box>
);

const SystemConnectionDetailModal = ({
  open,
  connection,
  system,
  onClose
}: SystemConnectionDetailModalProps) => {
  const theme = useTheme();

  if (!connection) {
    return null;
  }

  const parsed = parseJdbcConnectionString(connection.connectionString);

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle sx={{ fontWeight: 700, color: theme.palette.primary.dark }}>
        Connection Details
      </DialogTitle>
      <DialogContent dividers>
        <Stack spacing={3} sx={{ mt: 1 }}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <Stack spacing={2}>
                <DetailLine label="System" value={system?.name ?? '—'} />
                <DetailLine
                  label="Endpoint"
                  value={formatConnectionSummary(connection.connectionString)}
                />
                <DetailLine
                  label="Database"
                  value={parsed ? parsed.database : '—'}
                />
                <DetailLine
                  label="Host"
                  value={parsed ? `${parsed.host}${parsed.port ? `:${parsed.port}` : ''}` : '—'}
                />
              </Stack>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Stack spacing={2}>
                <DetailLine
                  label="Username"
                  value={parsed?.username ? parsed.username : '—'}
                />
                <DetailLine label="Status" value={connection.active ? 'Active' : 'Disabled'} />
                <DetailLine
                  label="Ingestion"
                  value={connection.ingestionEnabled ? 'Enabled' : 'Disabled'}
                />
                <DetailLine label="Notes" value={connection.notes ?? '—'} />
                <DetailLine
                  label="Last Updated"
                  value={connection.updatedAt ? new Date(connection.updatedAt).toLocaleString() : '—'}
                />
              </Stack>
            </Grid>
          </Grid>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} variant="contained">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SystemConnectionDetailModal;
