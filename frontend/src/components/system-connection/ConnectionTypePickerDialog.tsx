import { Button, ButtonBase, Dialog, DialogActions, DialogContent, DialogTitle, Grid, Paper, Stack, Typography } from '@mui/material';
import { SvgIconComponent } from '@mui/icons-material';
import CloudQueueRoundedIcon from '@mui/icons-material/CloudQueueRounded';
import StorageRoundedIcon from '@mui/icons-material/StorageRounded';
import LanRoundedIcon from '@mui/icons-material/LanRounded';

import { RelationalDatabaseType } from '../../types/data';
import { DATABASE_LABELS } from './SystemConnectionForm';

interface ConnectionTypeOption {
  id: RelationalDatabaseType;
  label: string;
  description: string;
  icon: SvgIconComponent;
}

const DEFAULT_OPTIONS: ConnectionTypeOption[] = [
  {
    id: 'databricks',
    label: DATABASE_LABELS.databricks,
    description: 'Use a Databricks SQL warehouse with host, HTTP path, and personal access token.',
    icon: CloudQueueRoundedIcon,
  },
  {
    id: 'postgresql',
    label: DATABASE_LABELS.postgresql,
    description: 'Connect to PostgreSQL-compatible engines with JDBC credentials.',
    icon: StorageRoundedIcon,
  },
  {
    id: 'sap',
    label: DATABASE_LABELS.sap,
    description: 'Connect to SAP HANA tenants via JDBC connection strings.',
    icon: LanRoundedIcon,
  },
];

interface ConnectionTypePickerDialogProps {
  open: boolean;
  onSelect: (type: RelationalDatabaseType) => void;
  onClose: () => void;
  options?: ConnectionTypeOption[];
}

const ConnectionTypePickerDialog = ({ open, onSelect, onClose, options = DEFAULT_OPTIONS }: ConnectionTypePickerDialogProps) => {
  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Select a data source</DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Choose the warehouse or database technology you want to connect. We will ask for the
          credentials required for that engine next.
        </Typography>
        <Grid container spacing={2}>
          {options.map((option) => {
            const Icon = option.icon;
            return (
              <Grid item xs={12} sm={6} key={option.id}>
                <ButtonBase
                  onClick={() => onSelect(option.id)}
                  sx={{
                    width: '100%',
                    textAlign: 'left',
                    borderRadius: 3,
                  }}
                >
                  <Paper
                    elevation={2}
                    sx={{
                      width: '100%',
                      borderRadius: 3,
                      p: 2,
                      height: '100%',
                    }}
                  >
                    <Stack spacing={1.5}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Icon color="primary" fontSize="large" />
                        <Typography variant="h6" fontWeight={600}>
                          {option.label}
                        </Typography>
                      </Stack>
                      <Typography variant="body2" color="text.secondary">
                        {option.description}
                      </Typography>
                    </Stack>
                  </Paper>
                </ButtonBase>
              </Grid>
            );
          })}
        </Grid>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
      </DialogActions>
    </Dialog>
  );
};

export default ConnectionTypePickerDialog;
