import { Box, CircularProgress, Divider, Grid, Paper, Stack, Typography, Button, Alert } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from 'react-query';
import { fetchDatabricksSettings } from '@services/databricksSettingsService';
import { DatabricksSqlSettings } from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

const FieldRow = ({ label, value }: { label: string; value: string | null | undefined }) => (
  <Stack spacing={0.5}>
    <Typography variant="body2" color="text.secondary">
      {label}
    </Typography>
    <Typography fontWeight={500}>{value ?? '—'}</Typography>
  </Stack>
);

const DataQualitySettingsPage = () => {
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();
  const settingsQuery = useQuery<DatabricksSqlSettings | null>(
    ['databricks', 'settings'],
    fetchDatabricksSettings,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) =>
        showError(`Failed to load Databricks settings: ${resolveErrorMessage(error)}`)
    }
  );

  const handleRetry = async () => {
    try {
      const result = await settingsQuery.refetch();
      if (result.error) {
        showError(`Failed to load Databricks settings: ${resolveErrorMessage(result.error)}`);
        return;
      }
      showSuccess('Databricks settings refreshed.');
    } catch (err) {
      showError(`Failed to load Databricks settings: ${resolveErrorMessage(err)}`);
    }
  };

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h4" gutterBottom>
          Data Quality Settings
        </Typography>
        <Typography color="text.secondary">
          Review the Databricks configuration used to store TestGen metadata and manage
          auto-provisioning.
        </Typography>
      </Box>

      {settingsQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : settingsQuery.isError ? (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Stack spacing={2}>
            <Alert severity="error">Unable to load Databricks settings.</Alert>
            <Button variant="contained" onClick={handleRetry}>
              Retry
            </Button>
          </Stack>
        </Paper>
      ) : settingsQuery.data ? (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <FieldRow label="Workspace" value={settingsQuery.data.workspaceHost} />
            </Grid>
            <Grid item xs={12} md={6}>
              <FieldRow label="HTTP Path" value={settingsQuery.data.httpPath} />
            </Grid>
            <Grid item xs={12} md={6}>
              <FieldRow label="Catalog" value={settingsQuery.data.catalog ?? 'default'} />
            </Grid>
            <Grid item xs={12} md={6}>
              <FieldRow label="Schema" value={settingsQuery.data.schemaName ?? 'default'} />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <FieldRow
                label="Data Quality Schema"
                value={settingsQuery.data.dataQualitySchema ?? 'dq_metadata'}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <FieldRow
                label="Storage Format"
                value={settingsQuery.data.dataQualityStorageFormat.toUpperCase()}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <FieldRow
                label="Auto Manage Tables"
                value={settingsQuery.data.dataQualityAutoManageTables ? 'Enabled' : 'Disabled'}
              />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <FieldRow
                label="Constructed Schema"
                value={settingsQuery.data.constructedSchema ?? 'constructed'}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <FieldRow
                label="Ingestion Batch Rows"
                value={settingsQuery.data.ingestionBatchRows?.toLocaleString() ?? '—'}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <FieldRow label="Warehouse" value={settingsQuery.data.warehouseName ?? '—'} />
            </Grid>
          </Grid>

          <Box mt={4}>
            <Button variant="contained" component={RouterLink} to="/data-configuration/data-warehouse">
              Manage Databricks Settings
            </Button>
          </Box>
        </Paper>
      ) : (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Typography color="text.secondary" gutterBottom>
            Databricks warehouse settings have not been configured yet.
          </Typography>
          <Button variant="contained" component={RouterLink} to="/data-configuration/data-warehouse">
            Configure Databricks
          </Button>
        </Paper>
      )}
      {snackbar}
    </Stack>
  );
};

export default DataQualitySettingsPage;
