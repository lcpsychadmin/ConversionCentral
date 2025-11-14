import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControlLabel,
  MenuItem,
  Paper,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material';

import PageHeader from '../components/common/PageHeader';
import { useAuth } from '../context/AuthContext';
import { useDatabricksSettings } from '../hooks/useDatabricksSettings';
import { DatabricksSqlSettingsInput, DatabricksSqlSettingsUpdate } from '../types/data';

interface FormState {
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog: string;
  schemaName: string;
  constructedSchema: string;
  ingestionBatchRows: string;
  ingestionMethod: 'sql' | 'spark';
  sparkCompute: 'classic' | 'serverless';
  warehouseName: string;
  accessToken: string;
}

const emptyForm: FormState = {
  displayName: 'Primary Warehouse',
  workspaceHost: '',
  httpPath: '',
  catalog: '',
  schemaName: '',
  constructedSchema: '',
  ingestionBatchRows: '',
  ingestionMethod: 'sql',
  sparkCompute: 'classic',
  warehouseName: '',
  accessToken: '',
};

const DataWarehouseSettingsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    settingsQuery,
    createSettings,
    updateSettings,
    testSettings,
    creating,
    updating,
    testing,
  } = useDatabricksSettings();

  const { data: settings, isLoading, isFetching, isError, error } = settingsQuery;
  const [form, setForm] = useState<FormState>(emptyForm);
  const [clearToken, setClearToken] = useState(false);

  useEffect(() => {
    if (isLoading) {
      return;
    }

    if (!settings) {
      setForm(emptyForm);
      setClearToken(false);
      return;
    }

    setForm({
      displayName: settings.displayName,
      workspaceHost: settings.workspaceHost,
      httpPath: settings.httpPath,
      catalog: settings.catalog ?? '',
      schemaName: settings.schemaName ?? '',
      constructedSchema: settings.constructedSchema ?? '',
      ingestionBatchRows: settings.ingestionBatchRows?.toString() ?? '',
      ingestionMethod: settings.ingestionMethod,
      sparkCompute: settings.sparkCompute ?? 'classic',
      warehouseName: settings.warehouseName ?? '',
      accessToken: '',
    });
    setClearToken(false);
  }, [settings, isLoading]);

  const busy = creating || updating;
  const disableInputs = !canManage || busy;
  const trimValue = (value?: string | null) => value?.trim() ?? '';
  const effectiveToken = trimValue(form.accessToken);
  const canTest = effectiveToken.length > 0 && !clearToken;
  const labelPropsFor = (value?: string | null) => ({ shrink: trimValue(value).length > 0 });

  const headerDescription = useMemo(() => {
    if (settings) {
      return 'Databricks SQL is configured. Update credentials or connection details below.';
    }
    return 'Configure the Databricks SQL warehouse used for ingestion.';
  }, [settings]);

  const parseBatchRows = (value: string): number | null => {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = Number.parseInt(trimmed, 10);
    if (Number.isNaN(parsed) || parsed <= 0) {
      return null;
    }
    return parsed;
  };

  const handleChange = (key: keyof FormState) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const { value } = event.target;
      setForm((previous) => ({ ...previous, [key]: value }));
    };

  const handleIngestionMethodChange = (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const value = event.target.value === 'spark' ? 'spark' : 'sql';
    setForm((previous) => ({ ...previous, ingestionMethod: value }));
  };

  const handleSparkComputeChange = (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const value = event.target.value === 'serverless' ? 'serverless' : 'classic';
    setForm((previous) => ({ ...previous, sparkCompute: value }));
  };

  const buildPayload = (): DatabricksSqlSettingsInput => {
    const effectiveMethod = form.ingestionMethod === 'spark' ? 'spark' : 'sql';
    const effectiveCompute = form.sparkCompute === 'serverless' ? 'serverless' : 'classic';
    return {
      displayName: trimValue(form.displayName) || 'Primary Warehouse',
      workspaceHost: trimValue(form.workspaceHost),
      httpPath: trimValue(form.httpPath),
      accessToken: effectiveToken,
      catalog: trimValue(form.catalog) || null,
      schemaName: trimValue(form.schemaName) || null,
      constructedSchema: trimValue(form.constructedSchema) || null,
      ingestionBatchRows: parseBatchRows(form.ingestionBatchRows),
      ingestionMethod: effectiveMethod,
      sparkCompute: effectiveCompute,
      warehouseName: trimValue(form.warehouseName) || null,
    };
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!canManage) {
      return;
    }

    if (!settings) {
      if (!effectiveToken) {
        return;
      }
      await createSettings(buildPayload());
      return;
    }

    const update: DatabricksSqlSettingsUpdate = {};
    const parsedBatchRows = parseBatchRows(form.ingestionBatchRows);
    const effectiveMethod = form.ingestionMethod === 'spark' ? 'spark' : 'sql';
  const effectiveCompute = form.sparkCompute === 'serverless' ? 'serverless' : 'classic';

    const trimmedDisplayName = trimValue(form.displayName);
    if (trimmedDisplayName && trimmedDisplayName !== settings.displayName) {
      update.displayName = trimmedDisplayName;
    }

    const trimmedWorkspaceHost = trimValue(form.workspaceHost);
    if (trimmedWorkspaceHost && trimmedWorkspaceHost !== settings.workspaceHost) {
      update.workspaceHost = trimmedWorkspaceHost;
    }

    const trimmedHttpPath = trimValue(form.httpPath);
    if (trimmedHttpPath && trimmedHttpPath !== settings.httpPath) {
      update.httpPath = trimmedHttpPath;
    }

    const trimmedCatalog = trimValue(form.catalog);
    if (trimmedCatalog !== (settings.catalog ?? '')) {
      update.catalog = trimmedCatalog ? trimmedCatalog : null;
    }

    const trimmedSchemaName = trimValue(form.schemaName);
    if (trimmedSchemaName !== (settings.schemaName ?? '')) {
      update.schemaName = trimmedSchemaName ? trimmedSchemaName : null;
    }

    const trimmedConstructedSchema = trimValue(form.constructedSchema);
    if (trimmedConstructedSchema !== (settings.constructedSchema ?? '')) {
      update.constructedSchema = trimmedConstructedSchema ? trimmedConstructedSchema : null;
    }

    if (parsedBatchRows !== (settings.ingestionBatchRows ?? null)) {
      update.ingestionBatchRows = parsedBatchRows;
    }

    if (effectiveMethod !== settings.ingestionMethod) {
      update.ingestionMethod = effectiveMethod;
    }

    if (effectiveCompute !== (settings.sparkCompute ?? 'classic')) {
      update.sparkCompute = effectiveCompute;
    }

    const trimmedWarehouseName = trimValue(form.warehouseName);
    if (trimmedWarehouseName !== (settings.warehouseName ?? '')) {
      update.warehouseName = trimmedWarehouseName ? trimmedWarehouseName : null;
    }

    if (clearToken) {
      update.accessToken = null;
    } else if (effectiveToken) {
      update.accessToken = effectiveToken;
    }

    if (Object.keys(update).length === 0) {
      return;
    }

    await updateSettings({ id: settings.id, input: update });
  };

  const handleReset = () => {
    if (settings) {
      setForm({
        displayName: settings.displayName,
        workspaceHost: settings.workspaceHost,
        httpPath: settings.httpPath,
        catalog: settings.catalog ?? '',
        schemaName: settings.schemaName ?? '',
        constructedSchema: settings.constructedSchema ?? '',
        ingestionBatchRows: settings.ingestionBatchRows?.toString() ?? '',
        ingestionMethod: settings.ingestionMethod,
        sparkCompute: settings.sparkCompute ?? 'classic',
        warehouseName: settings.warehouseName ?? '',
        accessToken: '',
      });
      setClearToken(false);
    } else {
      setForm(emptyForm);
      setClearToken(false);
    }
  };

  const handleTest = async () => {
    if (!canManage || !canTest) {
      return;
    }

    await testSettings(buildPayload());
  };

  if (isLoading && !settings) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  const errorMessage = isError
    ? error instanceof Error
      ? error.message
      : 'Unable to load data warehouse settings.'
    : null;

  const showTokenNotice = Boolean(settings?.hasAccessToken && !clearToken && !form.accessToken);

  return (
    <Box>
      <PageHeader title="Data Warehouse" subtitle={headerDescription} />

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {!canManage && (
        <Alert severity="info" sx={{ mb: 3 }}>
          You have read-only access to the data warehouse configuration. Contact an administrator to
          make changes.
        </Alert>
      )}

      <Paper component="form" elevation={3} onSubmit={handleSubmit} sx={{ p: 3, mb: 4, maxWidth: 720 }}>
        <Stack spacing={3}>
          <TextField
            label="Display Name"
            value={form.displayName}
            onChange={handleChange('displayName')}
            disabled={disableInputs}
            helperText="Used within the UI to identify this warehouse."
            fullWidth
            InputLabelProps={labelPropsFor(form.displayName)}
          />

          <TextField
            label="Workspace Host"
            value={form.workspaceHost}
            onChange={handleChange('workspaceHost')}
            disabled={disableInputs}
            required={!settings}
            helperText="Example: adb-123456789012345.7.azuredatabricks.net"
            fullWidth
            InputLabelProps={labelPropsFor(form.workspaceHost)}
          />

          <TextField
            label="SQL Warehouse HTTP Path"
            value={form.httpPath}
            onChange={handleChange('httpPath')}
            disabled={disableInputs}
            required={!settings}
            helperText="Example: /sql/1.0/warehouses/<warehouse-id>"
            fullWidth
            InputLabelProps={labelPropsFor(form.httpPath)}
          />

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Catalog"
              value={form.catalog}
              onChange={handleChange('catalog')}
              disabled={disableInputs}
              helperText="Optional. Overrides the default catalog when provided."
              fullWidth
              InputLabelProps={labelPropsFor(form.catalog)}
            />
            <TextField
              label="Schema"
              value={form.schemaName}
              onChange={handleChange('schemaName')}
              disabled={disableInputs}
              helperText="Optional schema within the selected catalog."
              fullWidth
              InputLabelProps={labelPropsFor(form.schemaName)}
            />
          </Stack>

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Constructed Data Schema"
              value={form.constructedSchema}
              onChange={handleChange('constructedSchema')}
              disabled={disableInputs}
              helperText="Optional dedicated schema for constructed data tables."
              fullWidth
              InputLabelProps={labelPropsFor(form.constructedSchema)}
            />
            <TextField
              label="Warehouse Name"
              value={form.warehouseName}
              onChange={handleChange('warehouseName')}
              disabled={disableInputs}
              helperText="Optional friendly name for reference only."
              fullWidth
              InputLabelProps={labelPropsFor(form.warehouseName)}
            />
          </Stack>

          <TextField
            label="Insert Batch Size"
            value={form.ingestionBatchRows}
            onChange={handleChange('ingestionBatchRows')}
            disabled={disableInputs}
            helperText="Maximum rows per insert statement. Leave blank to use the default."
            fullWidth
            type="number"
            inputProps={{ min: 1 }}
            InputLabelProps={labelPropsFor(form.ingestionBatchRows)}
          />

          <TextField
            select
            label="Ingestion Method"
            value={form.ingestionMethod}
            onChange={handleIngestionMethodChange}
            disabled={disableInputs}
            helperText="Choose the execution engine for loading data into Databricks."
            fullWidth
          >
            <MenuItem value="sql">SQL Warehouse</MenuItem>
            <MenuItem value="spark">Spark (beta)</MenuItem>
          </TextField>

          {form.ingestionMethod === 'spark' && (
            <TextField
              select
              label="Spark Compute Mode"
              value={form.sparkCompute}
              onChange={handleSparkComputeChange}
              disabled={disableInputs}
              helperText="Tell Spark Connect whether to target a classic cluster or serverless SQL warehouse."
              fullWidth
            >
              <MenuItem value="classic">Classic cluster (all-purpose)</MenuItem>
              <MenuItem value="serverless">Serverless SQL warehouse</MenuItem>
            </TextField>
          )}

          <TextField
            label={
              settings
                ? 'Personal Access Token (leave blank to retain existing token)'
                : 'Personal Access Token'
            }
            type="password"
            value={form.accessToken}
            onChange={handleChange('accessToken')}
            disabled={disableInputs}
            required={!settings}
            helperText="Generate a PAT with access to the target SQL warehouse."
            fullWidth
            autoComplete="off"
          />

          {settings && settings.hasAccessToken && (
            <FormControlLabel
              control={
                <Switch
                  size="small"
                  color="warning"
                  checked={clearToken}
                  onChange={(event) => setClearToken(event.target.checked)}
                  disabled={disableInputs}
                />
              }
              label="Clear the stored personal access token"
            />
          )}

          {showTokenNotice && (
            <Alert severity="info">
              A personal access token is already stored securely. Provide a new token to replace it,
              or use the toggle above to remove it.
            </Alert>
          )}

          <Stack direction="row" spacing={2} justifyContent="flex-end" alignItems="center">
            <Button type="button" variant="outlined" onClick={handleReset} disabled={busy || testing}>
              Reset
            </Button>
            <Button
              type="button"
              variant="outlined"
              onClick={handleTest}
              disabled={!canManage || !canTest || testing}
            >
              {testing ? 'Testing...' : 'Test Connection'}
            </Button>
            <Button
              type="submit"
              variant="contained"
              disabled={!canManage || busy || (!settings && !effectiveToken)}
            >
              {settings ? (busy ? 'Saving...' : 'Save Changes') : busy ? 'Creating...' : 'Create Connection'}
            </Button>
          </Stack>
        </Stack>
      </Paper>

      {(isFetching || busy) && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'text.secondary' }}>
          <CircularProgress size={18} />
          <Typography variant="body2">
            {busy ? 'Persisting changes' : 'Refreshing configuration'}
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default DataWarehouseSettingsPage;
