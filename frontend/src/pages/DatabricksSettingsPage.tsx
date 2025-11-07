import { ChangeEvent, FormEvent, ReactNode, SyntheticEvent, useEffect, useMemo, useState } from 'react';
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
  Tabs,
  Tab
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useDatabricksSettings } from '../hooks/useDatabricksSettings';
import { useSapHanaSettings } from '../hooks/useSapHanaSettings';
import {
  DatabricksSqlSettingsInput,
  DatabricksSqlSettingsUpdate,
  SapHanaSettingsInput,
  SapHanaSettingsUpdate
} from '../types/data';
import { useAuth } from '../context/AuthContext';

interface FormState {
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog: string;
  schemaName: string;
  constructedSchema: string;
  ingestionBatchRows: string;
  ingestionMethod: 'sql' | 'spark';
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
  warehouseName: '',
  accessToken: ''
};

interface SapHanaFormState {
  displayName: string;
  host: string;
  port: string;
  databaseName: string;
  username: string;
  password: string;
  schemaName: string;
  tenant: string;
  useSsl: boolean;
  ingestionBatchRows: string;
  isActive: boolean;
}

const emptySapHanaForm: SapHanaFormState = {
  displayName: 'SAP HANA Warehouse',
  host: '',
  port: '30015',
  databaseName: '',
  username: '',
  password: '',
  schemaName: '',
  tenant: '',
  useSsl: true,
  ingestionBatchRows: '',
  isActive: true
};

const DataWarehouseSettingsPage = () => {
  const theme = useTheme();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    settingsQuery,
    createSettings,
    updateSettings,
    testSettings,
    creating,
    updating,
    testing
  } = useDatabricksSettings();

  const {
    settingsQuery: sapHanaQuery,
    createSettings: createSapHanaSettings,
    updateSettings: updateSapHanaSettings,
    testSettings: testSapHanaSettings,
    creating: creatingSapHana,
    updating: updatingSapHana,
    testing: testingSapHana
  } = useSapHanaSettings();

  const { data: settings, isLoading, isFetching, isError, error } = settingsQuery;
  const {
    data: sapHanaSettings,
    isLoading: sapHanaLoading,
    isFetching: sapHanaFetching,
    isError: sapHanaError,
    error: sapHanaErrorObj
  } = sapHanaQuery;
  type TabKey = 'databricks' | 'sapHana';
  const [form, setForm] = useState<FormState>(emptyForm);
  const [clearToken, setClearToken] = useState(false);
  const [hanaForm, setHanaForm] = useState<SapHanaFormState>(emptySapHanaForm);
  const [hanaClearPassword, setHanaClearPassword] = useState(false);
  const [activeTab, setActiveTab] = useState<TabKey>('databricks');

  const TabPanel = ({ value, current, children }: { value: TabKey; current: TabKey; children: ReactNode }) => (
    <Box
      role="tabpanel"
      hidden={current !== value}
      id={`warehouse-tabpanel-${value}`}
      aria-labelledby={`warehouse-tab-${value}`}
      sx={{ mt: 3 }}
    >
      {current === value ? children : null}
    </Box>
  );

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
      warehouseName: settings.warehouseName ?? '',
      accessToken: ''
    });
    setClearToken(false);
  }, [settings, isLoading]);

  useEffect(() => {
    if (sapHanaLoading) {
      return;
    }
    if (!sapHanaSettings) {
      setHanaForm(emptySapHanaForm);
      setHanaClearPassword(false);
      return;
    }
    setHanaForm({
      displayName: sapHanaSettings.displayName,
      host: sapHanaSettings.host,
      port: sapHanaSettings.port?.toString() ?? '30015',
      databaseName: sapHanaSettings.databaseName,
      username: sapHanaSettings.username,
      password: '',
      schemaName: sapHanaSettings.schemaName ?? '',
      tenant: sapHanaSettings.tenant ?? '',
      useSsl: sapHanaSettings.useSsl,
      ingestionBatchRows: sapHanaSettings.ingestionBatchRows?.toString() ?? '',
      isActive: sapHanaSettings.isActive
    });
    setHanaClearPassword(false);
  }, [sapHanaSettings, sapHanaLoading]);

  const busy = creating || updating;
  const disableInputs = !canManage || busy;
  const trimValue = (value?: string | null) => value?.trim() ?? '';
  const effectiveToken = trimValue(form.accessToken);
  const canTest = effectiveToken.length > 0 && !clearToken;
  const labelPropsFor = (value?: string | null) => ({ shrink: trimValue(value).length > 0 });

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

  const parsePort = (value: string): number => {
    const trimmed = value.trim();
    const parsed = Number.parseInt(trimmed, 10);
    if (Number.isNaN(parsed) || parsed <= 0) {
      return 30015;
    }
    return parsed;
  };

  const hanaBusy = creatingSapHana || updatingSapHana;
  const hanaDisableInputs = !canManage || hanaBusy;
  const hanaEffectivePassword = trimValue(hanaForm.password);
  const hanaCanTest = hanaEffectivePassword.length > 0 && !hanaClearPassword;

  const handleTabChange = (_event: SyntheticEvent, value: TabKey) => {
    setActiveTab(value);
  };

  const headerDescription = useMemo(() => {
    if (!settings && !sapHanaSettings) {
      return 'Configure the data warehouses used for ingestion. Add Databricks SQL, SAP HANA, or both as targets.';
    }
    if (settings && sapHanaSettings) {
      return 'Databricks SQL and SAP HANA warehouses are configured. Update credentials or connection details below.';
    }
    if (settings) {
      return 'Databricks SQL is configured. Optionally add SAP HANA to support dual-target ingestion.';
    }
    return 'SAP HANA is configured. Optionally add Databricks SQL for SQL warehouse ingestion.';
  }, [settings, sapHanaSettings]);

  const handleChange = (key: keyof FormState) => {
    return (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setForm((previous) => ({ ...previous, [key]: value }));
    };
  };

  const handleIngestionMethodChange = (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const value = event.target.value === 'spark' ? 'spark' : 'sql';
    setForm((previous) => ({ ...previous, ingestionMethod: value }));
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
      const batchRowsValue = parseBatchRows(form.ingestionBatchRows);
      const effectiveMethod = form.ingestionMethod === 'spark' ? 'spark' : 'sql';
      const payload: DatabricksSqlSettingsInput = {
        displayName: trimValue(form.displayName) || 'Primary Warehouse',
        workspaceHost: trimValue(form.workspaceHost),
        httpPath: trimValue(form.httpPath),
        accessToken: effectiveToken,
        catalog: trimValue(form.catalog) || null,
        schemaName: trimValue(form.schemaName) || null,
        constructedSchema: trimValue(form.constructedSchema) || null,
        ingestionBatchRows: batchRowsValue,
        ingestionMethod: effectiveMethod,
        warehouseName: trimValue(form.warehouseName) || null
      };
      await createSettings(payload);
      return;
    }

    const update: DatabricksSqlSettingsUpdate = {};
    const parsedBatchRows = parseBatchRows(form.ingestionBatchRows);
    const effectiveMethod = form.ingestionMethod === 'spark' ? 'spark' : 'sql';
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
        warehouseName: settings.warehouseName ?? '',
        accessToken: ''
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
    const payload: DatabricksSqlSettingsInput = {
      displayName: trimValue(form.displayName) || 'Primary Warehouse',
      workspaceHost: trimValue(form.workspaceHost),
      httpPath: trimValue(form.httpPath),
      accessToken: effectiveToken,
      catalog: trimValue(form.catalog) || null,
      schemaName: trimValue(form.schemaName) || null,
      constructedSchema: trimValue(form.constructedSchema) || null,
      ingestionBatchRows: parseBatchRows(form.ingestionBatchRows),
      ingestionMethod: form.ingestionMethod === 'spark' ? 'spark' : 'sql',
      warehouseName: trimValue(form.warehouseName) || null
    };
    await testSettings(payload);
  };

  const handleHanaChange = (key: keyof SapHanaFormState) => {
    return (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setHanaForm((previous) => ({ ...previous, [key]: value }));
    };
  };

  const handleHanaToggleUseSsl = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setHanaForm((previous) => ({ ...previous, useSsl: checked }));
  };

  const handleHanaToggleActive = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setHanaForm((previous) => ({ ...previous, isActive: checked }));
  };

  const handleHanaSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!canManage) {
      return;
    }

    if (!sapHanaSettings) {
      if (!hanaEffectivePassword) {
        return;
      }
      const payload: SapHanaSettingsInput = {
        displayName: trimValue(hanaForm.displayName) || 'SAP HANA Warehouse',
        host: trimValue(hanaForm.host),
        port: parsePort(hanaForm.port),
        databaseName: trimValue(hanaForm.databaseName),
        username: trimValue(hanaForm.username),
        password: hanaEffectivePassword,
        schemaName: trimValue(hanaForm.schemaName) || null,
        tenant: trimValue(hanaForm.tenant) || null,
        useSsl: hanaForm.useSsl,
        ingestionBatchRows: parseBatchRows(hanaForm.ingestionBatchRows)
      };
      await createSapHanaSettings(payload);
      return;
    }

    const update: SapHanaSettingsUpdate = {};
    const trimmedDisplayName = trimValue(hanaForm.displayName);
    if (trimmedDisplayName && trimmedDisplayName !== sapHanaSettings.displayName) {
      update.displayName = trimmedDisplayName;
    }
    const trimmedHost = trimValue(hanaForm.host);
    if (trimmedHost && trimmedHost !== sapHanaSettings.host) {
      update.host = trimmedHost;
    }
    const parsedPort = parsePort(hanaForm.port);
    if (parsedPort !== sapHanaSettings.port) {
      update.port = parsedPort;
    }
    const trimmedDatabase = trimValue(hanaForm.databaseName);
    if (trimmedDatabase && trimmedDatabase !== sapHanaSettings.databaseName) {
      update.databaseName = trimmedDatabase;
    }
    const trimmedUsername = trimValue(hanaForm.username);
    if (trimmedUsername && trimmedUsername !== sapHanaSettings.username) {
      update.username = trimmedUsername;
    }
    if (hanaClearPassword) {
      update.password = null;
    } else if (hanaEffectivePassword) {
      update.password = hanaEffectivePassword;
    }
    const trimmedSchema = trimValue(hanaForm.schemaName);
    if (trimmedSchema !== (sapHanaSettings.schemaName ?? '')) {
      update.schemaName = trimmedSchema ? trimmedSchema : null;
    }
    const trimmedTenant = trimValue(hanaForm.tenant);
    if (trimmedTenant !== (sapHanaSettings.tenant ?? '')) {
      update.tenant = trimmedTenant ? trimmedTenant : null;
    }
    if (hanaForm.useSsl !== sapHanaSettings.useSsl) {
      update.useSsl = hanaForm.useSsl;
    }
    const parsedHanaBatch = parseBatchRows(hanaForm.ingestionBatchRows);
    if (parsedHanaBatch !== (sapHanaSettings.ingestionBatchRows ?? null)) {
      update.ingestionBatchRows = parsedHanaBatch;
    }
    if (hanaForm.isActive !== sapHanaSettings.isActive) {
      update.isActive = hanaForm.isActive;
    }

    if (Object.keys(update).length === 0) {
      return;
    }

    await updateSapHanaSettings({ id: sapHanaSettings.id, input: update });
  };

  const handleHanaReset = () => {
    if (sapHanaSettings) {
      setHanaForm({
        displayName: sapHanaSettings.displayName,
        host: sapHanaSettings.host,
        port: sapHanaSettings.port?.toString() ?? '30015',
        databaseName: sapHanaSettings.databaseName,
        username: sapHanaSettings.username,
        password: '',
        schemaName: sapHanaSettings.schemaName ?? '',
        tenant: sapHanaSettings.tenant ?? '',
        useSsl: sapHanaSettings.useSsl,
        ingestionBatchRows: sapHanaSettings.ingestionBatchRows?.toString() ?? '',
        isActive: sapHanaSettings.isActive
      });
      setHanaClearPassword(false);
    } else {
      setHanaForm(emptySapHanaForm);
      setHanaClearPassword(false);
    }
  };

  const handleHanaTest = async () => {
    if (!canManage || !hanaCanTest) {
      return;
    }
    const payload: SapHanaSettingsInput = {
      displayName: trimValue(hanaForm.displayName) || 'SAP HANA Warehouse',
      host: trimValue(hanaForm.host),
      port: parsePort(hanaForm.port),
      databaseName: trimValue(hanaForm.databaseName),
      username: trimValue(hanaForm.username),
      password: hanaEffectivePassword,
      schemaName: trimValue(hanaForm.schemaName) || null,
      tenant: trimValue(hanaForm.tenant) || null,
      useSsl: hanaForm.useSsl,
      ingestionBatchRows: parseBatchRows(hanaForm.ingestionBatchRows)
    };
    await testSapHanaSettings(payload);
  };

  if (isLoading && !settings) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  const errorMessage = isError
    ? (error instanceof Error ? error.message : 'Unable to load data warehouse settings.')
    : null;

  const showTokenNotice = Boolean(settings?.hasAccessToken && !clearToken && !form.accessToken);
  const hanaErrorMessage = sapHanaError
    ? (sapHanaErrorObj instanceof Error ? sapHanaErrorObj.message : 'Unable to load SAP HANA settings.')
    : null;
  const showHanaPasswordNotice = Boolean(
    sapHanaSettings?.hasPassword && !hanaClearPassword && !hanaForm.password
  );

  return (
    <Box>
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
          borderBottom: `3px solid ${theme.palette.primary.main}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
        }}
      >
        <Typography
          variant="h4"
          gutterBottom
          sx={{ color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }}
        >
          Data Warehouse
        </Typography>
        <Typography
          variant="body2"
          sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}
        >
          {headerDescription}
        </Typography>
      </Box>

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {hanaErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {hanaErrorMessage}
        </Alert>
      )}

      {!canManage && (
        <Alert severity="info" sx={{ mb: 3 }}>
          You have read-only access to the data warehouse configuration. Contact an administrator to
          make changes.
        </Alert>
      )}

      <Tabs
        value={activeTab}
        onChange={handleTabChange}
        aria-label="Data warehouse settings selector"
        sx={{ borderBottom: 1, borderColor: 'divider', maxWidth: 720 }}
      >
        <Tab label="Databricks SQL" value="databricks" id="warehouse-tab-databricks" aria-controls="warehouse-tabpanel-databricks" />
        <Tab label="SAP HANA" value="sapHana" id="warehouse-tab-sapHana" aria-controls="warehouse-tabpanel-sapHana" />
      </Tabs>

      <TabPanel value="databricks" current={activeTab}>
        <Paper
          component="form"
          elevation={3}
          onSubmit={handleSubmit}
          sx={{ p: 3, mb: 4, maxWidth: 720 }}
        >
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
              helperText="Optional schema/database within the selected catalog."
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

          <TextField
            label={settings ? 'Personal Access Token (leave blank to retain existing token)' : 'Personal Access Token'}
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
            <Button
              type="button"
              variant="outlined"
              onClick={handleReset}
              disabled={busy || testing}
            >
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
              {busy ? 'Persisting changes…' : 'Refreshing configuration…'}
            </Typography>
          </Box>
        )}
      </TabPanel>

      <TabPanel value="sapHana" current={activeTab}>
        <Paper
          component="form"
          elevation={3}
          onSubmit={handleHanaSubmit}
          sx={{ p: 3, mb: 4, maxWidth: 720 }}
        >
          <Stack spacing={3}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            SAP HANA Warehouse
          </Typography>

          {sapHanaLoading && !sapHanaSettings ? (
            <Box display="flex" justifyContent="center" alignItems="center" py={4}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              {!sapHanaSettings && (
                <Alert severity="info">
                  Configure SAP HANA to enable ingestion into an on-premises warehouse. Provide a
                  connection with appropriate read/write permissions.
                </Alert>
              )}

              <TextField
                label="Display Name"
                value={hanaForm.displayName}
                onChange={handleHanaChange('displayName')}
                disabled={hanaDisableInputs}
                helperText="Used within the UI to identify this warehouse."
                fullWidth
                InputLabelProps={labelPropsFor(hanaForm.displayName)}
              />

              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                <TextField
                  label="Host"
                  value={hanaForm.host}
                  onChange={handleHanaChange('host')}
                  disabled={hanaDisableInputs}
                  required
                  helperText="SAP HANA host or IP address."
                  fullWidth
                  InputLabelProps={labelPropsFor(hanaForm.host)}
                />
                <TextField
                  label="Port"
                  value={hanaForm.port}
                  onChange={handleHanaChange('port')}
                  disabled={hanaDisableInputs}
                  helperText="Default 30015."
                  type="number"
                  inputProps={{ min: 1, max: 65535 }}
                  fullWidth
                  InputLabelProps={labelPropsFor(hanaForm.port)}
                />
              </Stack>

              <TextField
                label="Database"
                value={hanaForm.databaseName}
                onChange={handleHanaChange('databaseName')}
                disabled={hanaDisableInputs}
                required
                helperText="Database or tenant name within SAP HANA."
                fullWidth
                InputLabelProps={labelPropsFor(hanaForm.databaseName)}
              />

              <TextField
                label="Username"
                value={hanaForm.username}
                onChange={handleHanaChange('username')}
                disabled={hanaDisableInputs}
                required
                helperText="User with insert and metadata privileges."
                fullWidth
                InputLabelProps={labelPropsFor(hanaForm.username)}
              />

              <TextField
                label={sapHanaSettings ? 'Password (leave blank to retain existing password)' : 'Password'}
                type="password"
                value={hanaForm.password}
                onChange={handleHanaChange('password')}
                disabled={hanaDisableInputs}
                required={!sapHanaSettings}
                helperText="Credentials are encrypted and stored securely."
                fullWidth
                autoComplete="off"
              />

              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                <TextField
                  label="Schema"
                  value={hanaForm.schemaName}
                  onChange={handleHanaChange('schemaName')}
                  disabled={hanaDisableInputs}
                  helperText="Optional schema override within the database."
                  fullWidth
                  InputLabelProps={labelPropsFor(hanaForm.schemaName)}
                />
                <TextField
                  label="Tenant"
                  value={hanaForm.tenant}
                  onChange={handleHanaChange('tenant')}
                  disabled={hanaDisableInputs}
                  helperText="Optional tenant identifier when using MDC."
                  fullWidth
                  InputLabelProps={labelPropsFor(hanaForm.tenant)}
                />
              </Stack>

              <FormControlLabel
                control={
                  <Switch
                    size="small"
                    checked={hanaForm.useSsl}
                    onChange={handleHanaToggleUseSsl}
                    disabled={hanaDisableInputs}
                  />
                }
                label={hanaForm.useSsl ? 'Encrypt connection with SSL' : 'SSL disabled'}
              />

              <TextField
                label="Insert Batch Size"
                value={hanaForm.ingestionBatchRows}
                onChange={handleHanaChange('ingestionBatchRows')}
                disabled={hanaDisableInputs}
                helperText="Maximum rows per insert statement. Leave blank to use the default."
                fullWidth
                type="number"
                inputProps={{ min: 1 }}
                InputLabelProps={labelPropsFor(hanaForm.ingestionBatchRows)}
              />

              {sapHanaSettings && (
                <FormControlLabel
                  control={
                    <Switch
                      size="small"
                      color="success"
                      checked={hanaForm.isActive}
                      onChange={handleHanaToggleActive}
                      disabled={hanaDisableInputs}
                    />
                  }
                  label={hanaForm.isActive ? 'Configuration is active' : 'Configuration is inactive'}
                />
              )}

              {sapHanaSettings && sapHanaSettings.hasPassword && (
                <FormControlLabel
                  control={
                    <Switch
                      size="small"
                      color="warning"
                      checked={hanaClearPassword}
                      onChange={(event) => setHanaClearPassword(event.target.checked)}
                      disabled={hanaDisableInputs}
                    />
                  }
                  label="Clear the stored password"
                />
              )}

              {showHanaPasswordNotice && (
                <Alert severity="info">
                  A password is already stored securely. Provide a new password to replace it, or
                  use the toggle above to remove it.
                </Alert>
              )}

              <Stack direction="row" spacing={2} justifyContent="flex-end" alignItems="center">
                <Button
                  type="button"
                  variant="outlined"
                  onClick={handleHanaReset}
                  disabled={hanaBusy || testingSapHana}
                >
                  Reset
                </Button>
                <Button
                  type="button"
                  variant="outlined"
                  onClick={handleHanaTest}
                  disabled={!canManage || !hanaCanTest || testingSapHana}
                >
                  {testingSapHana ? 'Testing...' : 'Test Connection'}
                </Button>
                <Button
                  type="submit"
                  variant="contained"
                  disabled={!canManage || hanaBusy || (!sapHanaSettings && !hanaEffectivePassword)}
                >
                  {sapHanaSettings
                    ? hanaBusy
                      ? 'Saving...'
                      : 'Save Changes'
                    : hanaBusy
                      ? 'Creating...'
                      : 'Create Connection'}
                </Button>
              </Stack>
            </>
          )}
          </Stack>
        </Paper>

        {(sapHanaFetching || hanaBusy) && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'text.secondary' }}>
            <CircularProgress size={18} />
            <Typography variant="body2">
              {hanaBusy ? 'Persisting SAP HANA changes…' : 'Refreshing SAP HANA configuration…'}
            </Typography>
          </Box>
        )}
      </TabPanel>
    </Box>
  );
};

export default DataWarehouseSettingsPage;
