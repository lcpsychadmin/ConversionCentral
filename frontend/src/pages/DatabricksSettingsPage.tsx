import { FormEvent, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControlLabel,
  Paper,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useDatabricksSettings } from '../hooks/useDatabricksSettings';
import { DatabricksSqlSettingsInput, DatabricksSqlSettingsUpdate } from '../types/data';
import { useAuth } from '../context/AuthContext';

interface FormState {
  displayName: string;
  workspaceHost: string;
  httpPath: string;
  catalog: string;
  schemaName: string;
  warehouseName: string;
  accessToken: string;
}

const emptyForm: FormState = {
  displayName: 'Primary Warehouse',
  workspaceHost: '',
  httpPath: '',
  catalog: '',
  schemaName: '',
  warehouseName: '',
  accessToken: ''
};

const DatabricksSettingsPage = () => {
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
      warehouseName: settings.warehouseName ?? '',
      accessToken: ''
    });
    setClearToken(false);
  }, [settings, isLoading]);

  const busy = creating || updating;
  const disableInputs = !canManage || busy;
  const effectiveToken = form.accessToken.trim();
  const canTest = effectiveToken.length > 0 && !clearToken;

  const headerDescription = useMemo(() => {
    if (!settings) {
      return 'Configure the Databricks SQL warehouse used to store ingestion and constructed data.';
    }
    return 'Update the active Databricks SQL warehouse configuration. Leave the token blank to keep the stored credential.';
  }, [settings]);

  const handleChange = (key: keyof FormState) => (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setForm((previous) => ({ ...previous, [key]: value }));
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
      const payload: DatabricksSqlSettingsInput = {
        displayName: form.displayName.trim() || 'Primary Warehouse',
        workspaceHost: form.workspaceHost.trim(),
        httpPath: form.httpPath.trim(),
        accessToken: effectiveToken,
        catalog: form.catalog.trim() || null,
        schemaName: form.schemaName.trim() || null,
        warehouseName: form.warehouseName.trim() || null
      };
      await createSettings(payload);
      return;
    }

    const update: DatabricksSqlSettingsUpdate = {};
    if (form.displayName.trim() && form.displayName.trim() !== settings.displayName) {
      update.displayName = form.displayName.trim();
    }
    if (form.workspaceHost.trim() && form.workspaceHost.trim() !== settings.workspaceHost) {
      update.workspaceHost = form.workspaceHost.trim();
    }
    if (form.httpPath.trim() && form.httpPath.trim() !== settings.httpPath) {
      update.httpPath = form.httpPath.trim();
    }
    if ((form.catalog || '') !== (settings.catalog ?? '')) {
      update.catalog = form.catalog.trim() ? form.catalog.trim() : null;
    }
    if ((form.schemaName || '') !== (settings.schemaName ?? '')) {
      update.schemaName = form.schemaName.trim() ? form.schemaName.trim() : null;
    }
    if ((form.warehouseName || '') !== (settings.warehouseName ?? '')) {
      update.warehouseName = form.warehouseName.trim() ? form.warehouseName.trim() : null;
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
      displayName: form.displayName.trim() || 'Primary Warehouse',
      workspaceHost: form.workspaceHost.trim(),
      httpPath: form.httpPath.trim(),
      accessToken: effectiveToken,
      catalog: form.catalog.trim() || null,
      schemaName: form.schemaName.trim() || null,
      warehouseName: form.warehouseName.trim() || null
    };
    await testSettings(payload);
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
      : 'Unable to load Databricks settings.'
    : null;

  const showTokenNotice = Boolean(settings?.hasAccessToken && !clearToken && !form.accessToken);

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
          Databricks Warehouse
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

      {!canManage && (
        <Alert severity="info" sx={{ mb: 3 }}>
          You have read-only access to the Databricks configuration. Contact an administrator to
          make changes.
        </Alert>
      )}

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
          />

          <TextField
            label="Workspace Host"
            value={form.workspaceHost}
            onChange={handleChange('workspaceHost')}
            disabled={disableInputs}
            required={!settings}
            helperText="Example: adb-123456789012345.7.azuredatabricks.net"
            fullWidth
          />

          <TextField
            label="SQL Warehouse HTTP Path"
            value={form.httpPath}
            onChange={handleChange('httpPath')}
            disabled={disableInputs}
            required={!settings}
            helperText="Example: /sql/1.0/warehouses/<warehouse-id>"
            fullWidth
          />

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Catalog"
              value={form.catalog}
              onChange={handleChange('catalog')}
              disabled={disableInputs}
              helperText="Optional. Overrides the default catalog when provided."
              fullWidth
            />
            <TextField
              label="Schema"
              value={form.schemaName}
              onChange={handleChange('schemaName')}
              disabled={disableInputs}
              helperText="Optional schema/database within the selected catalog."
              fullWidth
            />
          </Stack>

          <TextField
            label="Warehouse Name"
            value={form.warehouseName}
            onChange={handleChange('warehouseName')}
            disabled={disableInputs}
            helperText="Optional friendly name for reference only."
            fullWidth
          />

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
    </Box>
  );
};

export default DatabricksSettingsPage;
