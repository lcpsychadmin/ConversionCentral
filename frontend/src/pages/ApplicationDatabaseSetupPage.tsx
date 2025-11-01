import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  FormControl,
  FormControlLabel,
  FormLabel,
  Paper,
  Radio,
  RadioGroup,
  Step,
  StepButton,
  Stepper,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useApplicationDatabase } from '../hooks/useApplicationDatabase';
import {
  ApplicationDatabaseApplyInput,
  ApplicationDatabaseEngine,
  ApplicationDatabaseTestResult
} from '../types/data';
import { useAuth } from '../context/AuthContext';

interface ConnectionFormState {
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  useSsl: boolean;
  optionsText: string;
}

interface FormState {
  engine: ApplicationDatabaseEngine;
  displayName: string;
  connection: ConnectionFormState;
}

const defaultConnectionState: ConnectionFormState = {
  host: '',
  port: '',
  database: '',
  username: '',
  password: '',
  useSsl: true,
  optionsText: ''
};

const defaultFormState: FormState = {
  engine: 'default_postgres',
  displayName: 'Primary Application Database',
  connection: defaultConnectionState
};

const parseOptions = (input: string): Record<string, string> => {
  const options: Record<string, string> = {};
  const lines = input.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const [key, ...valueParts] = line.split('=');
    if (!key || valueParts.length === 0) {
      throw new Error(`Invalid option line: "${line}". Use key=value format.`);
    }
    const value = valueParts.join('=').trim();
    options[key.trim()] = value;
  }
  return options;
};

const buildPayload = (form: FormState): ApplicationDatabaseApplyInput => {
  const shouldIncludeConnection = form.engine !== 'default_postgres';
  const connection = form.connection;
  let options: Record<string, string> | null = null;

  if (connection.optionsText.trim()) {
    const parsed = parseOptions(connection.optionsText);
    options = Object.keys(parsed).length ? parsed : null;
  }

  let portValue: number | undefined;
  if (connection.port.trim()) {
    const parsedPort = Number(connection.port.trim());
    if (Number.isNaN(parsedPort)) {
      throw new Error('Port must be a number.');
    }
    portValue = parsedPort;
  }

  return {
    engine: form.engine,
    displayName: form.displayName.trim() || null,
    connection: shouldIncludeConnection
      ? {
          host: connection.host.trim() || undefined,
          port: portValue,
          database: connection.database.trim() || undefined,
          username: connection.username.trim() || undefined,
          password: connection.password,
          options,
          useSsl: connection.useSsl
        }
      : undefined
  };
};

const requireConnectionFields = (engine: ApplicationDatabaseEngine): boolean => engine !== 'default_postgres';

const getDefaultPort = (engine: ApplicationDatabaseEngine): string => {
  if (engine === 'sqlserver') {
    return '1433';
  }
  return '5432';
};

const ApplicationDatabaseSetupPage = () => {
  const theme = useTheme();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    statusQuery,
    testConnection,
    applySetting,
    setAdminEmail: persistAdminEmail,
    testing,
    applying,
    settingAdminEmail,
    lastTestResult
  } = useApplicationDatabase();
  const { data: status, isLoading, isFetching, isError, error } = statusQuery;

  const [form, setForm] = useState<FormState>(defaultFormState);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [localTestResult, setLocalTestResult] = useState<ApplicationDatabaseTestResult | null>(null);
  const [activeStep, setActiveStep] = useState(0);
  const [adminEmailInput, setAdminEmailInput] = useState('');
  const [adminEmailError, setAdminEmailError] = useState<string | null>(null);

  useEffect(() => {
    const activeSetting = status?.setting;
    if (!activeSetting) {
      setForm(defaultFormState);
      return;
    }

    setForm((previous) => ({
      ...previous,
      engine: activeSetting.engine,
      displayName: activeSetting.displayName ?? 'Primary Application Database',
      connection: {
        ...defaultConnectionState,
        port: getDefaultPort(activeSetting.engine)
      }
    }));
  }, [status?.setting]);

  useEffect(() => {
    setAdminEmailInput(status?.adminEmail ?? '');
  }, [status?.adminEmail]);

  useEffect(() => {
    if (!status?.configured) {
      setActiveStep(0);
      return;
    }
    if (!status.adminEmail) {
      setActiveStep(1);
    }
  }, [status?.configured, status?.adminEmail]);

  useEffect(() => {
    if (lastTestResult) {
      setLocalTestResult(lastTestResult);
    }
  }, [lastTestResult]);

  const requiresConnection = requireConnectionFields(form.engine);
  const connectionRequirementsMet = useMemo(() => {
    if (!requiresConnection) {
      return true;
    }
    const { host, database, username, password } = form.connection;
    return [host, database, username, password].every((value) => value.trim().length > 0);
  }, [form.connection, requiresConnection]);

  const busy = testing || applying;

  const headerDescription = useMemo(() => {
    if (!status?.configured) {
      return 'Choose which database backend the application should use and provide any required connection details.';
    }
    return 'Update the application database configuration. Credentials are not retrievable after save, so provide new values when changing connections.';
  }, [status?.configured]);

  const handleEngineChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const nextEngine = event.target.value as ApplicationDatabaseEngine;
    setForm((previous) => ({
      ...previous,
      engine: nextEngine,
      connection: {
        ...previous.connection,
        port: getDefaultPort(nextEngine)
      }
    }));
    setLocalTestResult(null);
    setValidationError(null);
  };

  const handleConnectionChange = (
    key: Exclude<keyof ConnectionFormState, 'useSsl'>
  ) => (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setForm((previous) => ({
      ...previous,
      connection: {
        ...previous.connection,
        [key]: value
      }
    }));
    setLocalTestResult(null);
  };

  const handleUseSslChange = (_event: React.ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setForm((previous) => ({
      ...previous,
      connection: {
        ...previous.connection,
        useSsl: checked
      }
    }));
    setLocalTestResult(null);
  };

  const handleDisplayNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setForm((previous) => ({ ...previous, displayName: event.target.value }));
  };

  const handleStepChange = (step: number) => {
    if (step === 1 && !status?.configured) {
      return;
    }
    setActiveStep(step);
  };

  const handleAdminEmailChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setAdminEmailInput(event.target.value);
    setAdminEmailError(null);
  };

  const validateAdminEmail = (): boolean => {
    const trimmed = adminEmailInput.trim();
    if (!trimmed) {
      setAdminEmailError('Enter an admin email address.');
      return false;
    }

    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailPattern.test(trimmed)) {
      setAdminEmailError('Provide a valid email address.');
      return false;
    }

    setAdminEmailError(null);
    return true;
  };

  const handleAdminEmailSave = async () => {
    if (!canManage) {
      return;
    }
    if (!validateAdminEmail()) {
      return;
    }
    const normalized = adminEmailInput.trim().toLowerCase();
    await persistAdminEmail(normalized);
    setAdminEmailInput(normalized);
  };

  const resetAdminEmailForm = () => {
    setAdminEmailInput(status?.adminEmail ?? '');
    setAdminEmailError(null);
  };

  const resetForm = () => {
    if (!status?.setting) {
      setForm(defaultFormState);
    } else {
      setForm({
        engine: status.setting.engine,
        displayName: status.setting.displayName ?? 'Primary Application Database',
        connection: {
          ...defaultConnectionState,
          port: getDefaultPort(status.setting.engine)
        }
      });
    }
    setValidationError(null);
    setLocalTestResult(null);
    setAdminEmailInput(status?.adminEmail ?? '');
    setAdminEmailError(null);
  };

  const validateForm = (): boolean => {
    if (!requiresConnection) {
      setValidationError(null);
      return true;
    }

    const missing: string[] = [];
    const { host, database, username, password } = form.connection;
    if (!host.trim()) missing.push('Host');
    if (!database.trim()) missing.push('Database');
    if (!username.trim()) missing.push('Username');
    if (!password.trim()) missing.push('Password');

    if (missing.length) {
      setValidationError(`Provide values for: ${missing.join(', ')}`);
      return false;
    }

    setValidationError(null);
    return true;
  };

  const getPayload = (): ApplicationDatabaseApplyInput | null => {
    try {
      const payload = buildPayload(form);
      return payload;
    } catch (err) {
      if (err instanceof Error) {
        setValidationError(err.message);
      } else {
        setValidationError('Invalid connection options.');
      }
      return null;
    }
  };

  const handleTest = async () => {
    if (!canManage) {
      return;
    }
    if (!validateForm()) {
      return;
    }
    const payload = getPayload();
    if (!payload) {
      return;
    }
    const result = await testConnection(payload);
    setLocalTestResult(result);
  };

  const handleApply = async () => {
    if (!canManage) {
      return;
    }
    if (!validateForm()) {
      return;
    }
    const payload = getPayload();
    if (!payload) {
      return;
    }
    await applySetting(payload);
    setLocalTestResult(null);
  };

  if (isLoading && !status) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <Typography>Loading configuration…</Typography>
      </Box>
    );
  }

  const errorMessage = isError
    ? error instanceof Error
      ? error.message
      : 'Unable to load application database status.'
    : null;

  return (
    <Box>
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.secondary.main, 0.15)} 0%, ${alpha(theme.palette.secondary.dark, 0.08)} 100%)`,
          borderBottom: `3px solid ${alpha(theme.palette.secondary.main, 0.6)}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.secondary.main, 0.18)}`
        }}
      >
        <Typography variant="h4" gutterBottom sx={{ fontWeight: 800 }}>
          Application Database Setup
        </Typography>
        <Typography variant="body2" sx={{ opacity: 0.85 }}>
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
          You have read-only access to this configuration. Contact an administrator to make changes.
        </Alert>
      )}
      <Stepper nonLinear activeStep={activeStep} sx={{ mb: 3, maxWidth: 800 }}>
        {['Database', 'Admin Contact'].map((label, index) => (
          <Step key={label} completed={index === 0 ? status?.configured ?? false : Boolean(status?.adminEmail)}>
            <StepButton
              color="inherit"
              onClick={() => handleStepChange(index)}
              disabled={index === 1 && !status?.configured}
            >
              {label}
            </StepButton>
          </Step>
        ))}
      </Stepper>

      {activeStep === 0 ? (
        <>
          {status?.configured && status.setting && (
            <Alert severity="success" sx={{ mb: 3 }}>
              Active configuration: <strong>{status.setting.displayName ?? 'Configured'}</strong> using
              <strong> {status.setting.engine.replace(/_/g, ' ')}</strong>. Connection summary:{' '}
              {status.setting.connectionDisplay ?? 'Internal default connection.'}
            </Alert>
          )}

          {validationError && (
            <Alert severity="warning" sx={{ mb: 3 }}>
              {validationError}
            </Alert>
          )}

          {localTestResult && (
            <Alert severity={localTestResult.success ? 'success' : 'error'} sx={{ mb: 3 }}>
              {localTestResult.message}
              {localTestResult.success && localTestResult.latencyMs != null
                ? ` (${localTestResult.latencyMs.toFixed(0)} ms)`
                : ''}
            </Alert>
          )}

          <Paper elevation={3} sx={{ p: 3, maxWidth: 800 }}>
            <Stack spacing={3}>
              <TextField
                label="Display Name"
                value={form.displayName}
                onChange={handleDisplayNameChange}
                disabled={!canManage || busy}
                helperText="Shown across the UI for context."
                fullWidth
              />

              <FormControl component="fieldset" disabled={!canManage || busy}>
                <FormLabel component="legend">Database Engine</FormLabel>
                <RadioGroup row value={form.engine} onChange={handleEngineChange}>
                  <FormControlLabel value="default_postgres" control={<Radio />} label="Default Postgres" />
                  <FormControlLabel value="custom_postgres" control={<Radio />} label="Custom Postgres" />
                  <FormControlLabel value="sqlserver" control={<Radio />} label="SQL Server" />
                </RadioGroup>
              </FormControl>

              {requiresConnection && (
                <Stack spacing={3}>
                  <Typography variant="subtitle1" fontWeight={600}>
                    Connection Details
                  </Typography>
                  <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                    <TextField
                      label="Host"
                      value={form.connection.host}
                      onChange={handleConnectionChange('host')}
                      disabled={!canManage || busy}
                      required
                      fullWidth
                    />
                    <TextField
                      label="Port"
                      type="number"
                      value={form.connection.port}
                      onChange={handleConnectionChange('port')}
                      disabled={!canManage || busy}
                      required
                      fullWidth
                    />
                  </Stack>
                  <TextField
                    label={form.engine === 'sqlserver' ? 'Database / Catalog' : 'Database'}
                    value={form.connection.database}
                    onChange={handleConnectionChange('database')}
                    disabled={!canManage || busy}
                    required
                    fullWidth
                  />
                  <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                    <TextField
                      label="Username"
                      value={form.connection.username}
                      onChange={handleConnectionChange('username')}
                      disabled={!canManage || busy}
                      required
                      fullWidth
                    />
                    <TextField
                      label="Password"
                      type="password"
                      value={form.connection.password}
                      onChange={handleConnectionChange('password')}
                      disabled={!canManage || busy}
                      required
                      autoComplete="new-password"
                      fullWidth
                    />
                  </Stack>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={form.connection.useSsl}
                        onChange={handleUseSslChange}
                        disabled={!canManage || busy}
                        color="primary"
                      />
                    }
                    label="Use SSL / encrypted connection"
                  />
                  <TextField
                    label="Additional Options"
                    value={form.connection.optionsText}
                    onChange={handleConnectionChange('optionsText')}
                    disabled={!canManage || busy}
                    placeholder="key=value (one per line)"
                    multiline
                    minRows={3}
                    fullWidth
                  />
                  <Alert severity="info">
                    Credentials are stored securely but cannot be retrieved later. Provide complete details when
                    updating the configuration.
                  </Alert>
                </Stack>
              )}

              <Stack direction="row" spacing={2} justifyContent="flex-end">
                <Button variant="outlined" onClick={resetForm} disabled={busy}>
                  Reset
                </Button>
                <Button
                  variant="outlined"
                  onClick={handleTest}
                  disabled={!canManage || busy || (requiresConnection && !connectionRequirementsMet)}
                >
                  {testing ? 'Testing…' : 'Test Connection'}
                </Button>
                <Button
                  variant="contained"
                  onClick={handleApply}
                  disabled={!canManage || busy || (requiresConnection && !connectionRequirementsMet)}
                >
                  {applying ? 'Saving…' : status?.configured ? 'Save Configuration' : 'Apply Configuration'}
                </Button>
              </Stack>
            </Stack>
          </Paper>

          {(isFetching || busy) && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 3, color: 'text.secondary' }}>
              <Typography variant="body2">
                {busy ? 'Processing request…' : 'Refreshing status…'}
              </Typography>
            </Box>
          )}
        </>
      ) : (
        <>
          {!status?.configured && (
            <Alert severity="warning" sx={{ mb: 3 }}>
              Configure the application database before setting the admin email.
            </Alert>
          )}

          {status?.configured && !status.adminEmail && (
            <Alert severity="info" sx={{ mb: 3 }}>
              Provide an admin email address to finish the initial setup. This address receives elevated access when
              using the mock login flow.
            </Alert>
          )}

          {status?.adminEmail && (
            <Alert severity="success" sx={{ mb: 3 }}>
              Current admin email: <strong>{status.adminEmail}</strong>
            </Alert>
          )}

          {adminEmailError && (
            <Alert severity="warning" sx={{ mb: 3 }}>
              {adminEmailError}
            </Alert>
          )}

          <Paper elevation={3} sx={{ p: 3, maxWidth: 600 }}>
            <Stack spacing={3}>
              <TextField
                label="Admin Email"
                value={adminEmailInput}
                onChange={handleAdminEmailChange}
                type="email"
                disabled={!canManage || settingAdminEmail}
                helperText="Used for the mock login experience until SSO is configured."
                required
                fullWidth
              />
              <Stack direction="row" spacing={2} justifyContent="flex-end">
                <Button variant="outlined" onClick={resetAdminEmailForm} disabled={settingAdminEmail}>
                  Reset
                </Button>
                <Button
                  variant="contained"
                  onClick={handleAdminEmailSave}
                  disabled={!canManage || settingAdminEmail}
                >
                  {settingAdminEmail ? 'Saving…' : 'Save Admin Email'}
                </Button>
              </Stack>
            </Stack>
          </Paper>

          {(isFetching || settingAdminEmail) && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 3, color: 'text.secondary' }}>
              <Typography variant="body2">
                {settingAdminEmail ? 'Saving admin email…' : 'Refreshing status…'}
              </Typography>
            </Box>
          )}
        </>
      )}
    </Box>
  );
};

export default ApplicationDatabaseSetupPage;
