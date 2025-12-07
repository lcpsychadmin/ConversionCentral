import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';

import { SystemConnection, SystemConnectionFormValues } from '../../types/data';
import { buildJdbcConnectionString, parseJdbcConnectionString } from '../../utils/connectionString';

interface DatabricksConnectionDialogProps {
  open: boolean;
  title: string;
  initialValues?: SystemConnection | null;
  loading?: boolean;
  testing?: boolean;
  onClose: () => void;
  onSubmit: (values: SystemConnectionFormValues, connectionString: string | null) => void;
  onTest?: (values: SystemConnectionFormValues, connectionString: string | null) => void;
}

interface FormState {
  name: string;
  workspaceHost: string;
  httpPath: string;
  personalAccessToken: string;
  catalog: string;
  schemaName: string;
  notes: string;
  active: boolean;
}

const buildInitialState = (
  initialValues?: SystemConnection | null
): FormState => {
  const parsed = initialValues ? parseJdbcConnectionString(initialValues.connectionString) : null;
  const httpPathOption = parsed?.options?.httpPath ?? parsed?.options?.HttpPath ?? '';
  const catalogOption = parsed?.options?.catalog ?? parsed?.options?.Catalog ?? '';
  const schemaOption = parsed?.options?.schema ?? parsed?.options?.Schema;
  const schemaFromPath = parsed?.database?.trim() ?? '';
  const schemaSource = schemaOption ?? schemaFromPath;
  const schemaName = typeof schemaSource === 'string' ? schemaSource.trim() : '';

  return {
    name: initialValues?.name ?? 'Databricks Connection',
    workspaceHost: parsed?.host ?? '',
    httpPath: httpPathOption,
    personalAccessToken: parsed?.password ?? '',
    catalog: catalogOption,
    schemaName,
    notes: initialValues?.notes ?? '',
    active: initialValues?.active ?? true
  };
};

const buildConnectionValues = (state: FormState): SystemConnectionFormValues => {
  const trimmedSchema = state.schemaName.trim();
  const trimmedCatalog = state.catalog.trim();

  return {
  name: state.name.trim() || 'Databricks Connection',
  databaseType: 'databricks',
  host: state.workspaceHost.trim(),
  port: '443',
  database: trimmedSchema || 'default',
  username: 'token',
  password: state.personalAccessToken,
  options: {
    transportMode: 'http',
    ssl: '1',
    httpPath: state.httpPath.trim(),
    http_path: state.httpPath.trim(),
    AuthMech: '3',
    ...(trimmedCatalog
      ? { catalog: trimmedCatalog, Catalog: trimmedCatalog }
      : {}),
    schema: trimmedSchema
  },
  notes: state.notes.trim() ? state.notes.trim() : null,
  active: state.active
  };
};

const DatabricksConnectionDialog = ({
  open,
  title,
  initialValues,
  loading = false,
  testing = false,
  onClose,
  onSubmit,
  onTest
}: DatabricksConnectionDialogProps) => {
  const initialState = useMemo(
    () => buildInitialState(initialValues),
    [initialValues]
  );
  const [form, setForm] = useState<FormState>(initialState);
  const [errors, setErrors] = useState<Record<string, string | undefined>>({});

  useEffect(() => {
    setForm(initialState);
    setErrors({});
  }, [initialState, open]);

  const handleChange = (field: keyof FormState) => (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const value = event.target.value;
    setForm((prev) => ({ ...prev, [field]: value }));
    setErrors((prev) => ({ ...prev, [field]: undefined, form: undefined }));
  };

  const handleToggle = (
    _event: ChangeEvent<HTMLInputElement>,
    checked: boolean
  ) => {
    setForm((prev) => ({ ...prev, active: checked }));
  };

  const resetAndClose = () => {
    setForm(initialState);
    setErrors({});
    onClose();
  };

  const validate = (): boolean => {
    const nextErrors: Record<string, string> = {};
    if (!form.name.trim()) {
      nextErrors.name = 'Connection name is required.';
    }
    if (!form.workspaceHost.trim()) {
      nextErrors.workspaceHost = 'Host is required.';
    }
    if (!form.httpPath.trim()) {
      nextErrors.httpPath = 'HTTP path is required.';
    }
    if (!form.personalAccessToken.trim()) {
      nextErrors.personalAccessToken = 'Personal access token is required.';
    }
    setErrors(nextErrors);
    return Object.keys(nextErrors).length === 0;
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!validate()) {
      return;
    }

    try {
      const values = buildConnectionValues(form);
      const connectionString = buildJdbcConnectionString(values);
      onSubmit(values, connectionString);
    } catch (error) {
      setErrors((prev) => ({
        ...prev,
        form: error instanceof Error ? error.message : 'Unable to build connection string.'
      }));
    }
  };

  const handleTest = () => {
    if (!onTest) {
      return;
    }
    if (!validate()) {
      return;
    }

    try {
      const values = buildConnectionValues(form);
      const connectionString = buildJdbcConnectionString(values);
      onTest(values, connectionString);
    } catch (error) {
      setErrors((prev) => ({
        ...prev,
        form: error instanceof Error ? error.message : 'Unable to build connection string.'
      }));
    }
  };

  const isDirty = useMemo(() => {
    return (
      form.name !== initialState.name ||
      form.workspaceHost !== initialState.workspaceHost ||
      form.httpPath !== initialState.httpPath ||
      form.personalAccessToken !== initialState.personalAccessToken ||
      form.catalog !== initialState.catalog ||
      form.schemaName !== initialState.schemaName ||
      form.notes !== initialState.notes ||
      form.active !== initialState.active
    );
  }, [form, initialState]);

  const canTest = Boolean(
    form.workspaceHost.trim() && form.httpPath.trim() && form.personalAccessToken.trim()
  );

  return (
    <Dialog open={open} onClose={resetAndClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {errors.form && <Alert severity="error">{errors.form}</Alert>}
            <TextField
              label="Connection Name"
              required
              fullWidth
              value={form.name}
              onChange={handleChange('name')}
              helperText={errors.name ?? 'Displayed throughout the workspace.'}
              error={!!errors.name}
            />
            <Box>
              <Typography variant="overline" color="text.secondary">
                Data source
              </Typography>
              <Typography variant="h6" sx={{ lineHeight: 1.2 }}>
                Databricks SQL Warehouse
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Provide the Databricks workspace host, SQL warehouse HTTP path, and a PAT with access.
              </Typography>
            </Box>

            <TextField
              label="Server Hostname"
              required
              fullWidth
              value={form.workspaceHost}
              onChange={handleChange('workspaceHost')}
              helperText={errors.workspaceHost ?? 'Example: dbc-12345-abc.cloud.databricks.com'}
              error={!!errors.workspaceHost}
            />

            <TextField
              label="HTTP Path"
              required
              fullWidth
              value={form.httpPath}
              onChange={handleChange('httpPath')}
              helperText={
                errors.httpPath ??
                'Example: /sql/1.0/warehouses/abc123 or /sql/1.0/endpoints/abc123.'
              }
              error={!!errors.httpPath}
            />

            <TextField
              label="Personal Access Token"
              type="password"
              required
              fullWidth
              value={form.personalAccessToken}
              onChange={handleChange('personalAccessToken')}
              helperText={errors.personalAccessToken ?? 'Must have access to the target SQL warehouse.'}
              error={!!errors.personalAccessToken}
              autoComplete="off"
            />

            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
              <TextField
                label="Catalog (optional)"
                fullWidth
                value={form.catalog}
                onChange={handleChange('catalog')}
                helperText="Overrides the Unity catalog used for this connection."
              />
              <TextField
                label="Schema / Database (optional)"
                fullWidth
                value={form.schemaName}
                onChange={handleChange('schemaName')}
                helperText={
                  errors.schemaName ?? 'Leave blank to browse every schema inside the selected catalog.'
                }
                error={!!errors.schemaName}
              />
            </Stack>

            <TextField
              label="Notes"
              fullWidth
              multiline
              minRows={2}
              value={form.notes}
              onChange={handleChange('notes')}
            />

            <FormControlLabel
              control={<Switch checked={form.active} onChange={handleToggle} />}
              label={form.active ? 'Connection is active' : 'Connection is disabled'}
            />
          </Stack>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'space-between' }}>
          <Button onClick={resetAndClose} disabled={loading || testing}>
            Cancel
          </Button>
          <Box display="flex" gap={1}>
            {onTest && (
              <LoadingButton onClick={handleTest} loading={testing} disabled={!canTest || loading}>
                Test Connection
              </LoadingButton>
            )}
            <LoadingButton
              type="submit"
              variant="contained"
              loading={loading}
              disabled={loading || (!!initialValues && !isDirty)}
            >
              Save Connection
            </LoadingButton>
          </Box>
        </DialogActions>
      </Box>
    </Dialog>
  );
};

export default DatabricksConnectionDialog;
