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
  MenuItem,
  Stack,
  Switch,
  TextField
} from '@mui/material';

import {
  RelationalDatabaseType,
  System,
  SystemConnection,
  SystemConnectionFormValues
} from '../../types/data';
import { buildJdbcConnectionString, parseJdbcConnectionString } from '../../utils/connectionString';

interface SystemConnectionFormProps {
  open: boolean;
  title: string;
  systems: System[];
  initialValues?: SystemConnection | null;
  loading?: boolean;
  testing?: boolean;
  onClose: () => void;
  onSubmit: (values: SystemConnectionFormValues, connectionString: string) => void;
  onTest?: (values: SystemConnectionFormValues, connectionString: string) => void;
}

type FieldErrorMap = Partial<Record<keyof SystemConnectionFormValues | 'form', string>>;

const DATABASE_OPTIONS: RelationalDatabaseType[] = ['postgresql'];

const sanitizeNotes = (notes?: string | null) => notes ?? '';

const buildInitialSnapshot = (
  systems: System[],
  initialValues?: SystemConnection | null
): SystemConnectionFormValues => {
  const parsed = initialValues ? parseJdbcConnectionString(initialValues.connectionString) : null;

  return {
    systemId: initialValues?.systemId ?? '',
    databaseType: parsed?.databaseType ?? 'postgresql',
    host: parsed?.host ?? '',
    port: parsed?.port || '5432',
    database: parsed?.database ?? '',
    username: parsed?.username ?? '',
    password: parsed?.password ?? '',
    options: parsed?.options ?? {},
    notes: sanitizeNotes(initialValues?.notes),
    active: initialValues?.active ?? true,
    ingestionEnabled: initialValues?.ingestionEnabled ?? true
  };
};

const validateValues = (values: SystemConnectionFormValues): FieldErrorMap => {
  const errors: FieldErrorMap = {};

  if (!values.systemId) {
    errors.systemId = 'System is required';
  }

  if (!values.host.trim()) {
    errors.host = 'Host is required';
  }

  if (!values.port.trim()) {
    errors.port = 'Port is required';
  } else if (!/^\d+$/.test(values.port)) {
    errors.port = 'Port must be numeric';
  }

  if (!values.database.trim()) {
    errors.database = 'Database is required';
  }

  if (!values.username.trim()) {
    errors.username = 'Username is required';
  }

  if (!values.password.trim()) {
    errors.password = 'Password is required';
  }

  return errors;
};

const normalizeValues = (
  values: SystemConnectionFormValues
): SystemConnectionFormValues => ({
  ...values,
  host: values.host.trim(),
  port: values.port.trim(),
  database: values.database.trim(),
  username: values.username.trim(),
  password: values.password,
  notes: values.notes?.trim() ? values.notes.trim() : null
});

const SystemConnectionForm = ({
  open,
  title,
  systems,
  initialValues,
  loading = false,
  testing = false,
  onClose,
  onSubmit,
  onTest
}: SystemConnectionFormProps) => {
  const initialSnapshot = useMemo(
    () => buildInitialSnapshot(systems, initialValues),
    [systems, initialValues]
  );

  const [values, setValues] = useState<SystemConnectionFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<FieldErrorMap>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof SystemConnectionFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setValues((prev) => ({ ...prev, [field]: value }));
      setErrors((prev) => ({ ...prev, [field]: undefined, form: undefined }));
    };

  const handleToggleActive = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setValues((prev) => ({ ...prev, active: checked }));
  };

  const handleToggleIngestion = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setValues((prev) => ({ ...prev, ingestionEnabled: checked }));
  };

  const resetAndClose = () => {
    setValues(initialSnapshot);
    setErrors({});
    onClose();
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const validationErrors = validateValues(values);
    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    try {
      const normalized = normalizeValues(values);
      const connectionString = buildJdbcConnectionString(normalized);
      onSubmit(normalized, connectionString);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to build connection string.';
      setErrors((prev) => ({ ...prev, form: message }));
    }
  };

  const handleTest = () => {
    if (!onTest) return;

    const validationErrors = validateValues(values);
    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    try {
      const normalized = normalizeValues(values);
      const connectionString = buildJdbcConnectionString(normalized);
      onTest(normalized, connectionString);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to build connection string.';
      setErrors((prev) => ({ ...prev, form: message }));
    }
  };

  const isDirty = useMemo(() => {
    return (
      values.systemId !== initialSnapshot.systemId ||
      values.databaseType !== initialSnapshot.databaseType ||
      values.host !== initialSnapshot.host ||
      values.port !== initialSnapshot.port ||
      values.database !== initialSnapshot.database ||
      values.username !== initialSnapshot.username ||
      values.password !== initialSnapshot.password ||
      sanitizeNotes(values.notes) !== sanitizeNotes(initialSnapshot.notes) ||
      values.active !== initialSnapshot.active ||
      values.ingestionEnabled !== initialSnapshot.ingestionEnabled
    );
  }, [values, initialSnapshot]);

  const canTest = Boolean(
    values.systemId &&
      values.host.trim() &&
      values.port.trim() &&
      /^\d+$/.test(values.port) &&
      values.database.trim() &&
      values.username.trim() &&
      values.password.trim()
  );

  return (
    <Dialog open={open} onClose={resetAndClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {errors.form && <Alert severity="error">{errors.form}</Alert>}
            <TextField
              select
              required
              label="System"
              fullWidth
              id="connection-system"
              name="systemId"
              value={values.systemId}
              onChange={handleChange('systemId')}
              error={!!errors.systemId}
              helperText={errors.systemId}
            >
              <MenuItem value="" disabled>
                Select a system
              </MenuItem>
              {systems.map((system) => (
                <MenuItem key={system.id} value={system.id}>
                  {system.name}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Database Type"
              fullWidth
              id="connection-database-type"
              name="databaseType"
              value={values.databaseType}
              onChange={handleChange('databaseType')}
              helperText="Relational engines supported today"
            >
              {DATABASE_OPTIONS.map((option) => (
                <MenuItem key={option} value={option}>
                  {option === 'postgresql' ? 'PostgreSQL' : option}
                </MenuItem>
              ))}
            </TextField>
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
              <TextField
                label="Host"
                fullWidth
                required
                id="connection-host"
                name="host"
                value={values.host}
                onChange={handleChange('host')}
                error={!!errors.host}
                helperText={errors.host ?? 'Hostname or IP address'}
              />
              <TextField
                label="Port"
                fullWidth
                required
                id="connection-port"
                name="port"
                value={values.port}
                onChange={handleChange('port')}
                error={!!errors.port}
                helperText={errors.port ?? 'Default 5432'}
              />
            </Stack>
            <TextField
              label="Database"
              fullWidth
              required
              id="connection-database"
              name="database"
              value={values.database}
              onChange={handleChange('database')}
              error={!!errors.database}
              helperText={errors.database}
            />
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
              <TextField
                label="Username"
                fullWidth
                required
                id="connection-username"
                name="username"
                value={values.username}
                onChange={handleChange('username')}
                error={!!errors.username}
                helperText={errors.username}
              />
              <TextField
                label="Password"
                fullWidth
                required
                id="connection-password"
                name="password"
                type="password"
                value={values.password}
                onChange={handleChange('password')}
                error={!!errors.password}
                helperText={errors.password}
              />
            </Stack>
            <TextField
              label="Notes"
              fullWidth
              multiline
              minRows={2}
              id="connection-notes"
              name="notes"
              value={values.notes ?? ''}
              onChange={handleChange('notes')}
            />
            <FormControlLabel
              control={<Switch checked={values.active} onChange={handleToggleActive} />}
              label={values.active ? 'Connection is active' : 'Connection is disabled'}
            />
            <FormControlLabel
              control={
                <Switch
                  checked={values.ingestionEnabled}
                  onChange={handleToggleIngestion}
                  color="secondary"
                />
              }
              label={
                values.ingestionEnabled
                  ? 'Ingestion features enabled'
                  : 'Hide ingestion features for this connection'
              }
            />
          </Stack>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'space-between' }}>
          <Button onClick={resetAndClose} disabled={loading || testing}>
            Cancel
          </Button>
          <Box display="flex" gap={1}>
            {onTest && (
              <LoadingButton
                onClick={handleTest}
                loading={testing}
                disabled={!canTest || loading}
              >
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

export default SystemConnectionForm;
