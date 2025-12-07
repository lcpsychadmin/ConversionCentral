import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';

import {
  RelationalDatabaseType,
  SystemConnection,
  SystemConnectionFormValues
} from '../../types/data';
import { buildJdbcConnectionString, parseJdbcConnectionString } from '../../utils/connectionString';

interface SystemConnectionFormProps {
  open: boolean;
  title: string;
  initialValues?: SystemConnection | null;
  loading?: boolean;
  testing?: boolean;
  onClose: () => void;
  onSubmit: (values: SystemConnectionFormValues, connectionString: string | null) => void;
  onTest?: (values: SystemConnectionFormValues, connectionString: string | null) => void;
  forcedDatabaseType?: RelationalDatabaseType | null;
  showDatabaseTypeSelector?: boolean;
}

type FieldErrorMap = Partial<Record<keyof SystemConnectionFormValues | 'form', string>>;

export const DATABASE_OPTIONS: RelationalDatabaseType[] = ['postgresql', 'databricks', 'sap'];

export const DATABASE_LABELS: Record<RelationalDatabaseType, string> = {
  postgresql: 'PostgreSQL',
  databricks: 'Databricks SQL Warehouse',
  sap: 'SAP HANA'
};

const DEFAULT_PORT_BY_DATABASE: Record<RelationalDatabaseType, string> = {
  postgresql: '5432',
  databricks: '443',
  sap: '30015'
};

const DATABASE_HELPER_TEXT: Partial<Record<RelationalDatabaseType, string>> = {
  sap: 'Tenant database name (case-sensitive).'
};

const getDefaultPort = (databaseType: RelationalDatabaseType): string =>
  DEFAULT_PORT_BY_DATABASE[databaseType] ?? '5432';

const sanitizeNotes = (notes?: string | null) => notes ?? '';

const buildInitialSnapshot = (
  initialValues?: SystemConnection | null,
  forcedType?: RelationalDatabaseType | null
): SystemConnectionFormValues => {
  const parsed = initialValues ? parseJdbcConnectionString(initialValues.connectionString) : null;
  const parsedOptions = parsed?.options ?? {};
  const resolvedType = parsed?.databaseType ?? forcedType ?? 'postgresql';

  return {
    name: initialValues?.name ?? 'Connection',
    databaseType: resolvedType,
    host: parsed?.host ?? '',
    port: parsed?.port || getDefaultPort(resolvedType),
    database: parsed?.database ?? '',
    username: parsed?.username ?? '',
    password: parsed?.password ?? '',
    options: parsedOptions,
    notes: sanitizeNotes(initialValues?.notes),
    active: initialValues?.active ?? true
  };
};

const validateValues = (values: SystemConnectionFormValues): FieldErrorMap => {
  const errors: FieldErrorMap = {};

  const trimmedName = values.name.trim();
  if (!trimmedName) {
    errors.name = 'Connection name is required';
  } else if (trimmedName.length > 120) {
    errors.name = 'Connection name must be 120 characters or fewer';
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

const normalizeValues = (values: SystemConnectionFormValues): SystemConnectionFormValues => ({
  ...values,
  name: values.name.trim(),
  host: values.host.trim(),
  port: values.port.trim(),
  database: values.database.trim(),
  username: values.username.trim(),
  password: values.password,
  notes: values.notes?.trim() ? values.notes.trim() : null,
});

const SystemConnectionForm = ({
  open,
  title,
  initialValues,
  loading = false,
  testing = false,
  onClose,
  onSubmit,
  onTest,
  forcedDatabaseType = null,
  showDatabaseTypeSelector = true
}: SystemConnectionFormProps) => {
  const initialSnapshot = useMemo(
    () => buildInitialSnapshot(initialValues, forcedDatabaseType),
    [initialValues, forcedDatabaseType]
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
      setValues((prev) => {
        if (field === 'databaseType') {
          const nextType = value as RelationalDatabaseType;
          const currentDefault = getDefaultPort(prev.databaseType);
          const shouldResetPort = !prev.port || prev.port === currentDefault;
          return {
            ...prev,
            databaseType: nextType,
            port: shouldResetPort ? getDefaultPort(nextType) : prev.port
          };
        }
        return { ...prev, [field]: value };
      });
      setErrors((prev) => {
        const next = { ...prev, [field]: undefined, form: undefined };
        if (field === 'databaseType') {
          next.port = undefined;
        }
        return next;
      });
    };

  const handleToggleActive = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setValues((prev) => ({ ...prev, active: checked }));
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

    const normalized = normalizeValues(values);

    try {
      const connectionString = buildJdbcConnectionString(normalized);
      onSubmit(normalized, connectionString);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to build connection string.';
      setErrors((prev) => ({ ...prev, form: message }));
    }
  };

  const handleTest = () => {
    if (!onTest) {
      return;
    }

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
      values.name !== initialSnapshot.name ||
      values.databaseType !== initialSnapshot.databaseType ||
      values.host !== initialSnapshot.host ||
      values.port !== initialSnapshot.port ||
      values.database !== initialSnapshot.database ||
      values.username !== initialSnapshot.username ||
      values.password !== initialSnapshot.password ||
      sanitizeNotes(values.notes) !== sanitizeNotes(initialSnapshot.notes) ||
      values.active !== initialSnapshot.active
    );
  }, [values, initialSnapshot]);

  const canTest = Boolean(
    values.host.trim() &&
      values.port.trim() &&
      /^\d+$/.test(values.port) &&
      values.database.trim() &&
      values.username.trim() &&
      values.password.trim()
  );

  const databaseLabel = values.databaseType === 'sap' ? 'Database / Tenant' : 'Database';
  const databaseHelper = errors.database ?? DATABASE_HELPER_TEXT[values.databaseType];
  const portHelper = errors.port ?? `Default ${getDefaultPort(values.databaseType)}`;
  const hostHelper = errors.host ?? 'Hostname or IP address';
  const usernameHelper = errors.username ?? undefined;
  const passwordHelper = errors.password ?? undefined;

  return (
    <Dialog open={open} onClose={resetAndClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {errors.form && <Alert severity="error">{errors.form}</Alert>}

            <TextField
              label="Connection Name"
              fullWidth
              required
              id="connection-name"
              name="name"
              value={values.name}
              onChange={handleChange('name')}
              helperText={errors.name ?? 'Displayed throughout the workspace.'}
              error={!!errors.name}
            />

            {showDatabaseTypeSelector ? (
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
                    {DATABASE_LABELS[option] ?? option}
                  </MenuItem>
                ))}
              </TextField>
            ) : (
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography variant="overline" color="text.secondary">
                    Data source
                  </Typography>
                  <Typography variant="subtitle1" fontWeight={600} lineHeight={1.2}>
                    {DATABASE_LABELS[values.databaseType] ?? values.databaseType}
                  </Typography>
                </Box>
                <Chip label={DATABASE_LABELS[values.databaseType] ?? values.databaseType} size="small" />
              </Box>
            )}

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
                helperText={hostHelper}
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
                helperText={portHelper}
              />
            </Stack>
            <TextField
              label={databaseLabel}
              fullWidth
              required
              id="connection-database"
              name="database"
              value={values.database}
              onChange={handleChange('database')}
              error={!!errors.database}
              helperText={databaseHelper}
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
                helperText={usernameHelper}
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
                helperText={passwordHelper}
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

export default SystemConnectionForm;
