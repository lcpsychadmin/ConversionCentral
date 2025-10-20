import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Alert,
  Box,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  FormHelperText,
  InputLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import Select, { SelectChangeEvent } from '@mui/material/Select';

import { IngestionLoadStrategy, IngestionSchedule } from '../../types/data';

export interface ScheduleSelectionOption {
  id: string;
  label: string;
  schemaName: string;
  tableName: string;
  targetPreview: string;
  disabled?: boolean;
}

interface IngestionScheduleFormProps {
  open: boolean;
  title: string;
  options: ScheduleSelectionOption[];
  initialValues?: IngestionSchedule | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: FormValues) => void;
  disableSelectionChange?: boolean;
}

export interface FormValues {
  connectionTableSelectionId: string;
  scheduleExpression: string;
  timezone: string;
  loadStrategy: IngestionLoadStrategy;
  watermarkColumn?: string;
  primaryKeyColumn?: string;
  targetSchema?: string;
  batchSize: number;
  isActive: boolean;
}

type FieldErrorMap = Partial<Record<keyof FormValues | 'form', string>>;

const DEFAULT_VALUES: FormValues = {
  connectionTableSelectionId: '',
  scheduleExpression: '0 2 * * *',
  timezone: 'UTC',
  loadStrategy: 'timestamp',
  watermarkColumn: '',
  primaryKeyColumn: '',
  targetSchema: '',
  batchSize: 5000,
  isActive: true
};

const buildInitialValues = (
  initial: IngestionSchedule | null | undefined,
  options: ScheduleSelectionOption[]
): FormValues => {
  if (!initial) {
    const firstOption = options.find((option) => !option.disabled) ?? options[0];
    return {
      ...DEFAULT_VALUES,
      connectionTableSelectionId: firstOption?.id ?? ''
    };
  }
  return {
    connectionTableSelectionId: initial.connectionTableSelectionId,
    scheduleExpression: initial.scheduleExpression,
    timezone: initial.timezone ?? 'UTC',
    loadStrategy: initial.loadStrategy,
    watermarkColumn: initial.watermarkColumn ?? '',
    primaryKeyColumn: initial.primaryKeyColumn ?? '',
    targetSchema: initial.targetSchema ?? '',
    batchSize: initial.batchSize,
    isActive: initial.isActive
  };
};

const validate = (values: FormValues): FieldErrorMap => {
  const errors: FieldErrorMap = {};

  if (!values.connectionTableSelectionId) {
    errors.connectionTableSelectionId = 'Select a table to ingest.';
  }

  if (!values.scheduleExpression.trim()) {
    errors.scheduleExpression = 'Cron expression is required.';
  }

  if (values.loadStrategy === 'timestamp' && !values.watermarkColumn?.trim()) {
    errors.watermarkColumn = 'Watermark column is required for timestamp strategy.';
  }

  if (values.loadStrategy === 'numeric_key' && !values.primaryKeyColumn?.trim()) {
    errors.primaryKeyColumn = 'Primary key column is required for numeric key strategy.';
  }

  if (values.batchSize < 1) {
    errors.batchSize = 'Batch size must be positive.';
  }

  return errors;
};

const trimString = (value?: string) => (value ? value.trim() : undefined);

const normalize = (values: FormValues): FormValues => ({
  ...values,
  scheduleExpression: values.scheduleExpression.trim(),
  timezone: values.timezone.trim() || 'UTC',
  watermarkColumn: trimString(values.watermarkColumn),
  primaryKeyColumn: trimString(values.primaryKeyColumn),
  targetSchema: trimString(values.targetSchema),
  batchSize: Number(values.batchSize),
  connectionTableSelectionId: values.connectionTableSelectionId
});

const IngestionScheduleForm = ({
  open,
  title,
  options,
  initialValues = null,
  loading = false,
  onClose,
  onSubmit,
  disableSelectionChange = false
}: IngestionScheduleFormProps) => {
  const initialSnapshot = useMemo(
    () => buildInitialValues(initialValues, options),
    [initialValues, options]
  );

  const [values, setValues] = useState<FormValues>(initialSnapshot);
  const [errors, setErrors] = useState<FieldErrorMap>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = <K extends keyof FormValues>(field: K) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const next = event.target.value;
      setValues((prev) => ({ ...prev, [field]: field === 'batchSize' ? Number(next) || 0 : next }));
      setErrors((prev) => ({ ...prev, [field]: undefined, form: undefined }));
    };

  const handleStrategyChange = (event: SelectChangeEvent<IngestionLoadStrategy>) => {
    const next = event.target.value as IngestionLoadStrategy;
    setValues((prev) => ({ ...prev, loadStrategy: next }));
    setErrors((prev) => ({ ...prev, watermarkColumn: undefined, primaryKeyColumn: undefined }));
  };

  const handleToggleActive = (_event: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setValues((prev) => ({ ...prev, isActive: checked }));
  };

  const resetAndClose = () => {
    setValues(initialSnapshot);
    setErrors({});
    onClose();
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const validationErrors = validate(values);
    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    try {
      const normalized = normalize(values);
      onSubmit(normalized);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to prepare schedule payload.';
      setErrors((prev) => ({ ...prev, form: message }));
    }
  };

  const selectedOption = options.find((option) => option.id === values.connectionTableSelectionId);
  const targetPreview = selectedOption?.targetPreview;

  return (
    <Dialog open={open} onClose={resetAndClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {errors.form && <Alert severity="error">{errors.form}</Alert>}
            <FormControl fullWidth required error={!!errors.connectionTableSelectionId}>
              <InputLabel id="ingestion-table-label">Table Selection</InputLabel>
              <Select
                labelId="ingestion-table-label"
                label="Table Selection"
                value={values.connectionTableSelectionId}
                disabled={disableSelectionChange}
                onChange={(event: SelectChangeEvent<string>) => {
                  setValues((prev) => ({
                    ...prev,
                    connectionTableSelectionId: event.target.value
                  }));
                  setErrors((prev) => ({ ...prev, connectionTableSelectionId: undefined }));
                }}
              >
                {options.map((option) => (
                  <MenuItem key={option.id} value={option.id} disabled={option.disabled}>
                    {option.disabled ? `${option.label} (not selected)` : option.label}
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText>{errors.connectionTableSelectionId}</FormHelperText>
            </FormControl>

            {targetPreview && (
              <Typography variant="body2" color="text.secondary">
                Target table will be <strong>{targetPreview}</strong>
              </Typography>
            )}

            <TextField
              label="Cron Expression"
              fullWidth
              required
              value={values.scheduleExpression}
              onChange={handleChange('scheduleExpression')}
              error={!!errors.scheduleExpression}
              helperText={errors.scheduleExpression ?? 'Use standard cron format (min hour dom mon dow).'}
            />

            <TextField
              label="Timezone"
              fullWidth
              value={values.timezone}
              onChange={handleChange('timezone')}
              helperText="IANA timezone, default UTC"
            />

            <FormControl fullWidth>
              <InputLabel id="ingestion-strategy-label">Load Strategy</InputLabel>
              <Select
                labelId="ingestion-strategy-label"
                label="Load Strategy"
                value={values.loadStrategy}
                onChange={handleStrategyChange}
              >
                <MenuItem value="timestamp">Timestamp (watermark)</MenuItem>
                <MenuItem value="numeric_key">Numeric key</MenuItem>
                <MenuItem value="full">Full reload</MenuItem>
              </Select>
            </FormControl>

            <TextField
              label="Watermark Column"
              fullWidth
              value={values.watermarkColumn ?? ''}
              onChange={handleChange('watermarkColumn')}
              error={!!errors.watermarkColumn}
              helperText={
                errors.watermarkColumn ??
                (values.loadStrategy === 'timestamp'
                  ? 'Column used to detect new rows (e.g. updated_at).'
                  : 'Optional when not using timestamp strategy.')
              }
            />

            <TextField
              label="Primary Key Column"
              fullWidth
              value={values.primaryKeyColumn ?? ''}
              onChange={handleChange('primaryKeyColumn')}
              error={!!errors.primaryKeyColumn}
              helperText={
                errors.primaryKeyColumn ??
                (values.loadStrategy === 'numeric_key'
                  ? 'Numeric identifier column used to find new rows.'
                  : 'Optional when not using numeric key strategy.')
              }
            />

            <TextField
              label="Target Schema Override"
              fullWidth
              value={values.targetSchema ?? ''}
              onChange={handleChange('targetSchema')}
              helperText="Optional override; defaults to source schema or dbo."
            />

            <TextField
              label="Batch Size"
              type="number"
              fullWidth
              value={values.batchSize}
              onChange={handleChange('batchSize')}
              error={!!errors.batchSize}
              helperText={errors.batchSize ?? 'Rows per batch when loading (default 5000).'}
            />

            <FormControlLabel
              control={<Switch checked={values.isActive} onChange={handleToggleActive} color="secondary" />}
              label={values.isActive ? 'Schedule is active' : 'Schedule is paused'}
            />
          </Stack>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'space-between' }}>
          <Typography variant="caption" color="text.disabled" sx={{ ml: 1.5 }}>
            Cron examples: `0 * * * *` (hourly), `0 2 * * *` (daily at 2am)
          </Typography>
          <Box display="flex" gap={1} alignItems="center">
            <LoadingButton onClick={resetAndClose} disabled={loading}>
              Cancel
            </LoadingButton>
            <LoadingButton type="submit" variant="contained" loading={loading}>
              Save Schedule
            </LoadingButton>
          </Box>
        </DialogActions>
      </Box>
    </Dialog>
  );
};

export default IngestionScheduleForm;
