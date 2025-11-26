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
import Checkbox from '@mui/material/Checkbox';
import ListItemText from '@mui/material/ListItemText';

import { DataWarehouseTarget, IngestionLoadStrategy, IngestionSchedule } from '../../types/data';

export interface ScheduleSelectionOption {
  id: string;
  label: string;
  schemaName: string;
  tableName: string;
  targetPreview: string;
  disabled?: boolean;
}

export interface WarehouseOption {
  value: DataWarehouseTarget;
  label: string;
  disabled?: boolean;
  helperText?: string;
}

export interface SapHanaOption {
  id: string;
  label: string;
  disabled?: boolean;
}

interface IngestionScheduleFormProps {
  open: boolean;
  title: string;
  options: ScheduleSelectionOption[];
  warehouseOptions: WarehouseOption[];
  sapHanaOptions: SapHanaOption[];
  initialValues?: IngestionSchedule | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: FormValues) => void;
  disableSelectionChange?: boolean;
  allowMultiSelect?: boolean;
}

export interface FormValues {
  connectionTableSelectionIds: string[];
  scheduleExpression: string;
  timezone: string;
  loadStrategy: IngestionLoadStrategy;
  watermarkColumn?: string;
  primaryKeyColumn?: string;
  targetSchema?: string;
  targetWarehouse: DataWarehouseTarget;
  sapHanaSettingId?: string;
  batchSize: number;
  isActive: boolean;
}

type FieldErrorMap = Partial<Record<keyof FormValues | 'form' | 'connectionTableSelectionId', string>>;

const DEFAULT_VALUES: FormValues = {
  connectionTableSelectionIds: [],
  scheduleExpression: '0 2 * * *',
  timezone: 'UTC',
  loadStrategy: 'timestamp',
  watermarkColumn: '',
  primaryKeyColumn: '',
  targetSchema: '',
  targetWarehouse: 'databricks_sql',
  sapHanaSettingId: '',
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
      connectionTableSelectionIds: firstOption ? [firstOption.id] : []
    };
  }
  return {
    connectionTableSelectionIds: [initial.connectionTableSelectionId],
    scheduleExpression: initial.scheduleExpression,
    timezone: initial.timezone ?? 'UTC',
    loadStrategy: initial.loadStrategy,
    watermarkColumn: initial.watermarkColumn ?? '',
    primaryKeyColumn: initial.primaryKeyColumn ?? '',
    targetSchema: initial.targetSchema ?? '',
    targetWarehouse: initial.targetWarehouse,
    sapHanaSettingId: initial.sapHanaSettingId ?? '',
    batchSize: initial.batchSize,
    isActive: initial.isActive
  };
};

const validate = (values: FormValues): FieldErrorMap => {
  const errors: FieldErrorMap = {};

  if (!values.connectionTableSelectionIds || values.connectionTableSelectionIds.length === 0) {
    errors.connectionTableSelectionId = 'Select at least one table to ingest.';
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

  // SAP HANA is no longer a warehouse target; no additional validation required here.

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
  sapHanaSettingId: values.sapHanaSettingId?.trim() || '',
  batchSize: Number(values.batchSize),
  connectionTableSelectionIds: values.connectionTableSelectionIds
});

const IngestionScheduleForm = ({
  open,
  title,
  options,
  warehouseOptions,
  sapHanaOptions,
  initialValues = null,
  loading = false,
  onClose,
  onSubmit,
  disableSelectionChange = false,
  allowMultiSelect = false
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

  useEffect(() => {
    // SAP HANA is no longer a warehouse target; leave sapHanaSetting untouched by warehouse selection.
  }, [sapHanaOptions, values.targetWarehouse, values.sapHanaSettingId]);

  const multiSelectEnabled = allowMultiSelect && !disableSelectionChange;

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

  const handleWarehouseChange = (event: SelectChangeEvent<DataWarehouseTarget>) => {
    const next = event.target.value as DataWarehouseTarget;
    setValues((prev) => ({ ...prev, targetWarehouse: next }));
    setErrors((prev) => ({ ...prev, sapHanaSettingId: undefined }));
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

  const selectedOptions = options.filter((option) => values.connectionTableSelectionIds.includes(option.id));
  const targetPreview = !multiSelectEnabled && selectedOptions.length === 1 ? selectedOptions[0]?.targetPreview : undefined;
  return (
    <Dialog open={open} onClose={resetAndClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {errors.form && <Alert severity="error">{errors.form}</Alert>}
            <FormControl fullWidth required error={!!errors.connectionTableSelectionId}>
              <InputLabel id="ingestion-table-label">
                {multiSelectEnabled ? 'Table Selections' : 'Table Selection'}
              </InputLabel>
              <Select
                labelId="ingestion-table-label"
                label={multiSelectEnabled ? 'Table Selections' : 'Table Selection'}
                multiple={multiSelectEnabled}
                value={multiSelectEnabled ? values.connectionTableSelectionIds : values.connectionTableSelectionIds[0] ?? ''}
                disabled={disableSelectionChange}
                onChange={(event: SelectChangeEvent<string | string[]>) => {
                  const rawValue = event.target.value;
                  const normalizedArray = Array.isArray(rawValue) ? rawValue : [rawValue];
                  const nextIds = multiSelectEnabled ? normalizedArray : [normalizedArray[0] ?? ''];
                  setValues((prev) => ({
                    ...prev,
                    connectionTableSelectionIds: nextIds.filter((item) => item)
                  }));
                  setErrors((prev) => ({ ...prev, connectionTableSelectionId: undefined }));
                }}
                renderValue={(selected) => {
                  if (!multiSelectEnabled) {
                    const option = options.find((opt) => opt.id === selected);
                    return option ? option.label : 'Select table';
                  }
                  const selections = Array.isArray(selected) ? selected : [selected];
                  if (selections.length === 0) {
                    return 'Select tables';
                  }
                  if (selections.length === 1) {
                    const option = options.find((opt) => opt.id === selections[0]);
                    return option ? option.label : '1 table selected';
                  }
                  return `${selections.length} tables selected`;
                }}
              >
                {options.map((option) => (
                  <MenuItem key={option.id} value={option.id} disabled={option.disabled}>
                    {multiSelectEnabled && !option.disabled && (
                      <Checkbox checked={values.connectionTableSelectionIds.includes(option.id)} />
                    )}
                    <ListItemText
                      primary={option.disabled ? `${option.label} (not selected)` : option.label}
                      secondary={multiSelectEnabled ? option.targetPreview : undefined}
                    />
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText>{errors.connectionTableSelectionId}</FormHelperText>
            </FormControl>

            {multiSelectEnabled && selectedOptions.length > 1 && (
              <Typography variant="body2" color="text.secondary">
                Target tables will be created for each selected source table using the same schedule settings.
              </Typography>
            )}

            {!multiSelectEnabled && targetPreview && (
              <Typography variant="body2" color="text.secondary">
                Target table will be <strong>{targetPreview}</strong>
              </Typography>
            )}

            <FormControl fullWidth required>
              <InputLabel id="target-warehouse-label">Target Warehouse</InputLabel>
              <Select
                labelId="target-warehouse-label"
                label="Target Warehouse"
                value={values.targetWarehouse}
                onChange={handleWarehouseChange}
              >
                {warehouseOptions.map((option) => (
                  <MenuItem key={option.value} value={option.value} disabled={option.disabled}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText>
                {
                  warehouseOptions.find((option) => option.value === values.targetWarehouse)?.helperText ??
                  'Choose where the ingested data should land.'
                }
              </FormHelperText>
            </FormControl>

            {/* SAP HANA is no longer displayed as a target warehouse option. */}

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
