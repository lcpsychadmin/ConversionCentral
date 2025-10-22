import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Alert, Box, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormControlLabel, FormHelperText, InputLabel, MenuItem, Stack, Switch, TextField, Typography } from '@mui/material';
import Select from '@mui/material/Select';
const DEFAULT_VALUES = {
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
const buildInitialValues = (initial, options) => {
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
const validate = (values) => {
    const errors = {};
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
const trimString = (value) => (value ? value.trim() : undefined);
const normalize = (values) => ({
    ...values,
    scheduleExpression: values.scheduleExpression.trim(),
    timezone: values.timezone.trim() || 'UTC',
    watermarkColumn: trimString(values.watermarkColumn),
    primaryKeyColumn: trimString(values.primaryKeyColumn),
    targetSchema: trimString(values.targetSchema),
    batchSize: Number(values.batchSize),
    connectionTableSelectionId: values.connectionTableSelectionId
});
const IngestionScheduleForm = ({ open, title, options, initialValues = null, loading = false, onClose, onSubmit, disableSelectionChange = false }) => {
    const initialSnapshot = useMemo(() => buildInitialValues(initialValues, options), [initialValues, options]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const next = event.target.value;
        setValues((prev) => ({ ...prev, [field]: field === 'batchSize' ? Number(next) || 0 : next }));
        setErrors((prev) => ({ ...prev, [field]: undefined, form: undefined }));
    };
    const handleStrategyChange = (event) => {
        const next = event.target.value;
        setValues((prev) => ({ ...prev, loadStrategy: next }));
        setErrors((prev) => ({ ...prev, watermarkColumn: undefined, primaryKeyColumn: undefined }));
    };
    const handleToggleActive = (_event, checked) => {
        setValues((prev) => ({ ...prev, isActive: checked }));
    };
    const resetAndClose = () => {
        setValues(initialSnapshot);
        setErrors({});
        onClose();
    };
    const handleSubmit = (event) => {
        event.preventDefault();
        const validationErrors = validate(values);
        if (Object.keys(validationErrors).length > 0) {
            setErrors(validationErrors);
            return;
        }
        try {
            const normalized = normalize(values);
            onSubmit(normalized);
        }
        catch (error) {
            const message = error instanceof Error ? error.message : 'Unable to prepare schedule payload.';
            setErrors((prev) => ({ ...prev, form: message }));
        }
    };
    const selectedOption = options.find((option) => option.id === values.connectionTableSelectionId);
    const targetPreview = selectedOption?.targetPreview;
    return (_jsx(Dialog, { open: open, onClose: resetAndClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [errors.form && _jsx(Alert, { severity: "error", children: errors.form }), _jsxs(FormControl, { fullWidth: true, required: true, error: !!errors.connectionTableSelectionId, children: [_jsx(InputLabel, { id: "ingestion-table-label", children: "Table Selection" }), _jsx(Select, { labelId: "ingestion-table-label", label: "Table Selection", value: values.connectionTableSelectionId, disabled: disableSelectionChange, onChange: (event) => {
                                            setValues((prev) => ({
                                                ...prev,
                                                connectionTableSelectionId: event.target.value
                                            }));
                                            setErrors((prev) => ({ ...prev, connectionTableSelectionId: undefined }));
                                        }, children: options.map((option) => (_jsx(MenuItem, { value: option.id, disabled: option.disabled, children: option.disabled ? `${option.label} (not selected)` : option.label }, option.id))) }), _jsx(FormHelperText, { children: errors.connectionTableSelectionId })] }), targetPreview && (_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Target table will be ", _jsx("strong", { children: targetPreview })] })), _jsx(TextField, { label: "Cron Expression", fullWidth: true, required: true, value: values.scheduleExpression, onChange: handleChange('scheduleExpression'), error: !!errors.scheduleExpression, helperText: errors.scheduleExpression ?? 'Use standard cron format (min hour dom mon dow).' }), _jsx(TextField, { label: "Timezone", fullWidth: true, value: values.timezone, onChange: handleChange('timezone'), helperText: "IANA timezone, default UTC" }), _jsxs(FormControl, { fullWidth: true, children: [_jsx(InputLabel, { id: "ingestion-strategy-label", children: "Load Strategy" }), _jsxs(Select, { labelId: "ingestion-strategy-label", label: "Load Strategy", value: values.loadStrategy, onChange: handleStrategyChange, children: [_jsx(MenuItem, { value: "timestamp", children: "Timestamp (watermark)" }), _jsx(MenuItem, { value: "numeric_key", children: "Numeric key" }), _jsx(MenuItem, { value: "full", children: "Full reload" })] })] }), _jsx(TextField, { label: "Watermark Column", fullWidth: true, value: values.watermarkColumn ?? '', onChange: handleChange('watermarkColumn'), error: !!errors.watermarkColumn, helperText: errors.watermarkColumn ??
                                    (values.loadStrategy === 'timestamp'
                                        ? 'Column used to detect new rows (e.g. updated_at).'
                                        : 'Optional when not using timestamp strategy.') }), _jsx(TextField, { label: "Primary Key Column", fullWidth: true, value: values.primaryKeyColumn ?? '', onChange: handleChange('primaryKeyColumn'), error: !!errors.primaryKeyColumn, helperText: errors.primaryKeyColumn ??
                                    (values.loadStrategy === 'numeric_key'
                                        ? 'Numeric identifier column used to find new rows.'
                                        : 'Optional when not using numeric key strategy.') }), _jsx(TextField, { label: "Target Schema Override", fullWidth: true, value: values.targetSchema ?? '', onChange: handleChange('targetSchema'), helperText: "Optional override; defaults to source schema or dbo." }), _jsx(TextField, { label: "Batch Size", type: "number", fullWidth: true, value: values.batchSize, onChange: handleChange('batchSize'), error: !!errors.batchSize, helperText: errors.batchSize ?? 'Rows per batch when loading (default 5000).' }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.isActive, onChange: handleToggleActive, color: "secondary" }), label: values.isActive ? 'Schedule is active' : 'Schedule is paused' })] }) }), _jsxs(DialogActions, { sx: { justifyContent: 'space-between' }, children: [_jsx(Typography, { variant: "caption", color: "text.disabled", sx: { ml: 1.5 }, children: "Cron examples: `0 * * * *` (hourly), `0 2 * * *` (daily at 2am)" }), _jsxs(Box, { display: "flex", gap: 1, alignItems: "center", children: [_jsx(LoadingButton, { onClick: resetAndClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, children: "Save Schedule" })] })] })] }) }));
};
export default IngestionScheduleForm;
