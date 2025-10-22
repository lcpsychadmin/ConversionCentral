import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Alert, Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControlLabel, MenuItem, Stack, Switch, TextField } from '@mui/material';
import { buildJdbcConnectionString, parseJdbcConnectionString } from '../../utils/connectionString';
const DATABASE_OPTIONS = ['postgresql'];
const sanitizeNotes = (notes) => notes ?? '';
const buildInitialSnapshot = (systems, initialValues) => {
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
const validateValues = (values) => {
    const errors = {};
    if (!values.systemId) {
        errors.systemId = 'System is required';
    }
    if (!values.host.trim()) {
        errors.host = 'Host is required';
    }
    if (!values.port.trim()) {
        errors.port = 'Port is required';
    }
    else if (!/^\d+$/.test(values.port)) {
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
const normalizeValues = (values) => ({
    ...values,
    host: values.host.trim(),
    port: values.port.trim(),
    database: values.database.trim(),
    username: values.username.trim(),
    password: values.password,
    notes: values.notes?.trim() ? values.notes.trim() : null
});
const SystemConnectionForm = ({ open, title, systems, initialValues, loading = false, testing = false, onClose, onSubmit, onTest }) => {
    const initialSnapshot = useMemo(() => buildInitialSnapshot(systems, initialValues), [systems, initialValues]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const value = event.target.value;
        setValues((prev) => ({ ...prev, [field]: value }));
        setErrors((prev) => ({ ...prev, [field]: undefined, form: undefined }));
    };
    const handleToggleActive = (_event, checked) => {
        setValues((prev) => ({ ...prev, active: checked }));
    };
    const handleToggleIngestion = (_event, checked) => {
        setValues((prev) => ({ ...prev, ingestionEnabled: checked }));
    };
    const resetAndClose = () => {
        setValues(initialSnapshot);
        setErrors({});
        onClose();
    };
    const handleSubmit = (event) => {
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
        }
        catch (error) {
            const message = error instanceof Error ? error.message : 'Unable to build connection string.';
            setErrors((prev) => ({ ...prev, form: message }));
        }
    };
    const handleTest = () => {
        if (!onTest)
            return;
        const validationErrors = validateValues(values);
        if (Object.keys(validationErrors).length > 0) {
            setErrors(validationErrors);
            return;
        }
        try {
            const normalized = normalizeValues(values);
            const connectionString = buildJdbcConnectionString(normalized);
            onTest(normalized, connectionString);
        }
        catch (error) {
            const message = error instanceof Error ? error.message : 'Unable to build connection string.';
            setErrors((prev) => ({ ...prev, form: message }));
        }
    };
    const isDirty = useMemo(() => {
        return (values.systemId !== initialSnapshot.systemId ||
            values.databaseType !== initialSnapshot.databaseType ||
            values.host !== initialSnapshot.host ||
            values.port !== initialSnapshot.port ||
            values.database !== initialSnapshot.database ||
            values.username !== initialSnapshot.username ||
            values.password !== initialSnapshot.password ||
            sanitizeNotes(values.notes) !== sanitizeNotes(initialSnapshot.notes) ||
            values.active !== initialSnapshot.active ||
            values.ingestionEnabled !== initialSnapshot.ingestionEnabled);
    }, [values, initialSnapshot]);
    const canTest = Boolean(values.systemId &&
        values.host.trim() &&
        values.port.trim() &&
        /^\d+$/.test(values.port) &&
        values.database.trim() &&
        values.username.trim() &&
        values.password.trim());
    return (_jsx(Dialog, { open: open, onClose: resetAndClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [errors.form && _jsx(Alert, { severity: "error", children: errors.form }), _jsxs(TextField, { select: true, required: true, label: "System", fullWidth: true, id: "connection-system", name: "systemId", value: values.systemId, onChange: handleChange('systemId'), error: !!errors.systemId, helperText: errors.systemId, children: [_jsx(MenuItem, { value: "", disabled: true, children: "Select a system" }), systems.map((system) => (_jsx(MenuItem, { value: system.id, children: system.name }, system.id)))] }), _jsx(TextField, { select: true, label: "Database Type", fullWidth: true, id: "connection-database-type", name: "databaseType", value: values.databaseType, onChange: handleChange('databaseType'), helperText: "Relational engines supported today", children: DATABASE_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option === 'postgresql' ? 'PostgreSQL' : option }, option))) }), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 2, children: [_jsx(TextField, { label: "Host", fullWidth: true, required: true, id: "connection-host", name: "host", value: values.host, onChange: handleChange('host'), error: !!errors.host, helperText: errors.host ?? 'Hostname or IP address' }), _jsx(TextField, { label: "Port", fullWidth: true, required: true, id: "connection-port", name: "port", value: values.port, onChange: handleChange('port'), error: !!errors.port, helperText: errors.port ?? 'Default 5432' })] }), _jsx(TextField, { label: "Database", fullWidth: true, required: true, id: "connection-database", name: "database", value: values.database, onChange: handleChange('database'), error: !!errors.database, helperText: errors.database }), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 2, children: [_jsx(TextField, { label: "Username", fullWidth: true, required: true, id: "connection-username", name: "username", value: values.username, onChange: handleChange('username'), error: !!errors.username, helperText: errors.username }), _jsx(TextField, { label: "Password", fullWidth: true, required: true, id: "connection-password", name: "password", type: "password", value: values.password, onChange: handleChange('password'), error: !!errors.password, helperText: errors.password })] }), _jsx(TextField, { label: "Notes", fullWidth: true, multiline: true, minRows: 2, id: "connection-notes", name: "notes", value: values.notes ?? '', onChange: handleChange('notes') }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.active, onChange: handleToggleActive }), label: values.active ? 'Connection is active' : 'Connection is disabled' }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.ingestionEnabled, onChange: handleToggleIngestion, color: "secondary" }), label: values.ingestionEnabled
                                    ? 'Ingestion features enabled'
                                    : 'Hide ingestion features for this connection' })] }) }), _jsxs(DialogActions, { sx: { justifyContent: 'space-between' }, children: [_jsx(Button, { onClick: resetAndClose, disabled: loading || testing, children: "Cancel" }), _jsxs(Box, { display: "flex", gap: 1, children: [onTest && (_jsx(LoadingButton, { onClick: handleTest, loading: testing, disabled: !canTest || loading, children: "Test Connection" })), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || (!!initialValues && !isDirty), children: "Save Connection" })] })] })] }) }));
};
export default SystemConnectionForm;
