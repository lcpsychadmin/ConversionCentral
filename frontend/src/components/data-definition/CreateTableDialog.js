import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Button, Dialog, DialogActions, DialogContent, DialogTitle, MenuItem, Stack, TextField } from '@mui/material';
const STATUS_OPTIONS = ['active', 'draft', 'deprecated'];
const TABLE_TYPE_OPTIONS = ['BASE', 'VIEW', 'REFERENCE'];
const defaultValues = {
    name: '',
    physicalName: '',
    schemaName: '',
    description: '',
    tableType: '',
    status: 'active'
};
const CreateTableDialog = ({ open, loading = false, mode = 'create', initialValues, onClose, onSubmit }) => {
    const baseValues = useMemo(() => initialValues
        ? {
            name: initialValues.name ?? '',
            physicalName: initialValues.physicalName ?? '',
            schemaName: initialValues.schemaName ?? '',
            description: initialValues.description ?? '',
            tableType: initialValues.tableType ?? '',
            status: initialValues.status ?? 'active'
        }
        : defaultValues, [initialValues]);
    const [values, setValues] = useState(baseValues);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        if (!open)
            return;
        setValues(baseValues);
        setErrors({});
    }, [open, baseValues]);
    const isDirty = useMemo(() => {
        const keys = Object.keys(values);
        return keys.some((key) => values[key] !== baseValues[key]);
    }, [values, baseValues]);
    const handleChange = (field) => (event) => {
        const value = event.target.value;
        setValues((prev) => ({ ...prev, [field]: value }));
        setErrors((prev) => ({ ...prev, [field]: undefined }));
    };
    const handleClose = () => {
        if (loading)
            return;
        setValues(baseValues);
        setErrors({});
        onClose();
    };
    const handleSubmit = async (event) => {
        event.preventDefault();
        const nextErrors = {};
        if (!values.name.trim()) {
            nextErrors.name = 'Table name is required.';
        }
        if (!values.physicalName.trim()) {
            nextErrors.physicalName = 'Physical name is required.';
        }
        if (Object.keys(nextErrors).length > 0) {
            setErrors(nextErrors);
            return;
        }
        await onSubmit({
            name: values.name.trim(),
            physicalName: values.physicalName.trim(),
            schemaName: values.schemaName.trim(),
            description: values.description.trim(),
            tableType: values.tableType.trim(),
            status: values.status.trim()
        });
    };
    return (_jsx(Dialog, { open: open, onClose: handleClose, maxWidth: "sm", fullWidth: true, children: _jsxs("form", { noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: mode === 'create' ? 'Create Table' : 'Edit Table' }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, mt: 1, children: [_jsx(TextField, { label: "Table Name", value: values.name, onChange: handleChange('name'), required: true, error: !!errors.name, helperText: errors.name, fullWidth: true }), _jsx(TextField, { label: "Physical Name", value: values.physicalName, onChange: handleChange('physicalName'), required: true, error: !!errors.physicalName, helperText: errors.physicalName, fullWidth: true }), _jsx(TextField, { label: "Schema", value: values.schemaName, onChange: handleChange('schemaName'), fullWidth: true }), _jsx(TextField, { label: "Description", value: values.description, onChange: handleChange('description'), multiline: true, minRows: 2, fullWidth: true }), _jsxs(TextField, { select: true, label: "Table Type", value: values.tableType, onChange: handleChange('tableType'), fullWidth: true, helperText: "Optional classification (e.g., base, view)", children: [_jsx(MenuItem, { value: "", children: _jsx("em", { children: "None" }) }), TABLE_TYPE_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option)))] }), _jsx(TextField, { select: true, label: "Status", value: values.status, onChange: handleChange('status'), fullWidth: true, children: STATUS_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option))) })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || !isDirty, children: mode === 'create' ? 'Create Table' : 'Save Changes' })] })] }) }));
};
export default CreateTableDialog;
