import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, MenuItem, Stack, TextField } from '@mui/material';
const STATUS_OPTIONS = ['planned', 'active', 'on_hold', 'completed', 'archived'];
const sanitize = (value) => value ?? '';
const ProjectForm = ({ open, title, initialValues, loading = false, onClose, onSubmit }) => {
    const initialSnapshot = useMemo(() => ({
        name: sanitize(initialValues?.name),
        description: sanitize(initialValues?.description),
        status: initialValues?.status ?? 'planned'
    }), [initialValues]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const next = event.target.value;
        setValues((prev) => ({ ...prev, [field]: next }));
        if (field === 'name') {
            setErrors((prev) => ({ ...prev, name: undefined }));
        }
    };
    const handleClose = () => {
        setValues(initialSnapshot);
        setErrors({});
        onClose();
    };
    const handleSubmit = (event) => {
        event.preventDefault();
        const trimmedName = values.name.trim();
        const trimmedDescription = values.description?.trim() ?? '';
        const nextErrors = {};
        if (!trimmedName) {
            nextErrors.name = 'Name is required';
        }
        if (Object.keys(nextErrors).length > 0) {
            setErrors(nextErrors);
            return;
        }
        onSubmit({
            name: trimmedName,
            description: trimmedDescription || null,
            status: values.status
        });
    };
    const isDirty = useMemo(() => {
        return (values.name !== initialSnapshot.name ||
            (values.description ?? '') !== (initialSnapshot.description ?? '') ||
            values.status !== initialSnapshot.status);
    }, [values, initialSnapshot]);
    return (_jsx(Dialog, { open: open, onClose: handleClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [_jsx(TextField, { label: "Name", required: true, fullWidth: true, value: values.name, onChange: handleChange('name'), error: !!errors.name, helperText: errors.name }), _jsx(TextField, { label: "Description", fullWidth: true, multiline: true, minRows: 3, value: values.description ?? '', onChange: handleChange('description') }), _jsx(TextField, { select: true, label: "Status", fullWidth: true, value: values.status, onChange: handleChange('status'), children: STATUS_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option))) })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || (!isDirty && !initialValues), children: "Save" })] })] }) }));
};
export default ProjectForm;
