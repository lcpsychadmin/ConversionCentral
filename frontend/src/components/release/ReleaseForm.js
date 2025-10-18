import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, MenuItem, Stack, TextField } from '@mui/material';
const STATUS_OPTIONS = ['planned', 'in_progress', 'ready', 'active', 'completed', 'archived'];
const ReleaseForm = ({ open, title, initialValues, projectOptions, loading = false, onClose, onSubmit }) => {
    const initialSnapshot = useMemo(() => {
        const defaultProjectId = initialValues?.projectId ?? projectOptions[0]?.id ?? '';
        return {
            projectId: defaultProjectId,
            name: initialValues?.name ?? '',
            description: initialValues?.description ?? '',
            status: initialValues?.status ?? 'planned'
        };
    }, [initialValues, projectOptions]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const next = event.target.value;
        setValues((prev) => ({ ...prev, [field]: next }));
        setErrors((prev) => ({ ...prev, [field]: undefined }));
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
        if (!values.projectId) {
            nextErrors.projectId = 'Select a project';
        }
        if (!trimmedName) {
            nextErrors.name = 'Name is required';
        }
        if (Object.keys(nextErrors).length > 0) {
            setErrors(nextErrors);
            return;
        }
        onSubmit({
            projectId: values.projectId,
            name: trimmedName,
            description: trimmedDescription || null,
            status: values.status
        });
    };
    const isDirty = useMemo(() => {
        return (values.projectId !== initialSnapshot.projectId ||
            values.name !== initialSnapshot.name ||
            (values.description ?? '') !== (initialSnapshot.description ?? '') ||
            values.status !== initialSnapshot.status);
    }, [values, initialSnapshot]);
    return (_jsx(Dialog, { open: open, onClose: handleClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [_jsx(TextField, { select: true, label: "Project", fullWidth: true, value: values.projectId, onChange: handleChange('projectId'), error: !!errors.projectId, helperText: errors.projectId, children: projectOptions.map((project) => (_jsx(MenuItem, { value: project.id, children: project.name }, project.id))) }), _jsx(TextField, { label: "Name", required: true, fullWidth: true, value: values.name, onChange: handleChange('name'), error: !!errors.name, helperText: errors.name }), _jsx(TextField, { label: "Description", fullWidth: true, multiline: true, minRows: 3, value: values.description ?? '', onChange: handleChange('description') }), _jsx(TextField, { select: true, label: "Status", fullWidth: true, value: values.status, onChange: handleChange('status'), children: STATUS_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option))) })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || (!isDirty && !initialValues), children: "Save" })] })] }) }));
};
export default ReleaseForm;
