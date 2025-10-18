import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, MenuItem, Stack, TextField } from '@mui/material';
const STATUS_OPTIONS = ['active', 'inactive', 'decommissioned'];
const sanitize = (value) => value ?? '';
const SystemForm = ({ open, title, initialValues, loading = false, onClose, onSubmit }) => {
    const initialSnapshot = useMemo(() => ({
        name: sanitize(initialValues?.name),
        physicalName: sanitize(initialValues?.physicalName),
        description: sanitize(initialValues?.description),
        systemType: sanitize(initialValues?.systemType),
        status: initialValues?.status ?? 'active',
        securityClassification: sanitize(initialValues?.securityClassification)
    }), [initialValues]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const value = event.target.value;
        setValues((prev) => ({ ...prev, [field]: value }));
        if (field === 'name' || field === 'physicalName') {
            setErrors((prev) => ({ ...prev, [field]: undefined }));
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
        const trimmedPhysicalName = values.physicalName.trim();
        const nextErrors = {};
        if (!trimmedName) {
            nextErrors.name = 'Name is required';
        }
        if (!trimmedPhysicalName) {
            nextErrors.physicalName = 'Physical name is required';
        }
        if (Object.keys(nextErrors).length > 0) {
            setErrors(nextErrors);
            return;
        }
        onSubmit({
            name: trimmedName,
            physicalName: trimmedPhysicalName,
            description: values.description?.trim() || null,
            systemType: values.systemType?.trim() || null,
            status: values.status,
            securityClassification: values.securityClassification?.trim() || null
        });
    };
    const isDirty = useMemo(() => {
        return (values.name !== initialSnapshot.name ||
            values.physicalName !== initialSnapshot.physicalName ||
            (values.description ?? '') !== (initialSnapshot.description ?? '') ||
            (values.systemType ?? '') !== (initialSnapshot.systemType ?? '') ||
            values.status !== initialSnapshot.status ||
            (values.securityClassification ?? '') !== (initialSnapshot.securityClassification ?? ''));
    }, [values, initialSnapshot]);
    return (_jsx(Dialog, { open: open, onClose: handleClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [_jsx(TextField, { label: "Name", fullWidth: true, required: true, id: "system-name", name: "name", value: values.name, onChange: handleChange('name'), error: !!errors.name, helperText: errors.name }), _jsx(TextField, { label: "Physical Name", fullWidth: true, required: true, id: "system-physical-name", name: "physicalName", value: values.physicalName, onChange: handleChange('physicalName'), error: !!errors.physicalName, helperText: errors.physicalName }), _jsx(TextField, { label: "Description", fullWidth: true, multiline: true, minRows: 2, id: "system-description", name: "description", value: values.description ?? '', onChange: handleChange('description') }), _jsx(TextField, { label: "System Type", fullWidth: true, id: "system-type", name: "systemType", value: values.systemType ?? '', onChange: handleChange('systemType') }), _jsx(TextField, { select: true, label: "Status", fullWidth: true, id: "system-status", name: "status", value: values.status, onChange: handleChange('status'), children: STATUS_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option))) }), _jsx(TextField, { label: "Security Classification", fullWidth: true, id: "system-security-class", name: "securityClassification", value: values.securityClassification ?? '', onChange: handleChange('securityClassification') })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || (!isDirty && !!initialValues), children: "Save" })] })] }) }));
};
export default SystemForm;
