import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useState } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, Stack, TextField, Box, Typography, Alert } from '@mui/material';
import { useToast } from '../../hooks/useToast';
const AddRowDialog = ({ open, fields, onAdd, onClose }) => {
    const [formData, setFormData] = useState({});
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [validationErrors, setValidationErrors] = useState({});
    const toast = useToast();
    const handleChange = useCallback((fieldKey, value) => {
        setFormData((prev) => ({
            ...prev,
            [fieldKey]: value
        }));
        // Clear error for this field
        if (validationErrors[fieldKey]) {
            setValidationErrors((prev) => {
                const newErrors = { ...prev };
                delete newErrors[fieldKey];
                return newErrors;
            });
        }
    }, [validationErrors]);
    const handleSubmit = async () => {
        // Validate required fields
        const errors = {};
        for (const field of fields) {
            const value = formData[field.name];
            if (!field.isNullable && (value === undefined || value === null || value === '')) {
                errors[field.name] = `${field.name} is required`;
            }
        }
        if (Object.keys(errors).length > 0) {
            setValidationErrors(errors);
            return;
        }
        setIsSubmitting(true);
        try {
            await onAdd(formData);
            toast.showSuccess('Row added successfully');
            setFormData({});
            setValidationErrors({});
            onClose();
        }
        catch (error) {
            const message = error instanceof Error ? error.message : 'Failed to add row';
            toast.showError(message);
        }
        finally {
            setIsSubmitting(false);
        }
    };
    const handleClose = () => {
        if (!isSubmitting) {
            setFormData({});
            setValidationErrors({});
            onClose();
        }
    };
    return (_jsxs(Dialog, { open: open, onClose: handleClose, maxWidth: "sm", fullWidth: true, children: [_jsx(DialogTitle, { children: "Add New Row" }), _jsx(DialogContent, { sx: { pt: 2 }, children: _jsx(Stack, { spacing: 2, children: fields.length === 0 ? (_jsx(Alert, { severity: "info", children: "No fields defined for this table yet." })) : (fields.map((field) => (_jsxs(Box, { children: [_jsx(TextField, { fullWidth: true, label: field.name, type: field.dataType?.toLowerCase() === 'integer' ? 'number' : 'text', value: formData[field.name] ?? '', onChange: (e) => handleChange(field.name, e.target.value), disabled: isSubmitting, inputProps: {
                                    step: field.dataType?.toLowerCase() === 'integer' ? '1' : undefined,
                                    min: field.dataType?.toLowerCase() === 'integer' ? '0' : undefined
                                }, error: !!validationErrors[field.name] }), validationErrors[field.name] && (_jsx(Typography, { variant: "caption", color: "error", sx: { mt: 0.5, display: 'block' }, children: validationErrors[field.name] })), !field.isNullable && (_jsx(Typography, { variant: "caption", color: "textSecondary", sx: { display: 'block', mt: 0.5 }, children: "Required field" }))] }, field.id)))) }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: isSubmitting, children: "Cancel" }), _jsx(Button, { onClick: handleSubmit, variant: "contained", disabled: isSubmitting || fields.length === 0, children: isSubmitting ? 'Adding...' : 'Add Row' })] })] }));
};
export default AddRowDialog;
