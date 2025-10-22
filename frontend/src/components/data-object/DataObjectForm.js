import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Alert, Autocomplete, Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, MenuItem, Stack, TextField } from '@mui/material';
const DEFAULT_STATUS_OPTIONS = ['draft', 'active', 'archived'];
const sanitize = (value) => (value ?? '');
const arraysEqual = (a, b) => {
    if (a.length !== b.length)
        return false;
    const sortedA = [...a].sort();
    const sortedB = [...b].sort();
    return sortedA.every((value, index) => value === sortedB[index]);
};
const DataObjectForm = ({ open, title, initialValues, loading = false, onClose, onSubmit, processAreas, systems }) => {
    const initialSnapshot = useMemo(() => {
        const defaultProcessArea = processAreas[0]?.id ?? '';
        return {
            name: sanitize(initialValues?.name),
            description: sanitize(initialValues?.description),
            status: initialValues?.status ?? 'draft',
            processAreaId: initialValues?.processAreaId ?? defaultProcessArea,
            systemIds: initialValues?.systems?.map((system) => system.id) ?? []
        };
    }, [initialValues, processAreas]);
    const [values, setValues] = useState(initialSnapshot);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        setValues(initialSnapshot);
        setErrors({});
    }, [initialSnapshot, open]);
    const handleChange = (field) => (event) => {
        const value = event.target.value;
        setValues((prev) => ({ ...prev, [field]: value }));
        setErrors((prev) => ({ ...prev, [field]: undefined }));
    };
    const handleSystemsChange = (_, selected) => {
        setValues((prev) => ({ ...prev, systemIds: selected.map((system) => system.id) }));
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
        if (!values.processAreaId) {
            nextErrors.processAreaId = 'Process area is required';
        }
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
            status: values.status,
            processAreaId: values.processAreaId,
            systemIds: values.systemIds
        });
    };
    const isDirty = useMemo(() => {
        return (values.name !== initialSnapshot.name ||
            (values.description ?? '') !== (initialSnapshot.description ?? '') ||
            values.status !== initialSnapshot.status ||
            values.processAreaId !== initialSnapshot.processAreaId ||
            !arraysEqual(values.systemIds, initialSnapshot.systemIds));
    }, [values, initialSnapshot]);
    const noProcessAreas = processAreas.length === 0;
    return (_jsx(Dialog, { open: open, onClose: handleClose, fullWidth: true, maxWidth: "sm", children: _jsxs(Box, { component: "form", noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: title }), _jsx(DialogContent, { children: _jsxs(Stack, { spacing: 2, mt: 1, children: [noProcessAreas && (_jsx(Alert, { severity: "warning", children: "Create a process area before adding data objects. Once available, you can assign the data object to it here." })), _jsx(TextField, { label: "Name", fullWidth: true, required: true, id: "data-object-name", name: "name", value: values.name, onChange: handleChange('name'), error: !!errors.name, helperText: errors.name }), _jsx(TextField, { label: "Description", fullWidth: true, multiline: true, minRows: 2, id: "data-object-description", name: "description", value: values.description ?? '', onChange: handleChange('description') }), _jsx(TextField, { select: true, label: "Process Area", fullWidth: true, required: true, id: "data-object-process-area", name: "processAreaId", value: values.processAreaId ?? '', onChange: handleChange('processAreaId'), error: !!errors.processAreaId, helperText: errors.processAreaId ?? 'Select the process area that owns this data object.', disabled: noProcessAreas, children: processAreas.map((area) => (_jsx(MenuItem, { value: area.id, children: area.name }, area.id))) }), _jsx(TextField, { select: true, label: "Status", fullWidth: true, id: "data-object-status", name: "status", value: values.status, onChange: handleChange('status'), children: DEFAULT_STATUS_OPTIONS.map((option) => (_jsx(MenuItem, { value: option, children: option }, option))) }), _jsx(Autocomplete, { multiple: true, options: systems, disableCloseOnSelect: true, getOptionLabel: (option) => option.name, isOptionEqualToValue: (option, value) => option.id === value?.id, value: systems.filter((system) => values.systemIds.includes(system.id)), onChange: handleSystemsChange, renderInput: (params) => (_jsx(TextField, { ...params, label: "Systems", placeholder: systems.length === 0 ? 'No systems available' : 'Select systems' })) })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || !isDirty, children: "Save" })] })] }) }));
};
export default DataObjectForm;
