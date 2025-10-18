import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Autocomplete, Button, Dialog, DialogActions, DialogContent, DialogTitle, Stack, TextField, Typography } from '@mui/material';
const AddExistingFieldDialog = ({ open, tableName, fields, loading = false, onClose, onSubmit }) => {
    const [selectedField, setSelectedField] = useState(null);
    const [notes, setNotes] = useState('');
    const [error, setError] = useState(null);
    useEffect(() => {
        if (!open) {
            return;
        }
        setSelectedField(null);
        setNotes('');
        setError(null);
    }, [open, fields]);
    const fieldOptions = useMemo(() => fields.slice().sort((a, b) => a.name.localeCompare(b.name)), [fields]);
    const handleClose = () => {
        if (loading) {
            return;
        }
        onClose();
    };
    const handleSubmit = async () => {
        if (!selectedField) {
            setError('Select a field to add.');
            return;
        }
        setError(null);
        await onSubmit({ fieldId: selectedField.id, notes });
    };
    return (_jsxs(Dialog, { open: open, onClose: handleClose, maxWidth: "sm", fullWidth: true, children: [_jsx(DialogTitle, { children: "Add Field to Definition" }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, mt: 1, children: [_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Table: ", tableName] }), _jsx(Autocomplete, { options: fieldOptions, value: selectedField, onChange: (_, value) => {
                                setSelectedField(value);
                                setError(null);
                            }, getOptionLabel: (option) => `${option.name} (${option.fieldType})`, isOptionEqualToValue: (option, value) => option.id === value.id, renderInput: (params) => (_jsx(TextField, { ...params, label: "Field", placeholder: fields.length ? 'Select field' : 'No fields available', required: true, error: Boolean(error), helperText: error })), disabled: loading || !fields.length }), _jsx(TextField, { label: "Notes", value: notes, onChange: (event) => setNotes(event.target.value), multiline: true, minRows: 2, fullWidth: true, disabled: loading })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { onClick: handleSubmit, variant: "contained", loading: loading, disabled: loading || !fields.length, children: "Add Field" })] })] }));
};
export default AddExistingFieldDialog;
