import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useState } from 'react';
import { Box, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, Grid, IconButton, Stack, Switch, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Typography, FormControlLabel, Alert } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { createValidationRule, updateValidationRule, deleteValidationRule } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';
const getRuleTypeLabel = (ruleType) => {
    const labels = {
        required: 'Required',
        unique: 'Unique',
        range: 'Range',
        pattern: 'Pattern',
        custom: 'Custom Expression',
        cross_field: 'Cross-Field'
    };
    return labels[ruleType] || ruleType;
};
const ValidationRulesManager = ({ constructedTableId, fields, validationRules, onRulesChange }) => {
    const theme = useTheme();
    const toast = useToast();
    // State
    const [dialogOpen, setDialogOpen] = useState(false);
    const [editingRuleId, setEditingRuleId] = useState(null);
    const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
    const [deleteRuleId, setDeleteRuleId] = useState(null);
    const [isSaving, setIsSaving] = useState(false);
    const [formData, setFormData] = useState({
        name: '',
        description: '',
        ruleType: 'required',
        fieldId: null,
        configuration: {},
        errorMessage: 'Validation failed',
        isActive: true
    });
    // Handlers
    const handleOpenDialog = useCallback(() => {
        setFormData({
            name: '',
            description: '',
            ruleType: 'required',
            fieldId: null,
            configuration: {},
            errorMessage: 'Validation failed',
            isActive: true
        });
        setEditingRuleId(null);
        setDialogOpen(true);
    }, []);
    const handleCloseDialog = useCallback(() => {
        setDialogOpen(false);
    }, []);
    const handleSaveRule = useCallback(async () => {
        if (!formData.name.trim()) {
            toast.showError('Rule name is required');
            return;
        }
        setIsSaving(true);
        try {
            if (editingRuleId) {
                // Update existing rule
                await updateValidationRule(editingRuleId, formData);
                toast.showSuccess('Rule updated successfully');
            }
            else {
                // Create new rule
                await createValidationRule({
                    ...formData,
                    constructedTableId
                });
                toast.showSuccess('Rule created successfully');
            }
            setDialogOpen(false);
            onRulesChange();
        }
        catch (error) {
            toast.showError(error.message || 'Failed to save rule');
        }
        finally {
            setIsSaving(false);
        }
    }, [formData, editingRuleId, constructedTableId, toast, onRulesChange]);
    const handleDeleteClick = (ruleId) => {
        setDeleteRuleId(ruleId);
        setDeleteConfirmOpen(true);
    };
    const handleDeleteConfirm = async () => {
        if (!deleteRuleId)
            return;
        try {
            await deleteValidationRule(deleteRuleId);
            toast.showSuccess('Rule deleted successfully');
            onRulesChange();
        }
        catch (error) {
            toast.showError(error.message || 'Failed to delete rule');
        }
        finally {
            setDeleteConfirmOpen(false);
            setDeleteRuleId(null);
        }
    };
    const handleToggleActive = async (rule) => {
        try {
            await updateValidationRule(rule.id, {
                isActive: !rule.isActive
            });
            toast.showSuccess(rule.isActive ? 'Rule disabled' : 'Rule enabled');
            onRulesChange();
        }
        catch (error) {
            toast.showError(error.message || 'Failed to toggle rule');
        }
    };
    const renderConfigForm = () => {
        switch (formData.ruleType) {
            case 'required':
                return (_jsxs(TextField, { fullWidth: true, label: "Field", select: true, value: formData.fieldId || '', onChange: (e) => setFormData({ ...formData, configuration: { fieldName: e.target.value } }), SelectProps: {
                        native: true
                    }, children: [_jsx("option", { value: "", children: "Select field" }), fields.map((field) => (_jsx("option", { value: field.name, children: field.name }, field.id)))] }));
            case 'unique':
                return (_jsxs(TextField, { fullWidth: true, label: "Field", select: true, value: formData.fieldId || '', onChange: (e) => setFormData({ ...formData, configuration: { fieldName: e.target.value } }), SelectProps: {
                        native: true
                    }, children: [_jsx("option", { value: "", children: "Select field" }), fields.map((field) => (_jsx("option", { value: field.name, children: field.name }, field.id)))] }));
            case 'range':
                return (_jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 12, children: _jsxs(TextField, { fullWidth: true, label: "Field", select: true, value: formData.fieldId || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, fieldName: e.target.value }
                                }), SelectProps: {
                                    native: true
                                }, children: [_jsx("option", { value: "", children: "Select field" }), fields.map((field) => (_jsx("option", { value: field.name, children: field.name }, field.id)))] }) }), _jsx(Grid, { item: true, xs: 6, children: _jsx(TextField, { fullWidth: true, label: "Minimum", type: "number", value: formData.configuration.min || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, min: Number(e.target.value) }
                                }) }) }), _jsx(Grid, { item: true, xs: 6, children: _jsx(TextField, { fullWidth: true, label: "Maximum", type: "number", value: formData.configuration.max || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, max: Number(e.target.value) }
                                }) }) })] }));
            case 'pattern':
                return (_jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 12, children: _jsxs(TextField, { fullWidth: true, label: "Field", select: true, value: formData.fieldId || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, fieldName: e.target.value }
                                }), SelectProps: {
                                    native: true
                                }, children: [_jsx("option", { value: "", children: "Select field" }), fields.map((field) => (_jsx("option", { value: field.name, children: field.name }, field.id)))] }) }), _jsx(Grid, { item: true, xs: 12, children: _jsx(TextField, { fullWidth: true, label: "Regex Pattern", multiline: true, rows: 2, placeholder: "e.g., ^[0-9]{3}-[0-9]{3}-[0-9]{4}$", value: formData.configuration.pattern || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, pattern: e.target.value }
                                }) }) })] }));
            case 'custom':
                return (_jsx(TextField, { fullWidth: true, label: "Expression", multiline: true, rows: 3, placeholder: "e.g., Age > 18 AND Status == 'Active'", value: formData.configuration.expression || '', onChange: (e) => setFormData({
                        ...formData,
                        configuration: { expression: e.target.value }
                    }) }));
            case 'cross_field':
                return (_jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 12, children: _jsx(TextField, { fullWidth: true, label: "Fields (comma-separated)", placeholder: "e.g., StartDate, EndDate", value: (formData.configuration.fields || []).join(', '), onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: {
                                        ...formData.configuration,
                                        fields: e.target.value.split(',').map((f) => f.trim())
                                    }
                                }) }) }), _jsx(Grid, { item: true, xs: 12, children: _jsx(TextField, { fullWidth: true, label: "Rule", multiline: true, rows: 2, placeholder: "e.g., StartDate <= EndDate", value: formData.configuration.rule || '', onChange: (e) => setFormData({
                                    ...formData,
                                    configuration: { ...formData.configuration, rule: e.target.value }
                                }) }) })] }));
            default:
                return null;
        }
    };
    return (_jsxs(_Fragment, { children: [_jsxs(Box, { sx: { p: 2 }, children: [_jsxs(Box, { sx: { mb: 2, display: 'flex', justifyContent: 'space-between' }, children: [_jsxs(Typography, { variant: "h6", children: ["Validation Rules (", validationRules.length, ")"] }), _jsx(Button, { variant: "contained", startIcon: _jsx(AddIcon, {}), onClick: handleOpenDialog, children: "Create Rule" })] }), validationRules.length === 0 ? (_jsx(Alert, { severity: "info", children: "No validation rules defined yet. Click \"Create Rule\" to add validation rules." })) : (_jsx(TableContainer, { sx: { border: 1, borderColor: 'divider', borderRadius: 1 }, children: _jsxs(Table, { children: [_jsx(TableHead, { children: _jsxs(TableRow, { sx: { backgroundColor: alpha(theme.palette.primary.main, 0.1) }, children: [_jsx(TableCell, { sx: { fontWeight: 'bold' }, children: "Name" }), _jsx(TableCell, { sx: { fontWeight: 'bold' }, children: "Type" }), _jsx(TableCell, { sx: { fontWeight: 'bold' }, children: "Field" }), _jsx(TableCell, { sx: { fontWeight: 'bold' }, children: "Active" }), _jsx(TableCell, { sx: { fontWeight: 'bold', width: 100 }, children: "Actions" })] }) }), _jsx(TableBody, { children: validationRules.map((rule) => (_jsxs(TableRow, { children: [_jsx(TableCell, { children: _jsxs(Box, { children: [_jsx(Typography, { variant: "body2", sx: { fontWeight: 500 }, children: rule.name }), rule.description && (_jsx(Typography, { variant: "caption", color: "textSecondary", children: rule.description }))] }) }), _jsx(TableCell, { children: _jsx(Chip, { label: getRuleTypeLabel(rule.ruleType), size: "small", color: rule.isActive ? 'primary' : 'default', variant: rule.isActive ? 'filled' : 'outlined' }) }), _jsx(TableCell, { children: rule.fieldId ? fields.find((f) => f.id === rule.fieldId)?.name : 'N/A' }), _jsx(TableCell, { children: _jsx(Switch, { checked: rule.isActive, onChange: () => handleToggleActive(rule), size: "small" }) }), _jsx(TableCell, { children: _jsx(Stack, { direction: "row", spacing: 0.5, children: _jsx(IconButton, { size: "small", color: "error", onClick: () => handleDeleteClick(rule.id), children: _jsx(DeleteIcon, { fontSize: "small" }) }) }) })] }, rule.id))) })] }) }))] }), _jsxs(Dialog, { open: dialogOpen, onClose: handleCloseDialog, maxWidth: "sm", fullWidth: true, children: [_jsx(DialogTitle, { children: editingRuleId ? 'Edit Validation Rule' : 'Create Validation Rule' }), _jsx(DialogContent, { sx: { pt: 2 }, children: _jsxs(Stack, { spacing: 2, children: [_jsx(TextField, { fullWidth: true, label: "Rule Name", value: formData.name, onChange: (e) => setFormData({ ...formData, name: e.target.value }), disabled: isSaving }), _jsx(TextField, { fullWidth: true, label: "Description", multiline: true, rows: 2, value: formData.description, onChange: (e) => setFormData({ ...formData, description: e.target.value }), disabled: isSaving }), _jsxs(TextField, { fullWidth: true, label: "Rule Type", select: true, value: formData.ruleType, onChange: (e) => setFormData({
                                        ...formData,
                                        ruleType: e.target.value,
                                        configuration: {},
                                        fieldId: null
                                    }), SelectProps: {
                                        native: true
                                    }, disabled: isSaving, children: [_jsx("option", { value: "required", children: "Required" }), _jsx("option", { value: "unique", children: "Unique" }), _jsx("option", { value: "range", children: "Range" }), _jsx("option", { value: "pattern", children: "Pattern" }), _jsx("option", { value: "custom", children: "Custom Expression" }), _jsx("option", { value: "cross_field", children: "Cross-Field" })] }), renderConfigForm(), _jsx(TextField, { fullWidth: true, label: "Error Message", multiline: true, rows: 2, value: formData.errorMessage, onChange: (e) => setFormData({ ...formData, errorMessage: e.target.value }), disabled: isSaving }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: formData.isActive, onChange: (e) => setFormData({ ...formData, isActive: e.target.checked }), disabled: isSaving }), label: "Active" })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleCloseDialog, disabled: isSaving, children: "Cancel" }), _jsx(Button, { onClick: handleSaveRule, variant: "contained", disabled: isSaving || !formData.name.trim(), children: isSaving ? 'Saving...' : 'Save' })] })] }), _jsxs(Dialog, { open: deleteConfirmOpen, onClose: () => setDeleteConfirmOpen(false), children: [_jsx(DialogTitle, { children: "Delete Validation Rule?" }), _jsx(DialogContent, { children: _jsx(Box, { sx: { mt: 2 }, children: _jsx(Typography, { children: "Are you sure you want to delete this validation rule? This action cannot be undone." }) }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: () => setDeleteConfirmOpen(false), children: "Cancel" }), _jsx(Button, { onClick: handleDeleteConfirm, color: "error", variant: "contained", children: "Delete" })] })] })] }));
};
export default ValidationRulesManager;
