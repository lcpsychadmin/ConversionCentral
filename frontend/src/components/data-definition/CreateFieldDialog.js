import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import { Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControlLabel, Stack, Switch, TextField, Typography } from '@mui/material';
const defaultValues = {
    name: '',
    description: '',
    applicationUsage: '',
    businessDefinition: '',
    enterpriseAttribute: '',
    fieldType: '',
    fieldLength: '',
    decimalPlaces: '',
    systemRequired: false,
    businessProcessRequired: false,
    suppressedField: false,
    active: true,
    legalRegulatoryImplications: '',
    securityClassification: '',
    dataValidation: '',
    referenceTable: '',
    groupingTab: ''
};
const CreateFieldDialog = ({ open, tableName, loading = false, mode = 'create', initialValues, onClose, onSubmit }) => {
    const baseValues = useMemo(() => {
        if (!initialValues) {
            return defaultValues;
        }
        return {
            name: initialValues.name ?? '',
            description: initialValues.description ?? '',
            applicationUsage: initialValues.applicationUsage ?? '',
            businessDefinition: initialValues.businessDefinition ?? '',
            enterpriseAttribute: initialValues.enterpriseAttribute ?? '',
            fieldType: initialValues.fieldType ?? '',
            fieldLength: initialValues.fieldLength != null ? String(initialValues.fieldLength) : '',
            decimalPlaces: initialValues.decimalPlaces != null ? String(initialValues.decimalPlaces) : '',
            systemRequired: Boolean(initialValues.systemRequired ?? false),
            businessProcessRequired: Boolean(initialValues.businessProcessRequired ?? false),
            suppressedField: Boolean(initialValues.suppressedField ?? false),
            active: initialValues.active === undefined ? true : Boolean(initialValues.active),
            legalRegulatoryImplications: initialValues.legalRegulatoryImplications ?? '',
            securityClassification: initialValues.securityClassification ?? '',
            dataValidation: initialValues.dataValidation ?? '',
            referenceTable: initialValues.referenceTable ?? '',
            groupingTab: initialValues.groupingTab ?? ''
        };
    }, [initialValues]);
    const [values, setValues] = useState(baseValues);
    const [errors, setErrors] = useState({});
    useEffect(() => {
        if (!open)
            return;
        setValues(baseValues);
        setErrors({});
    }, [open, baseValues]);
    const isDirty = useMemo(() => {
        return Object.entries(values).some(([key, value]) => value !== baseValues[key]);
    }, [values, baseValues]);
    const handleChange = (field) => (event) => {
        const value = event.target.value;
        setValues((prev) => ({ ...prev, [field]: value }));
        setErrors((prev) => ({ ...prev, [field]: undefined }));
    };
    const handleBooleanChange = (field) => (_, checked) => {
        setValues((prev) => ({ ...prev, [field]: checked }));
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
            nextErrors.name = 'Field name is required.';
        }
        if (!values.fieldType.trim()) {
            nextErrors.fieldType = 'Field type is required.';
        }
        const validateNonNegativeInteger = (raw, field, label) => {
            if (!raw.trim()) {
                return;
            }
            const parsed = Number(raw);
            if (Number.isNaN(parsed) || parsed < 0) {
                nextErrors[field] = `${label} must be zero or greater.`;
            }
        };
        validateNonNegativeInteger(values.fieldLength, 'fieldLength', 'Field length');
        validateNonNegativeInteger(values.decimalPlaces, 'decimalPlaces', 'Decimal places');
        if (Object.keys(nextErrors).length > 0) {
            setErrors(nextErrors);
            return;
        }
        await onSubmit({
            name: values.name.trim(),
            description: values.description.trim(),
            applicationUsage: values.applicationUsage.trim(),
            businessDefinition: values.businessDefinition.trim(),
            enterpriseAttribute: values.enterpriseAttribute.trim(),
            fieldType: values.fieldType.trim(),
            fieldLength: values.fieldLength.trim(),
            decimalPlaces: values.decimalPlaces.trim(),
            systemRequired: values.systemRequired,
            businessProcessRequired: values.businessProcessRequired,
            suppressedField: values.suppressedField,
            active: values.active,
            legalRegulatoryImplications: values.legalRegulatoryImplications.trim(),
            securityClassification: values.securityClassification.trim(),
            dataValidation: values.dataValidation.trim(),
            referenceTable: values.referenceTable.trim(),
            groupingTab: values.groupingTab.trim()
        });
    };
    return (_jsx(Dialog, { open: open, onClose: handleClose, maxWidth: "sm", fullWidth: true, children: _jsxs("form", { noValidate: true, onSubmit: handleSubmit, children: [_jsx(DialogTitle, { children: mode === 'create' ? 'Create Field' : 'Edit Field' }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, mt: 1, children: [tableName && (_jsxs(Typography, { variant: "body2", color: "text.secondary", children: ["Table: ", tableName] })), _jsx(TextField, { label: "Field Name", value: values.name, onChange: handleChange('name'), required: true, error: !!errors.name, helperText: errors.name, fullWidth: true }), _jsx(TextField, { label: "Description", value: values.description, onChange: handleChange('description'), multiline: true, minRows: 2, fullWidth: true }), _jsx(TextField, { label: "Application Usage", value: values.applicationUsage, onChange: handleChange('applicationUsage'), multiline: true, minRows: 2, fullWidth: true }), _jsx(TextField, { label: "Business Definition", value: values.businessDefinition, onChange: handleChange('businessDefinition'), multiline: true, minRows: 2, fullWidth: true }), _jsx(TextField, { label: "Enterprise Attribute", value: values.enterpriseAttribute, onChange: handleChange('enterpriseAttribute'), fullWidth: true }), _jsx(TextField, { label: "Field Type", value: values.fieldType, onChange: handleChange('fieldType'), required: true, error: !!errors.fieldType, helperText: errors.fieldType, fullWidth: true }), _jsxs(Stack, { direction: { xs: 'column', md: 'row' }, spacing: 2, children: [_jsx(TextField, { label: "Field Length", value: values.fieldLength, onChange: handleChange('fieldLength'), type: "number", inputProps: { min: 0 }, error: !!errors.fieldLength, helperText: errors.fieldLength, fullWidth: true }), _jsx(TextField, { label: "Decimal Places", value: values.decimalPlaces, onChange: handleChange('decimalPlaces'), type: "number", inputProps: { min: 0 }, error: !!errors.decimalPlaces, helperText: errors.decimalPlaces, fullWidth: true })] }), _jsxs(Stack, { direction: "row", spacing: 2, flexWrap: "wrap", children: [_jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.systemRequired, onChange: handleBooleanChange('systemRequired') }), label: "System Required" }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.businessProcessRequired, onChange: handleBooleanChange('businessProcessRequired') }), label: "Business Process Required" }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.suppressedField, onChange: handleBooleanChange('suppressedField') }), label: "Suppressed Field" }), _jsx(FormControlLabel, { control: _jsx(Switch, { checked: values.active, onChange: handleBooleanChange('active') }), label: "Active" })] }), _jsx(TextField, { label: "Legal / Regulatory Implications", value: values.legalRegulatoryImplications, onChange: handleChange('legalRegulatoryImplications'), multiline: true, minRows: 2, fullWidth: true }), _jsx(TextField, { label: "Security Classification", value: values.securityClassification, onChange: handleChange('securityClassification'), fullWidth: true }), _jsx(TextField, { label: "Data Validation", value: values.dataValidation, onChange: handleChange('dataValidation'), multiline: true, minRows: 2, fullWidth: true }), _jsx(TextField, { label: "Reference Table", value: values.referenceTable, onChange: handleChange('referenceTable'), fullWidth: true }), _jsx(TextField, { label: "Grouping Tab", value: values.groupingTab, onChange: handleChange('groupingTab'), fullWidth: true })] }) }), _jsxs(DialogActions, { children: [_jsx(Button, { onClick: handleClose, disabled: loading, children: "Cancel" }), _jsx(LoadingButton, { type: "submit", variant: "contained", loading: loading, disabled: loading || !isDirty, children: mode === 'create' ? 'Create Field' : 'Save Changes' })] })] }) }));
};
export default CreateFieldDialog;
