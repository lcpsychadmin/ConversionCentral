import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';

export type FieldFormValues = {
  name: string;
  description: string;
  applicationUsage: string;
  businessDefinition: string;
  enterpriseAttribute: string;
  fieldType: string;
  fieldLength: string;
  decimalPlaces: string;
  systemRequired: boolean;
  businessProcessRequired: boolean;
  suppressedField: boolean;
  active: boolean;
  isUnique: boolean;
  legalRegulatoryImplications: string;
  securityClassification: string;
  dataValidation: string;
  referenceTable: string;
  groupingTab: string;
};

interface CreateFieldDialogProps {
  open: boolean;
  tableName?: string | null;
  loading?: boolean;
  mode?: 'create' | 'edit';
  initialValues?: Partial<Record<keyof FieldFormValues, string | boolean | null>>;
  onClose: () => void;
  onSubmit: (values: FieldFormValues) => Promise<void>;
}

const defaultValues: FieldFormValues = {
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
  isUnique: false,
  legalRegulatoryImplications: '',
  securityClassification: '',
  dataValidation: '',
  referenceTable: '',
  groupingTab: ''
};

const CreateFieldDialog = ({
  open,
  tableName,
  loading = false,
  mode = 'create',
  initialValues,
  onClose,
  onSubmit
}: CreateFieldDialogProps) => {
  const baseValues = useMemo((): FieldFormValues => {
    if (!initialValues) {
      return defaultValues;
    }
    return {
      name: (initialValues.name as string | undefined) ?? '',
      description: (initialValues.description as string | undefined) ?? '',
      applicationUsage: (initialValues.applicationUsage as string | undefined) ?? '',
      businessDefinition: (initialValues.businessDefinition as string | undefined) ?? '',
      enterpriseAttribute: (initialValues.enterpriseAttribute as string | undefined) ?? '',
      fieldType: (initialValues.fieldType as string | undefined) ?? '',
      fieldLength: initialValues.fieldLength != null ? String(initialValues.fieldLength) : '',
      decimalPlaces: initialValues.decimalPlaces != null ? String(initialValues.decimalPlaces) : '',
      systemRequired: Boolean(initialValues.systemRequired ?? false),
      businessProcessRequired: Boolean(initialValues.businessProcessRequired ?? false),
      suppressedField: Boolean(initialValues.suppressedField ?? false),
      active: initialValues.active === undefined ? true : Boolean(initialValues.active),
      isUnique: Boolean(initialValues.isUnique ?? false),
      legalRegulatoryImplications:
        (initialValues.legalRegulatoryImplications as string | undefined) ?? '',
      securityClassification: (initialValues.securityClassification as string | undefined) ?? '',
      dataValidation: (initialValues.dataValidation as string | undefined) ?? '',
      referenceTable: (initialValues.referenceTable as string | undefined) ?? '',
      groupingTab: (initialValues.groupingTab as string | undefined) ?? ''
    };
  }, [initialValues]);

  const [values, setValues] = useState<FieldFormValues>(baseValues);
  const [errors, setErrors] = useState<{ name?: string; fieldType?: string; fieldLength?: string; decimalPlaces?: string }>({});

  useEffect(() => {
    if (!open) return;
    setValues(baseValues);
    setErrors({});
  }, [open, baseValues]);

  const isDirty = useMemo(() => {
    return Object.entries(values).some(([key, value]) => value !== baseValues[key as keyof FieldFormValues]);
  }, [values, baseValues]);

  const handleChange = (field: keyof FieldFormValues) => (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setValues((prev) => ({ ...prev, [field]: value }));
    setErrors((prev) => ({ ...prev, [field]: undefined }));
  };

  const handleBooleanChange = (
    field: 'systemRequired' | 'businessProcessRequired' | 'suppressedField' | 'active' | 'isUnique'
  ) => (_: ChangeEvent<HTMLInputElement>, checked: boolean) => {
    setValues((prev) => ({ ...prev, [field]: checked }));
  };

  const handleClose = () => {
    if (loading) return;
    setValues(baseValues);
    setErrors({});
    onClose();
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextErrors: typeof errors = {};

    if (!values.name.trim()) {
      nextErrors.name = 'Field name is required.';
    }
    if (!values.fieldType.trim()) {
      nextErrors.fieldType = 'Field type is required.';
    }

    const validateNonNegativeInteger = (
      raw: string,
      field: 'fieldLength' | 'decimalPlaces',
      label: string
    ) => {
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
  isUnique: values.isUnique,
      legalRegulatoryImplications: values.legalRegulatoryImplications.trim(),
      securityClassification: values.securityClassification.trim(),
      dataValidation: values.dataValidation.trim(),
      referenceTable: values.referenceTable.trim(),
      groupingTab: values.groupingTab.trim()
    });
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form noValidate onSubmit={handleSubmit}>
        <DialogTitle>{mode === 'create' ? 'Create Field' : 'Edit Field'}</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} mt={1}>
            {tableName && (
              <Typography variant="body2" color="text.secondary">
                Table: {tableName}
              </Typography>
            )}
            <TextField
              label="Field Name"
              value={values.name}
              onChange={handleChange('name')}
              required
              error={!!errors.name}
              helperText={errors.name}
              fullWidth
            />
            <TextField
              label="Description"
              value={values.description}
              onChange={handleChange('description')}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Application Usage"
              value={values.applicationUsage}
              onChange={handleChange('applicationUsage')}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Business Definition"
              value={values.businessDefinition}
              onChange={handleChange('businessDefinition')}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Enterprise Attribute"
              value={values.enterpriseAttribute}
              onChange={handleChange('enterpriseAttribute')}
              fullWidth
            />
            <TextField
              label="Field Type"
              value={values.fieldType}
              onChange={handleChange('fieldType')}
              required
              error={!!errors.fieldType}
              helperText={errors.fieldType}
              fullWidth
            />
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
              <TextField
                label="Field Length"
                value={values.fieldLength}
                onChange={handleChange('fieldLength')}
                type="number"
                inputProps={{ min: 0 }}
                error={!!errors.fieldLength}
                helperText={errors.fieldLength}
                fullWidth
              />
              <TextField
                label="Decimal Places"
                value={values.decimalPlaces}
                onChange={handleChange('decimalPlaces')}
                type="number"
                inputProps={{ min: 0 }}
                error={!!errors.decimalPlaces}
                helperText={errors.decimalPlaces}
                fullWidth
              />
            </Stack>
            <Stack direction="row" spacing={2} flexWrap="wrap">
              <FormControlLabel
                control={<Switch checked={values.systemRequired} onChange={handleBooleanChange('systemRequired')} />}
                label="System Required"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={values.businessProcessRequired}
                    onChange={handleBooleanChange('businessProcessRequired')}
                  />
                }
                label="Business Process Required"
              />
              <FormControlLabel
                control={<Switch checked={values.suppressedField} onChange={handleBooleanChange('suppressedField')} />}
                label="Suppressed Field"
              />
              <FormControlLabel
                control={<Switch checked={values.active} onChange={handleBooleanChange('active')} />}
                label="Active"
              />
              <FormControlLabel
                control={<Switch checked={values.isUnique} onChange={handleBooleanChange('isUnique')} />}
                label="Unique Field"
              />
            </Stack>
            <TextField
              label="Legal / Regulatory Implications"
              value={values.legalRegulatoryImplications}
              onChange={handleChange('legalRegulatoryImplications')}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Security Classification"
              value={values.securityClassification}
              onChange={handleChange('securityClassification')}
              fullWidth
            />
            <TextField
              label="Data Validation"
              value={values.dataValidation}
              onChange={handleChange('dataValidation')}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Reference Table"
              value={values.referenceTable}
              onChange={handleChange('referenceTable')}
              fullWidth
            />
            <TextField
              label="Grouping Tab"
              value={values.groupingTab}
              onChange={handleChange('groupingTab')}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <LoadingButton type="submit" variant="contained" loading={loading} disabled={loading || !isDirty}>
            {mode === 'create' ? 'Create Field' : 'Save Changes'}
          </LoadingButton>
        </DialogActions>
      </form>
    </Dialog>
  );
};

export default CreateFieldDialog;
