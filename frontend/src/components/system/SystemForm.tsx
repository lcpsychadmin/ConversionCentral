import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Stack,
  TextField
} from '@mui/material';

import { System, SystemFormValues } from '../../types/data';

interface SystemFormProps {
  open: boolean;
  title: string;
  initialValues?: System | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: SystemFormValues) => void;
}

const STATUS_OPTIONS = ['active', 'inactive', 'decommissioned'];

const sanitize = (value?: string | null) => value ?? '';

const SystemForm = ({ open, title, initialValues, loading = false, onClose, onSubmit }: SystemFormProps) => {
  const initialSnapshot = useMemo<SystemFormValues>(
    () => ({
      name: sanitize(initialValues?.name),
      physicalName: sanitize(initialValues?.physicalName),
      description: sanitize(initialValues?.description),
      systemType: sanitize(initialValues?.systemType),
      status: initialValues?.status ?? 'active',
      securityClassification: sanitize(initialValues?.securityClassification)
    }),
    [initialValues]
  );

  const [values, setValues] = useState<SystemFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<{ name?: string; physicalName?: string }>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof SystemFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
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

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const trimmedName = values.name.trim();
    const trimmedPhysicalName = values.physicalName.trim();
    const nextErrors: { name?: string; physicalName?: string } = {};

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
    return (
      values.name !== initialSnapshot.name ||
      values.physicalName !== initialSnapshot.physicalName ||
      (values.description ?? '') !== (initialSnapshot.description ?? '') ||
      (values.systemType ?? '') !== (initialSnapshot.systemType ?? '') ||
      values.status !== initialSnapshot.status ||
      (values.securityClassification ?? '') !== (initialSnapshot.securityClassification ?? '')
    );
  }, [values, initialSnapshot]);

  return (
    <Dialog open={open} onClose={handleClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            <TextField
              label="Name"
              fullWidth
              required
              id="system-name"
              name="name"
              value={values.name}
              onChange={handleChange('name')}
              error={!!errors.name}
              helperText={errors.name}
            />
            <TextField
              label="Physical Name"
              fullWidth
              required
              id="system-physical-name"
              name="physicalName"
              value={values.physicalName}
              onChange={handleChange('physicalName')}
              error={!!errors.physicalName}
              helperText={errors.physicalName}
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              minRows={2}
              id="system-description"
              name="description"
              value={values.description ?? ''}
              onChange={handleChange('description')}
            />
            <TextField
              label="System Type"
              fullWidth
              id="system-type"
              name="systemType"
              value={values.systemType ?? ''}
              onChange={handleChange('systemType')}
            />
            <TextField
              select
              label="Status"
              fullWidth
              id="system-status"
              name="status"
              value={values.status}
              onChange={handleChange('status')}
            >
              {STATUS_OPTIONS.map((option) => (
                <MenuItem key={option} value={option}>
                  {option}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              label="Security Classification"
              fullWidth
              id="system-security-class"
              name="securityClassification"
              value={values.securityClassification ?? ''}
              onChange={handleChange('securityClassification')}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <LoadingButton type="submit" variant="contained" loading={loading} disabled={loading || (!isDirty && !!initialValues)}>
            Save
          </LoadingButton>
        </DialogActions>
      </Box>
    </Dialog>
  );
};

export default SystemForm;
