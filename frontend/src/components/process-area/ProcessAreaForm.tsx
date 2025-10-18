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

import { ProcessAreaFormValues, ProcessArea } from '../../types/data';

interface ProcessAreaFormProps {
  open: boolean;
  title: string;
  initialValues?: ProcessArea | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: ProcessAreaFormValues) => void;
}

const trimValue = (value?: string | null) => {
  if (value == null) return '';
  return value;
};

const STATUS_OPTIONS = ['draft', 'active', 'archived'];

const ProcessAreaForm = ({
  open,
  title,
  initialValues,
  loading = false,
  onClose,
  onSubmit
}: ProcessAreaFormProps) => {
  const initialSnapshot = useMemo<ProcessAreaFormValues>(
    () => ({
      name: trimValue(initialValues?.name),
      description: trimValue(initialValues?.description ?? ''),
      status: initialValues?.status ?? 'draft'
    }),
    [initialValues]
  );

  const [values, setValues] = useState<ProcessAreaFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<{ name?: string }>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof ProcessAreaFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setValues((prev) => ({ ...prev, [field]: value }));
      if (field === 'name') {
        setErrors((prev) => ({ ...prev, name: undefined }));
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
    const trimmedDescription = values.description?.trim() ?? '';

    const nextErrors: { name?: string } = {};

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
    return (
      values.name !== initialSnapshot.name ||
      (values.description ?? '') !== (initialSnapshot.description ?? '') ||
      values.status !== initialSnapshot.status
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
              id="process-area-name"
              name="name"
              value={values.name}
              onChange={handleChange('name')}
              error={!!errors.name}
              helperText={errors.name}
              required
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              minRows={3}
              id="process-area-description"
              name="description"
              value={values.description ?? ''}
              onChange={handleChange('description')}
            />
            <TextField
              select
              label="Status"
              fullWidth
              id="process-area-status"
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
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <LoadingButton
            type="submit"
            variant="contained"
            loading={loading}
            disabled={loading || (!isDirty && !initialValues)}
          >
            Save
          </LoadingButton>
        </DialogActions>
      </Box>
    </Dialog>
  );
};

export default ProcessAreaForm;
