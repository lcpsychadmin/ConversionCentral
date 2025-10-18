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

import { Project, ProjectFormValues } from '../../types/data';

interface ProjectFormProps {
  open: boolean;
  title: string;
  initialValues?: Project | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: ProjectFormValues) => void;
}

const STATUS_OPTIONS = ['planned', 'active', 'on_hold', 'completed', 'archived'];

const sanitize = (value?: string | null) => value ?? '';

const ProjectForm = ({
  open,
  title,
  initialValues,
  loading = false,
  onClose,
  onSubmit
}: ProjectFormProps) => {
  const initialSnapshot = useMemo<ProjectFormValues>(
    () => ({
      name: sanitize(initialValues?.name),
      description: sanitize(initialValues?.description),
      status: initialValues?.status ?? 'planned'
    }),
    [initialValues]
  );

  const [values, setValues] = useState<ProjectFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<{ name?: string }>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof ProjectFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const next = event.target.value;
      setValues((prev) => ({ ...prev, [field]: next }));
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
              required
              fullWidth
              value={values.name}
              onChange={handleChange('name')}
              error={!!errors.name}
              helperText={errors.name}
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              minRows={3}
              value={values.description ?? ''}
              onChange={handleChange('description')}
            />
            <TextField
              select
              label="Status"
              fullWidth
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

export default ProjectForm;
