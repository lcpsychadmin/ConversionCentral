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

import { Project, Release, ReleaseFormValues } from '../../types/data';

interface ReleaseFormProps {
  open: boolean;
  title: string;
  initialValues?: Release | null;
  projectOptions: Project[];
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: ReleaseFormValues) => void;
}

const STATUS_OPTIONS = ['planned', 'in_progress', 'ready', 'active', 'completed', 'archived'];

const ReleaseForm = ({
  open,
  title,
  initialValues,
  projectOptions,
  loading = false,
  onClose,
  onSubmit
}: ReleaseFormProps) => {
  const initialSnapshot = useMemo<ReleaseFormValues>(() => {
    const defaultProjectId = initialValues?.projectId ?? projectOptions[0]?.id ?? '';
    return {
      projectId: defaultProjectId,
      name: initialValues?.name ?? '',
      description: initialValues?.description ?? '',
      status: initialValues?.status ?? 'planned'
    };
  }, [initialValues, projectOptions]);

  const [values, setValues] = useState<ReleaseFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<{ name?: string; projectId?: string }>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof ReleaseFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const next = event.target.value;
      setValues((prev) => ({ ...prev, [field]: next }));
      setErrors((prev) => ({ ...prev, [field]: undefined }));
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

    const nextErrors: { name?: string; projectId?: string } = {};
    if (!values.projectId) {
      nextErrors.projectId = 'Select a project';
    }
    if (!trimmedName) {
      nextErrors.name = 'Name is required';
    }

    if (Object.keys(nextErrors).length > 0) {
      setErrors(nextErrors);
      return;
    }

    onSubmit({
      projectId: values.projectId,
      name: trimmedName,
      description: trimmedDescription || null,
      status: values.status
    });
  };

  const isDirty = useMemo(() => {
    return (
      values.projectId !== initialSnapshot.projectId ||
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
              select
              label="Project"
              fullWidth
              value={values.projectId}
              onChange={handleChange('projectId')}
              error={!!errors.projectId}
              helperText={errors.projectId}
            >
              {projectOptions.map((project) => (
                <MenuItem key={project.id} value={project.id}>
                  {project.name}
                </MenuItem>
              ))}
            </TextField>
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

export default ReleaseForm;
