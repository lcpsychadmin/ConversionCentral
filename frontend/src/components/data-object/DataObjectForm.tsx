import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Alert,
  Autocomplete,
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

import { DataObject, DataObjectFormValues, ProcessArea, System } from '../../types/data';

interface DataObjectFormProps {
  open: boolean;
  title: string;
  initialValues?: DataObject | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: DataObjectFormValues) => void;
  processAreas: ProcessArea[];
  systems: System[];
}

const DEFAULT_STATUS_OPTIONS = ['draft', 'active', 'archived'];

const sanitize = (value?: string | null) => (value ?? '');

const arraysEqual = (a: string[], b: string[]) => {
  if (a.length !== b.length) return false;
  const sortedA = [...a].sort();
  const sortedB = [...b].sort();
  return sortedA.every((value, index) => value === sortedB[index]);
};

const DataObjectForm = ({
  open,
  title,
  initialValues,
  loading = false,
  onClose,
  onSubmit,
  processAreas,
  systems
}: DataObjectFormProps) => {
  const initialSnapshot = useMemo<DataObjectFormValues>(() => {
    const defaultProcessArea = processAreas[0]?.id ?? '';
    return {
      name: sanitize(initialValues?.name),
      description: sanitize(initialValues?.description),
      status: initialValues?.status ?? 'draft',
      processAreaId: initialValues?.processAreaId ?? defaultProcessArea,
      systemIds: initialValues?.systems?.map((system) => system.id) ?? []
    };
  }, [initialValues, processAreas]);

  const [values, setValues] = useState<DataObjectFormValues>(initialSnapshot);
  const [errors, setErrors] = useState<{ name?: string; processAreaId?: string }>({});

  useEffect(() => {
    setValues(initialSnapshot);
    setErrors({});
  }, [initialSnapshot, open]);

  const handleChange = (field: keyof DataObjectFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setValues((prev: DataObjectFormValues) => ({ ...prev, [field]: value }));
      setErrors((prev) => ({ ...prev, [field]: undefined }));
    };

  const handleSystemsChange = (_: unknown, selected: System[]) => {
    setValues((prev) => ({ ...prev, systemIds: selected.map((system) => system.id) }));
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
    const nextErrors: { name?: string; processAreaId?: string } = {};

    if (!values.processAreaId) {
      nextErrors.processAreaId = 'Product team is required';
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
    return (
      values.name !== initialSnapshot.name ||
      (values.description ?? '') !== (initialSnapshot.description ?? '') ||
      values.status !== initialSnapshot.status ||
      values.processAreaId !== initialSnapshot.processAreaId ||
      !arraysEqual(values.systemIds, initialSnapshot.systemIds)
    );
  }, [values, initialSnapshot]);

  const noProcessAreas = processAreas.length === 0;

  return (
    <Dialog open={open} onClose={handleClose} fullWidth maxWidth="sm">
      <Box component="form" noValidate onSubmit={handleSubmit}>
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} mt={1}>
            {noProcessAreas && (
              <Alert severity="warning">
                Create a product team before adding data objects. Once available, you can assign the data object to it here.
              </Alert>
            )}
            <TextField
              label="Name"
              fullWidth
              required
              id="data-object-name"
              name="name"
              value={values.name}
              onChange={handleChange('name')}
              error={!!errors.name}
              helperText={errors.name}
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              minRows={2}
              id="data-object-description"
              name="description"
              value={values.description ?? ''}
              onChange={handleChange('description')}
            />
            <TextField
              select
              label="Product Team"
              fullWidth
              required
              id="data-object-process-area"
              name="processAreaId"
              value={values.processAreaId ?? ''}
              onChange={handleChange('processAreaId')}
              error={!!errors.processAreaId}
              helperText={errors.processAreaId ?? 'Select the product team that owns this data object.'}
              disabled={noProcessAreas}
            >
              {processAreas.map((area) => (
                <MenuItem key={area.id} value={area.id}>
                  {area.name}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Status"
              fullWidth
              id="data-object-status"
              name="status"
              value={values.status}
              onChange={handleChange('status')}
            >
              {DEFAULT_STATUS_OPTIONS.map((option) => (
                <MenuItem key={option} value={option}>
                  {option}
                </MenuItem>
              ))}
            </TextField>
            <Autocomplete
              multiple
              options={systems}
              disableCloseOnSelect
              getOptionLabel={(option) => option.name}
              isOptionEqualToValue={(option, value) => option.id === value?.id}
              value={systems.filter((system) => values.systemIds.includes(system.id))}
              onChange={handleSystemsChange}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Systems"
                  placeholder={systems.length === 0 ? 'No systems available' : 'Select systems'}
                />
              )}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <LoadingButton type="submit" variant="contained" loading={loading} disabled={loading || !isDirty}>
            Save
          </LoadingButton>
        </DialogActions>
      </Box>
    </Dialog>
  );
};

export default DataObjectForm;
