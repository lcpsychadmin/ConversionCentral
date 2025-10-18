import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Stack,
  TextField
} from '@mui/material';

interface CreateTableDialogProps {
  open: boolean;
  loading?: boolean;
  mode?: 'create' | 'edit';
  initialValues?: {
    name?: string | null;
    physicalName?: string | null;
    schemaName?: string | null;
    description?: string | null;
    tableType?: string | null;
    status?: string | null;
  };
  onClose: () => void;
  onSubmit: (
    values: {
      name: string;
      physicalName: string;
      schemaName: string;
      description: string;
      tableType: string;
      status: string;
    }
  ) => Promise<void>;
}

const STATUS_OPTIONS = ['active', 'draft', 'deprecated'];
const TABLE_TYPE_OPTIONS = ['BASE', 'VIEW', 'REFERENCE'];

type TableFormValues = {
  name: string;
  physicalName: string;
  schemaName: string;
  description: string;
  tableType: string;
  status: string;
};

const defaultValues: TableFormValues = {
  name: '',
  physicalName: '',
  schemaName: '',
  description: '',
  tableType: '',
  status: 'active'
};

const CreateTableDialog = ({
  open,
  loading = false,
  mode = 'create',
  initialValues,
  onClose,
  onSubmit
}: CreateTableDialogProps) => {
  const baseValues = useMemo<TableFormValues>(
    () =>
      initialValues
        ? {
            name: initialValues.name ?? '',
            physicalName: initialValues.physicalName ?? '',
            schemaName: initialValues.schemaName ?? '',
            description: initialValues.description ?? '',
            tableType: initialValues.tableType ?? '',
            status: initialValues.status ?? 'active'
          }
        : defaultValues,
    [initialValues]
  );

  const [values, setValues] = useState<TableFormValues>(baseValues);
  const [errors, setErrors] = useState<{ name?: string; physicalName?: string }>({});

  useEffect(() => {
    if (!open) return;
    setValues(baseValues);
    setErrors({});
  }, [open, baseValues]);

  const isDirty = useMemo(() => {
    const keys = Object.keys(values) as Array<keyof TableFormValues>;
    return keys.some((key) => values[key] !== baseValues[key]);
  }, [values, baseValues]);

  const handleChange =
    (field: keyof TableFormValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setValues((prev) => ({ ...prev, [field]: value }));
      setErrors((prev) => ({ ...prev, [field]: undefined }));
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
      nextErrors.name = 'Table name is required.';
    }
    if (!values.physicalName.trim()) {
      nextErrors.physicalName = 'Physical name is required.';
    }

    if (Object.keys(nextErrors).length > 0) {
      setErrors(nextErrors);
      return;
    }

    await onSubmit({
      name: values.name.trim(),
      physicalName: values.physicalName.trim(),
      schemaName: values.schemaName.trim(),
      description: values.description.trim(),
      tableType: values.tableType.trim(),
      status: values.status.trim()
    });
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form noValidate onSubmit={handleSubmit}>
        <DialogTitle>{mode === 'create' ? 'Create Table' : 'Edit Table'}</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} mt={1}>
            <TextField
              label="Table Name"
              value={values.name}
              onChange={handleChange('name')}
              required
              error={!!errors.name}
              helperText={errors.name}
              fullWidth
            />
            <TextField
              label="Physical Name"
              value={values.physicalName}
              onChange={handleChange('physicalName')}
              required
              error={!!errors.physicalName}
              helperText={errors.physicalName}
              fullWidth
            />
            <TextField
              label="Schema"
              value={values.schemaName}
              onChange={handleChange('schemaName')}
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
              select
              label="Table Type"
              value={values.tableType}
              onChange={handleChange('tableType')}
              fullWidth
              helperText="Optional classification (e.g., base, view)"
            >
              <MenuItem value="">
                <em>None</em>
              </MenuItem>
              {TABLE_TYPE_OPTIONS.map((option) => (
                <MenuItem key={option} value={option}>
                  {option}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Status"
              value={values.status}
              onChange={handleChange('status')}
              fullWidth
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
          <LoadingButton type="submit" variant="contained" loading={loading} disabled={loading || !isDirty}>
            {mode === 'create' ? 'Create Table' : 'Save Changes'}
          </LoadingButton>
        </DialogActions>
      </form>
    </Dialog>
  );
};

export default CreateTableDialog;
