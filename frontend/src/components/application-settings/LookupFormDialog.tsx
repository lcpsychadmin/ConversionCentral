import { useEffect, useMemo, useRef, useState } from 'react';
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

export interface LookupFormValues {
  name: string;
  description: string | null;
  status: string;
  displayOrder: number | null;
}

interface StatusOption {
  value: string;
  label: string;
}

interface LookupFormDialogProps {
  open: boolean;
  title: string;
  initialValues?: LookupFormValues | null;
  loading?: boolean;
  onClose: () => void;
  onSubmit: (values: LookupFormValues) => Promise<void> | void;
  statusOptions?: StatusOption[];
  confirmLabel?: string;
}

const DEFAULT_VALUES: LookupFormValues = {
  name: '',
  description: null,
  status: 'active',
  displayOrder: null
};

const DEFAULT_STATUS_OPTIONS: StatusOption[] = [
  { value: 'active', label: 'Active' },
  { value: 'inactive', label: 'Inactive' },
  { value: 'deprecated', label: 'Deprecated' }
];

const LookupFormDialog = ({
  open,
  title,
  initialValues,
  loading = false,
  onClose,
  onSubmit,
  statusOptions,
  confirmLabel = 'Save'
}: LookupFormDialogProps) => {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [status, setStatus] = useState('active');
  const [displayOrder, setDisplayOrder] = useState('');
  const [errors, setErrors] = useState<{ name?: string; displayOrder?: string }>({});
  const nameInputRef = useRef<HTMLInputElement | null>(null);

  const options = useMemo(() => statusOptions ?? DEFAULT_STATUS_OPTIONS, [statusOptions]);

  useEffect(() => {
    const values = initialValues ?? DEFAULT_VALUES;
    setName(values.name ?? '');
    setDescription(values.description ?? '');
    setStatus(values.status ?? 'active');
    setDisplayOrder(values.displayOrder !== null && values.displayOrder !== undefined ? String(values.displayOrder) : '');
    setErrors({});
  }, [initialValues, open]);

  useEffect(() => {
    if (open && !loading) {
      nameInputRef.current?.focus();
    }
  }, [open, loading]);

  const handleSubmit = async () => {
    const trimmedName = name.trim();
    const trimmedDescription = description.trim();
    const nextErrors: { name?: string; displayOrder?: string } = {};

    if (!trimmedName) {
      nextErrors.name = 'Name is required.';
    }

    let parsedDisplayOrder: number | null = null;
    const normalizedDisplayOrder = displayOrder.trim();
    if (normalizedDisplayOrder.length > 0) {
      const asNumber = Number(normalizedDisplayOrder);
      if (!Number.isInteger(asNumber) || asNumber < 0) {
        nextErrors.displayOrder = 'Enter a non-negative whole number.';
      } else {
        parsedDisplayOrder = asNumber;
      }
    }

    if (Object.keys(nextErrors).length > 0) {
      setErrors(nextErrors);
      return;
    }

    setErrors({});
    await onSubmit({
      name: trimmedName,
      description: trimmedDescription.length > 0 ? trimmedDescription : null,
      status: status || 'active',
      displayOrder: parsedDisplayOrder
    });
  };

  return (
    <Dialog open={open} onClose={loading ? undefined : onClose} fullWidth maxWidth="sm">
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <Stack spacing={3} sx={{ mt: 1.5 }}>
          <TextField
            label="Name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            required
            disabled={loading}
            error={Boolean(errors.name)}
            helperText={errors.name}
            inputRef={nameInputRef}
            fullWidth
          />
          <TextField
            label="Description"
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            disabled={loading}
            multiline
            minRows={3}
            fullWidth
            placeholder="Provide context for where this requirement applies."
          />
          <TextField
            label="Status"
            select
            value={status}
            onChange={(event) => setStatus(event.target.value)}
            disabled={loading}
          >
            {options.map((option) => (
              <MenuItem key={option.value} value={option.value}>
                {option.label}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label="Display Order"
            value={displayOrder}
            onChange={(event) => setDisplayOrder(event.target.value)}
            disabled={loading}
            inputProps={{ inputMode: 'numeric', pattern: '[0-9]*' }}
            error={Boolean(errors.displayOrder)}
            helperText={errors.displayOrder ?? 'Optional. Determines ordering in dropdowns.'}
          />
        </Stack>
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={onClose} disabled={loading} variant="text">
          Cancel
        </Button>
        <Button onClick={handleSubmit} disabled={loading} variant="contained">
          {loading ? 'Saving...' : confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LookupFormDialog;
