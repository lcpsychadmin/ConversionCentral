import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Autocomplete,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography
} from '@mui/material';

import { Field } from '../../types/data';

interface AddExistingFieldDialogProps {
  open: boolean;
  tableName: string;
  fields: Field[];
  loading?: boolean;
  onClose: () => void;
  onSubmit: (payload: { fieldId: string; notes: string }) => Promise<void> | void;
}

const AddExistingFieldDialog = ({
  open,
  tableName,
  fields,
  loading = false,
  onClose,
  onSubmit
}: AddExistingFieldDialogProps) => {
  const [selectedField, setSelectedField] = useState<Field | null>(null);
  const [notes, setNotes] = useState('');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    setSelectedField(null);
    setNotes('');
    setError(null);
  }, [open, fields]);

  const fieldOptions = useMemo(() => fields.slice().sort((a, b) => a.name.localeCompare(b.name)), [fields]);

  const handleClose = () => {
    if (loading) {
      return;
    }
    onClose();
  };

  const handleSubmit = async () => {
    if (!selectedField) {
      setError('Select a field to add.');
      return;
    }

    setError(null);
    await onSubmit({ fieldId: selectedField.id, notes });
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Add Field to Definition</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2} mt={1}>
          <Typography variant="body2" color="text.secondary">
            Table: {tableName}
          </Typography>
          <Autocomplete
            options={fieldOptions}
            value={selectedField}
            onChange={(_, value) => {
              setSelectedField(value);
              setError(null);
            }}
            getOptionLabel={(option) => `${option.name} (${option.fieldType})`}
            isOptionEqualToValue={(option, value) => option.id === value.id}
            renderInput={(params) => (
              <TextField
                {...params}
                label="Field"
                placeholder={fields.length ? 'Select field' : 'No fields available'}
                required
                error={Boolean(error)}
                helperText={error}
              />
            )}
            disabled={loading || !fields.length}
          />
          <TextField
            label="Notes"
            value={notes}
            onChange={(event) => setNotes(event.target.value)}
            multiline
            minRows={2}
            fullWidth
            disabled={loading}
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={loading}>
          Cancel
        </Button>
        <LoadingButton
          onClick={handleSubmit}
          variant="contained"
          loading={loading}
          disabled={loading || !fields.length}
        >
          Add Field
        </LoadingButton>
      </DialogActions>
    </Dialog>
  );
};

export default AddExistingFieldDialog;
