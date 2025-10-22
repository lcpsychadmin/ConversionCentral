import { useCallback, useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Stack,
  TextField,
  Box,
  Typography,
  Alert
} from '@mui/material';

import { ConstructedField } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

interface Props {
  open: boolean;
  fields: ConstructedField[];
  onAdd: (rowData: Record<string, any>) => Promise<void>;
  onClose: () => void;
}

const AddRowDialog: React.FC<Props> = ({ open, fields, onAdd, onClose }) => {
  const [formData, setFormData] = useState<Record<string, any>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});
  const toast = useToast();

  const handleChange = useCallback((fieldKey: string, value: any) => {
    setFormData((prev) => ({
      ...prev,
      [fieldKey]: value
    }));
    // Clear error for this field
    if (validationErrors[fieldKey]) {
      setValidationErrors((prev) => {
        const newErrors = { ...prev };
        delete newErrors[fieldKey];
        return newErrors;
      });
    }
  }, [validationErrors]);

  const handleSubmit = async () => {
    // Validate required fields
    const errors: Record<string, string> = {};
    for (const field of fields) {
      const value = formData[field.name];
      if (!field.isNullable && (value === undefined || value === null || value === '')) {
        errors[field.name] = `${field.name} is required`;
      }
    }

    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setIsSubmitting(true);
    try {
      await onAdd(formData);
      toast.showSuccess('Row added successfully');
      setFormData({});
      setValidationErrors({});
      onClose();
    } catch (error: any) {
      toast.showError(error.message || 'Failed to add row');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleClose = () => {
    if (!isSubmitting) {
      setFormData({});
      setValidationErrors({});
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Add New Row</DialogTitle>
      <DialogContent sx={{ pt: 2 }}>
        <Stack spacing={2}>
          {fields.length === 0 ? (
            <Alert severity="info">No fields defined for this table yet.</Alert>
          ) : (
            fields.map((field) => (
              <Box key={field.id}>
                <TextField
                  fullWidth
                  label={field.name}
                  type={field.dataType?.toLowerCase() === 'integer' ? 'number' : 'text'}
                  value={formData[field.name] ?? ''}
                  onChange={(e) => handleChange(field.name, e.target.value)}
                  disabled={isSubmitting}
                  inputProps={{
                    step: field.dataType?.toLowerCase() === 'integer' ? '1' : undefined,
                    min: field.dataType?.toLowerCase() === 'integer' ? '0' : undefined
                  }}
                  error={!!validationErrors[field.name]}
                />
                {validationErrors[field.name] && (
                  <Typography variant="caption" color="error" sx={{ mt: 0.5, display: 'block' }}>
                    {validationErrors[field.name]}
                  </Typography>
                )}
                {!field.isNullable && (
                  <Typography variant="caption" color="textSecondary" sx={{ display: 'block', mt: 0.5 }}>
                    Required field
                  </Typography>
                )}
              </Box>
            ))
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={isSubmitting}>
          Cancel
        </Button>
        <Button
          onClick={handleSubmit}
          variant="contained"
          disabled={isSubmitting || fields.length === 0}
        >
          {isSubmitting ? 'Adding...' : 'Add Row'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddRowDialog;
