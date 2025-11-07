import { useCallback, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Grid,
  IconButton,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';

import { ConstructedField, ConstructedDataValidationRule, createValidationRule, updateValidationRule, deleteValidationRule } from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  validationRules: ConstructedDataValidationRule[];
  onRulesChange: () => void;
}

type RuleType = ConstructedDataValidationRule['ruleType'];

interface RuleConfiguration extends Record<string, unknown> {
  fieldName?: string;
  min?: number;
  max?: number;
  pattern?: string;
  expression?: string;
  fields?: string[];
  rule?: string;
}

interface RuleFormData {
  name: string;
  description: string;
  ruleType: RuleType;
  fieldId?: string | null;
  configuration: RuleConfiguration;
  errorMessage: string;
  isActive: boolean;
  appliesTo_NewOnly: boolean;
}

const getRuleTypeLabel = (ruleType: RuleType) => {
  const labels: Record<RuleType, string> = {
    required: 'Required',
    unique: 'Unique',
    range: 'Range',
    pattern: 'Pattern',
    custom: 'Custom Expression',
    cross_field: 'Cross-Field'
  };
  return labels[ruleType];
};

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return fallback;
};

const createInitialFormState = (): RuleFormData => ({
  name: '',
  description: '',
  ruleType: 'required',
  fieldId: null,
  configuration: {},
  errorMessage: 'Validation failed',
  isActive: true,
  appliesTo_NewOnly: false
});

const sanitizeConfiguration = (configuration: RuleConfiguration): Record<string, unknown> => {
  return Object.entries(configuration).reduce<Record<string, unknown>>((acc, [key, value]) => {
    if (value === undefined) {
      return acc;
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        return acc;
      }
      acc[key] = trimmed;
      return acc;
    }
    if (Array.isArray(value)) {
      if (value.length > 0) {
        acc[key] = value;
      }
      return acc;
    }
    acc[key] = value;
    return acc;
  }, {});
};

type RuleApiBasePayload = Omit<ConstructedDataValidationRule, 'id' | 'createdAt' | 'updatedAt' | 'constructedTableId'>;

const buildRulePayload = (data: RuleFormData): RuleApiBasePayload => ({
  name: data.name.trim(),
  description: data.description.trim() || null,
  ruleType: data.ruleType,
  fieldId: data.fieldId ?? null,
  configuration: sanitizeConfiguration(data.configuration),
  errorMessage: data.errorMessage.trim() || 'Validation failed',
  isActive: data.isActive,
  appliesTo_NewOnly: data.appliesTo_NewOnly
});

const ValidationRulesManager: React.FC<Props> = ({
  constructedTableId,
  fields,
  validationRules,
  onRulesChange
}) => {
  const theme = useTheme();
  const toast = useToast();

  // State
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingRuleId, setEditingRuleId] = useState<string | null>(null);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [deleteRuleId, setDeleteRuleId] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [formData, setFormData] = useState<RuleFormData>(() => createInitialFormState());

  // Handlers
  const handleOpenDialog = useCallback(() => {
    setFormData(createInitialFormState());
    setEditingRuleId(null);
    setDialogOpen(true);
  }, []);

  const handleCloseDialog = useCallback(() => {
    setDialogOpen(false);
  }, []);

  const handleSaveRule = useCallback(async () => {
    if (!formData.name.trim()) {
      toast.showError('Rule name is required');
      return;
    }

    setIsSaving(true);
    try {
      const basePayload = buildRulePayload(formData);
      if (editingRuleId) {
        await updateValidationRule(editingRuleId, basePayload);
        toast.showSuccess('Rule updated successfully');
      } else {
        await createValidationRule({ ...basePayload, constructedTableId });
        toast.showSuccess('Rule created successfully');
      }
      setDialogOpen(false);
      onRulesChange();
    } catch (error: unknown) {
      toast.showError(getErrorMessage(error, 'Failed to save rule'));
    } finally {
      setIsSaving(false);
    }
  }, [formData, editingRuleId, constructedTableId, toast, onRulesChange]);

  const handleDeleteClick = (ruleId: string) => {
    setDeleteRuleId(ruleId);
    setDeleteConfirmOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteRuleId) return;

    try {
      await deleteValidationRule(deleteRuleId);
      toast.showSuccess('Rule deleted successfully');
      onRulesChange();
    } catch (error: unknown) {
      toast.showError(getErrorMessage(error, 'Failed to delete rule'));
    } finally {
      setDeleteConfirmOpen(false);
      setDeleteRuleId(null);
    }
  };

  const handleToggleActive = async (rule: ConstructedDataValidationRule) => {
    try {
      await updateValidationRule(rule.id, {
        isActive: !rule.isActive
      });
      toast.showSuccess(rule.isActive ? 'Rule disabled' : 'Rule enabled');
      onRulesChange();
    } catch (error: unknown) {
      toast.showError(getErrorMessage(error, 'Failed to toggle rule'));
    }
  };

  const renderConfigForm = () => {
    switch (formData.ruleType) {
      case 'required':
        return (
          <TextField
            fullWidth
            label="Field"
            select
            value={formData.fieldId || ''}
            onChange={(e) => {
              const nextFieldId = e.target.value || null;
              const selectedField = fields.find((field) => field.id === nextFieldId);
              setFormData({
                ...formData,
                fieldId: nextFieldId,
                configuration: { fieldName: selectedField?.name || undefined }
              });
            }}
            InputLabelProps={{ shrink: true }}
            SelectProps={{
              native: true
            }}
          >
            <option value="">Select field</option>
            {fields.map((field) => (
              <option key={field.id} value={field.id}>
                {field.name}
              </option>
            ))}
          </TextField>
        );

      case 'unique':
        return (
          <TextField
            fullWidth
            label="Field"
            select
            value={formData.fieldId || ''}
            onChange={(e) => {
              const nextFieldId = e.target.value || null;
              const selectedField = fields.find((field) => field.id === nextFieldId);
              setFormData({
                ...formData,
                fieldId: nextFieldId,
                configuration: { fieldName: selectedField?.name || undefined }
              });
            }}
            InputLabelProps={{ shrink: true }}
            SelectProps={{
              native: true
            }}
          >
            <option value="">Select field</option>
            {fields.map((field) => (
              <option key={field.id} value={field.id}>
                {field.name}
              </option>
            ))}
          </TextField>
        );

      case 'range':
        return (
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Field"
                select
                value={formData.fieldId || ''}
                onChange={(e) => {
                  const nextFieldId = e.target.value || null;
                  const selectedField = fields.find((field) => field.id === nextFieldId);
                  setFormData({
                    ...formData,
                    fieldId: nextFieldId,
                    configuration: {
                      ...formData.configuration,
                      fieldName: selectedField?.name || undefined
                    }
                  });
                }}
                InputLabelProps={{ shrink: true }}
                SelectProps={{
                  native: true
                }}
              >
                <option value="">Select field</option>
                {fields.map((field) => (
                  <option key={field.id} value={field.id}>
                    {field.name}
                  </option>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={6}>
              <TextField
                fullWidth
                label="Minimum"
                type="number"
                value={formData.configuration.min ?? ''}
                onChange={(e) => {
                  const nextValue = e.target.value === '' ? undefined : Number(e.target.value);
                  setFormData({
                    ...formData,
                    configuration: { ...formData.configuration, min: nextValue }
                  });
                }}
              />
            </Grid>
            <Grid item xs={6}>
              <TextField
                fullWidth
                label="Maximum"
                type="number"
                value={formData.configuration.max ?? ''}
                onChange={(e) => {
                  const nextValue = e.target.value === '' ? undefined : Number(e.target.value);
                  setFormData({
                    ...formData,
                    configuration: { ...formData.configuration, max: nextValue }
                  });
                }}
              />
            </Grid>
          </Grid>
        );

      case 'pattern':
        return (
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Field"
                select
                value={formData.fieldId || ''}
                onChange={(e) => {
                  const nextFieldId = e.target.value || null;
                  const selectedField = fields.find((field) => field.id === nextFieldId);
                  setFormData({
                    ...formData,
                    fieldId: nextFieldId,
                    configuration: {
                      ...formData.configuration,
                      fieldName: selectedField?.name || undefined
                    }
                  });
                }}
                InputLabelProps={{ shrink: true }}
                SelectProps={{
                  native: true
                }}
              >
                <option value="">Select field</option>
                {fields.map((field) => (
                  <option key={field.id} value={field.id}>
                    {field.name}
                  </option>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Regex Pattern"
                multiline
                rows={2}
                placeholder="e.g., ^[0-9]{3}-[0-9]{3}-[0-9]{4}$"
                value={formData.configuration.pattern || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    configuration: { ...formData.configuration, pattern: e.target.value }
                  })
                }
              />
            </Grid>
          </Grid>
        );

      case 'custom':
        return (
          <TextField
            fullWidth
            label="Expression"
            multiline
            rows={3}
            placeholder="e.g., Age > 18 AND Status == 'Active'"
            value={formData.configuration.expression || ''}
            onChange={(e) =>
              setFormData({
                ...formData,
                configuration: { expression: e.target.value }
              })
            }
          />
        );

      case 'cross_field':
        return (
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Fields (comma-separated)"
                placeholder="e.g., StartDate, EndDate"
                value={(formData.configuration.fields || []).join(', ')}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    configuration: {
                      ...formData.configuration,
                      fields: e.target.value
                        .split(',')
                        .map((f) => f.trim())
                        .filter((value) => value.length > 0)
                    }
                  })
                }
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Rule"
                multiline
                rows={2}
                placeholder="e.g., StartDate <= EndDate"
                value={formData.configuration.rule || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    configuration: { ...formData.configuration, rule: e.target.value }
                  })
                }
              />
            </Grid>
          </Grid>
        );

      default:
        return null;
    }
  };

  return (
    <>
      <Box sx={{ p: 2 }}>
        <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between' }}>
          <Typography variant="h6">Validation Rules ({validationRules.length})</Typography>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleOpenDialog}
          >
            Create Rule
          </Button>
        </Box>

        {validationRules.length === 0 ? (
          <Alert severity="info">
            No validation rules defined yet. Select Create Rule to add validation rules.
          </Alert>
        ) : (
          <TableContainer sx={{ border: 1, borderColor: 'divider', borderRadius: 1 }}>
            <Table>
              <TableHead>
                <TableRow sx={{ backgroundColor: alpha(theme.palette.primary.main, 0.1) }}>
                  <TableCell sx={{ fontWeight: 'bold' }}>Name</TableCell>
                  <TableCell sx={{ fontWeight: 'bold' }}>Type</TableCell>
                  <TableCell sx={{ fontWeight: 'bold' }}>Field</TableCell>
                  <TableCell sx={{ fontWeight: 'bold' }}>Active</TableCell>
                  <TableCell sx={{ fontWeight: 'bold', width: 100 }}>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {validationRules.map((rule) => (
                  <TableRow key={rule.id}>
                    <TableCell>
                      <Box>
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {rule.name}
                        </Typography>
                        {rule.description && (
                          <Typography variant="caption" color="textSecondary">
                            {rule.description}
                          </Typography>
                        )}
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={getRuleTypeLabel(rule.ruleType)}
                        size="small"
                        color={rule.isActive ? 'primary' : 'default'}
                        variant={rule.isActive ? 'filled' : 'outlined'}
                      />
                    </TableCell>
                    <TableCell>
                      {rule.fieldId ? fields.find((f) => f.id === rule.fieldId)?.name : 'N/A'}
                    </TableCell>
                    <TableCell>
                      <Switch
                        checked={rule.isActive}
                        onChange={() => handleToggleActive(rule)}
                        size="small"
                      />
                    </TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={0.5}>
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => handleDeleteClick(rule.id)}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Stack>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Box>

      {/* Create/Edit Dialog */}
      <Dialog open={dialogOpen} onClose={handleCloseDialog} maxWidth="sm" fullWidth>
        <DialogTitle>
          {editingRuleId ? 'Edit Validation Rule' : 'Create Validation Rule'}
        </DialogTitle>
        <DialogContent sx={{ pt: 2 }}>
          <Stack spacing={2}>
            <TextField
              fullWidth
              label="Rule Name"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              disabled={isSaving}
            />
            <TextField
              fullWidth
              label="Description"
              multiline
              rows={2}
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              disabled={isSaving}
            />
            <TextField
              fullWidth
              label="Rule Type"
              select
              value={formData.ruleType}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  ruleType: e.target.value as RuleType,
                  configuration: {},
                  fieldId: null
                })
              }
              InputLabelProps={{ shrink: true }}
              SelectProps={{
                native: true
              }}
              disabled={isSaving}
            >
              <option value="required">Required</option>
              <option value="unique">Unique</option>
              <option value="range">Range</option>
              <option value="pattern">Pattern</option>
              <option value="custom">Custom Expression</option>
              <option value="cross_field">Cross-Field</option>
            </TextField>

            {renderConfigForm()}

            <TextField
              fullWidth
              label="Error Message"
              multiline
              rows={2}
              value={formData.errorMessage}
              onChange={(e) => setFormData({ ...formData, errorMessage: e.target.value })}
              disabled={isSaving}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={formData.isActive}
                  onChange={(e) => setFormData({ ...formData, isActive: e.target.checked })}
                  disabled={isSaving}
                />
              }
              label="Active"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog} disabled={isSaving}>
            Cancel
          </Button>
          <Button
            onClick={handleSaveRule}
            variant="contained"
            disabled={isSaving || !formData.name.trim()}
          >
            {isSaving ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteConfirmOpen} onClose={() => setDeleteConfirmOpen(false)}>
        <DialogTitle>Delete Validation Rule?</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <Typography>
              Are you sure you want to delete this validation rule? This action cannot be undone.
            </Typography>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteConfirmOpen(false)}>Cancel</Button>
          <Button onClick={handleDeleteConfirm} color="error" variant="contained">
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default ValidationRulesManager;
