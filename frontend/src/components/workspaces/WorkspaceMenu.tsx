import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import { SelectChangeEvent } from '@mui/material/Select';
import { isAxiosError } from 'axios';
import { ChangeEvent, FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useMutation, useQueryClient } from 'react-query';

import { WORKSPACES_QUERY_KEY, useWorkspaces } from '@hooks/useWorkspaces';
import { createWorkspace, deleteWorkspace, updateWorkspace } from '@services/workspaceService';
import ConfirmDialog from '../common/ConfirmDialog';

const buildInitialFormState = () => ({
  name: '',
  description: '',
  isDefault: true,
  isActive: true
});

const extractErrorMessage = (error: unknown, fallback: string) => {
  if (isAxiosError(error)) {
    const detail = (error.response?.data as { detail?: string } | undefined)?.detail;
    return detail ?? fallback;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

type WorkspaceMenuProps = {
  variant?: 'header' | 'sidebar';
};

const WorkspaceMenu = ({ variant = 'header' }: WorkspaceMenuProps) => {
  const queryClient = useQueryClient();
  const { data: workspaces = [], isLoading: workspacesLoading } = useWorkspaces();
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState('');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [formValues, setFormValues] = useState(buildInitialFormState);
  const [formError, setFormError] = useState<string | null>(null);
  const nameFieldRef = useRef<HTMLInputElement>(null);
  const [manageDialogOpen, setManageDialogOpen] = useState(false);
  const [manageValues, setManageValues] = useState({ name: '', description: '' });
  const [manageError, setManageError] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const manageNameFieldRef = useRef<HTMLInputElement>(null);

  const defaultWorkspaceId = useMemo(
    () => workspaces.find((workspace) => workspace.isDefault)?.id ?? '',
    [workspaces]
  );

  const selectedWorkspace = useMemo(
    () => workspaces.find((workspace) => workspace.id === selectedWorkspaceId) ?? null,
    [selectedWorkspaceId, workspaces]
  );

  useEffect(() => {
    setSelectedWorkspaceId(defaultWorkspaceId);
  }, [defaultWorkspaceId]);

  useEffect(() => {
    if (dialogOpen) {
      nameFieldRef.current?.focus();
    }
  }, [dialogOpen]);

  useEffect(() => {
    if (manageDialogOpen) {
      manageNameFieldRef.current?.focus();
    }
  }, [manageDialogOpen]);

  const invalidateWorkspaces = useCallback(() => {
    queryClient.invalidateQueries(WORKSPACES_QUERY_KEY);
  }, [queryClient]);

  const updateDefaultMutation = useMutation(
    (workspaceId: string) => updateWorkspace(workspaceId, { isDefault: true }),
    {
      onSuccess: (_, workspaceId) => {
        setSelectedWorkspaceId(workspaceId);
        invalidateWorkspaces();
      }
    }
  );

  const createWorkspaceMutation = useMutation(createWorkspace, {
    onSuccess: (workspace) => {
      setDialogOpen(false);
      setFormValues(buildInitialFormState());
      setFormError(null);
      setSelectedWorkspaceId(workspace.id);
      invalidateWorkspaces();
    }
  });

  const updateWorkspaceDetailsMutation = useMutation(
    (payload: { workspaceId: string; name: string; description: string | null }) =>
      updateWorkspace(payload.workspaceId, {
        name: payload.name,
        description: payload.description
      }),
    {
      onSuccess: () => {
        setManageDialogOpen(false);
        setManageError(null);
        invalidateWorkspaces();
      }
    }
  );

  const deleteWorkspaceMutation = useMutation(deleteWorkspace, {
    onSuccess: () => {
      setManageDialogOpen(false);
      setManageError(null);
      setDeleteDialogOpen(false);
      setSelectedWorkspaceId('');
      invalidateWorkspaces();
    }
  });

  const cascadeWarning =
    'Deleting a workspace removes all associated data objects, data definitions, and data quality assets. This action cannot be undone.';

  const latestManageError = updateWorkspaceDetailsMutation.isError
    ? updateWorkspaceDetailsMutation.error
    : deleteWorkspaceMutation.isError
    ? deleteWorkspaceMutation.error
    : null;

  const handleWorkspaceChange = (event: SelectChangeEvent<unknown>) => {
    const value = String(event.target.value ?? '');
    setSelectedWorkspaceId(value);
    if (!value) {
      return;
    }
    updateDefaultMutation.mutate(value);
  };

  const handleDialogClose = () => {
    setDialogOpen(false);
    setFormValues(buildInitialFormState());
    setFormError(null);
  };

  const handleFormChange = (field: 'name' | 'description') => (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { value } = event.target;
    setFormValues((prev) => ({ ...prev, [field]: value }));
  };

  const handleSwitchChange = (field: 'isDefault' | 'isActive') => (
    _: ChangeEvent<HTMLInputElement>,
    checked: boolean
  ) => {
    setFormValues((prev) => ({ ...prev, [field]: checked }));
  };

  const handleCreateWorkspace = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmedName = formValues.name.trim();
    if (!trimmedName) {
      setFormError('Workspace name is required.');
      return;
    }
    setFormError(null);

    createWorkspaceMutation.mutate({
      name: trimmedName,
      description: formValues.description?.trim() || null,
      isActive: formValues.isActive,
      isDefault: formValues.isDefault
    });
  };

  const handleManageDialogOpen = () => {
    if (!selectedWorkspace) {
      return;
    }
    setManageValues({
      name: selectedWorkspace.name,
      description: selectedWorkspace.description ?? ''
    });
    setManageError(null);
    updateWorkspaceDetailsMutation.reset();
    deleteWorkspaceMutation.reset();
    setManageDialogOpen(true);
  };

  const handleManageDialogClose = () => {
    setManageDialogOpen(false);
    setManageError(null);
    updateWorkspaceDetailsMutation.reset();
    deleteWorkspaceMutation.reset();
  };

  const handleManageFieldChange = (field: 'name' | 'description') => (
    event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { value } = event.target;
    setManageValues((prev) => ({ ...prev, [field]: value }));
  };

  const handleManageWorkspaceSave = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedWorkspace) {
      return;
    }
    const trimmedName = manageValues.name.trim();
    if (!trimmedName) {
      setManageError('Workspace name is required.');
      return;
    }
    const trimmedDescription = manageValues.description.trim();
    setManageError(null);
    updateWorkspaceDetailsMutation.mutate({
      workspaceId: selectedWorkspace.id,
      name: trimmedName,
      description: trimmedDescription ? trimmedDescription : null
    });
  };

  const handleDeleteConfirmed = () => {
    if (!selectedWorkspace) {
      return;
    }
    deleteWorkspaceMutation.mutate(selectedWorkspace.id);
  };

  const selectionHelperText = updateDefaultMutation.isError
    ? extractErrorMessage(updateDefaultMutation.error, 'Unable to switch workspaces. Please try again.')
    : 'Select the workspace that should be treated as the default across reporting views.';

  const noWorkspacesAvailable = !workspacesLoading && workspaces.length === 0;
  const isSidebar = variant === 'sidebar';

  const textField = (
    <TextField
      select
      size="small"
      label="Default workspace"
      value={selectedWorkspaceId}
      sx={{
        minWidth: isSidebar ? '100%' : { xs: '100%', sm: 220 },
        flexGrow: 1
      }}
      disabled={workspacesLoading || noWorkspacesAvailable || updateDefaultMutation.isLoading}
      helperText={!isSidebar ? selectionHelperText : undefined}
      FormHelperTextProps={{
        sx: !isSidebar
          ? { color: 'common.white', opacity: 0.85 }
          : undefined
      }}
      InputProps={{
        endAdornment: workspacesLoading ? <CircularProgress size={18} /> : undefined
      }}
      SelectProps={{ onChange: handleWorkspaceChange }}
    >
      {noWorkspacesAvailable ? (
        <MenuItem value="" disabled>
          No workspaces yet
        </MenuItem>
      ) : (
        workspaces.map((workspace) => (
          <MenuItem key={workspace.id} value={workspace.id}>
            <Stack direction="row" spacing={1} alignItems="center">
              <Typography component="span">{workspace.name}</Typography>
              {!workspace.isActive && (
                <Typography component="span" variant="caption" color="text.secondary">
                  (inactive)
                </Typography>
              )}
            </Stack>
          </MenuItem>
        ))
      )}
    </TextField>
  );

  const workspaceSelector = isSidebar ? (
    <Tooltip
      title={selectionHelperText}
      placement="top"
      disableHoverListener={!isSidebar}
      disableFocusListener={!isSidebar}
      disableTouchListener={!isSidebar}
    >
      <Box sx={{ width: '100%' }}>{textField}</Box>
    </Tooltip>
  ) : (
    textField
  );

  return (
    <Box
      sx={{
        display: 'flex',
        ...(isSidebar
          ? {
              flexDirection: 'column',
              alignItems: 'stretch',
              width: '100%',
              mr: 0,
              gap: 1
            }
          : {
              alignItems: { xs: 'stretch', sm: 'center' },
              flexWrap: 'wrap',
              gap: 1,
              mr: { xs: 0, md: 1 },
              width: { xs: '100%', sm: 'auto' }
            })
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: isSidebar ? 'stretch' : 'center',
          gap: 1,
          width: isSidebar ? '100%' : 'auto',
          flexGrow: isSidebar ? 1 : 0
        }}
      >
        <Box sx={{ flexGrow: 1 }}>{workspaceSelector}</Box>
        <Tooltip
          title={selectedWorkspace ? 'Manage workspace' : 'Select a workspace to manage'}
          placement={isSidebar ? 'top' : 'bottom'}
        >
          <span>
            <IconButton
              size="small"
              onClick={handleManageDialogOpen}
              disabled={!selectedWorkspace || workspacesLoading}
              sx={{
                border: isSidebar ? '1px solid rgba(0,0,0,0.12)' : '1px solid rgba(255,255,255,0.6)',
                color: isSidebar ? 'text.primary' : 'common.white',
                backgroundColor: isSidebar ? 'background.paper' : 'transparent',
                '&:hover': {
                  borderColor: isSidebar ? 'text.primary' : 'common.white',
                  backgroundColor: isSidebar ? 'action.hover' : 'rgba(255,255,255,0.08)'
                },
                '&.Mui-disabled': {
                  borderColor: isSidebar ? 'divider' : 'rgba(255,255,255,0.3)',
                  color: isSidebar ? 'text.disabled' : 'rgba(255,255,255,0.4)'
                }
              }}
            >
              <SettingsOutlinedIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      </Box>
      <Tooltip title="Create a workspace">
        <span>
          <Button
            variant="outlined"
            color={isSidebar ? 'primary' : 'inherit'}
            size="small"
            startIcon={<AddCircleOutlineIcon fontSize="small" />}
            onClick={() => setDialogOpen(true)}
            disabled={createWorkspaceMutation.isLoading}
            sx={{
              width: isSidebar ? '100%' : 'auto',
              justifyContent: 'center',
              borderColor: isSidebar ? 'divider' : 'rgba(255,255,255,0.6)',
              color: isSidebar ? 'text.primary' : 'common.white',
              '&:hover': {
                borderColor: isSidebar ? 'text.primary' : 'common.white',
                backgroundColor: isSidebar ? 'action.hover' : undefined
              }
            }}
          >
            New workspace
          </Button>
        </span>
      </Tooltip>
      <Dialog open={dialogOpen} onClose={handleDialogClose} fullWidth maxWidth="sm">
        <Box component="form" onSubmit={handleCreateWorkspace}>
          <DialogTitle>Create workspace</DialogTitle>
          <DialogContent dividers>
            <Stack spacing={2}>
            <TextField
              label="Name"
              value={formValues.name}
              onChange={handleFormChange('name')}
              required
              error={Boolean(formError)}
              helperText={formError ?? 'Provide a descriptive name for the workspace.'}
                inputRef={nameFieldRef}
            />
            <TextField
              label="Description"
              value={formValues.description}
              onChange={handleFormChange('description')}
              multiline
              minRows={3}
            />
            <FormControlLabel
              control={
                <Switch
                  checked={formValues.isDefault}
                  onChange={handleSwitchChange('isDefault')}
                  color="primary"
                />
              }
              label="Make this the default workspace"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={formValues.isActive}
                  onChange={handleSwitchChange('isActive')}
                  color="primary"
                />
              }
              label={formValues.isActive ? 'Workspace is active' : 'Workspace is inactive'}
            />
              {createWorkspaceMutation.isError && (
                <Alert severity="error">
                  {extractErrorMessage(
                    createWorkspaceMutation.error,
                    'Unable to create workspace. Please try again.'
                  )}
                </Alert>
              )}
            </Stack>
          </DialogContent>
          <DialogActions>
            <Button onClick={handleDialogClose} disabled={createWorkspaceMutation.isLoading}>
              Cancel
            </Button>
            <Button type="submit" variant="contained" disabled={createWorkspaceMutation.isLoading}>
              {createWorkspaceMutation.isLoading ? 'Creating…' : 'Create workspace'}
            </Button>
          </DialogActions>
        </Box>
      </Dialog>
      <Dialog open={manageDialogOpen} onClose={handleManageDialogClose} fullWidth maxWidth="sm">
        <Box component="form" onSubmit={handleManageWorkspaceSave}>
          <DialogTitle>Manage workspace</DialogTitle>
          <DialogContent dividers>
            <Stack spacing={2}>
              <TextField
                label="Name"
                value={manageValues.name}
                onChange={handleManageFieldChange('name')}
                required
                error={Boolean(manageError)}
                helperText={manageError ?? 'Update the workspace name.'}
                inputRef={manageNameFieldRef}
              />
              <TextField
                label="Description"
                value={manageValues.description}
                onChange={handleManageFieldChange('description')}
                multiline
                minRows={3}
              />
              <Alert severity="warning">{cascadeWarning}</Alert>
              {Boolean(latestManageError) && (
                <Alert severity="error">
                  {extractErrorMessage(
                    latestManageError as unknown,
                    'Unable to update the workspace. Please try again.'
                  )}
                </Alert>
              )}
            </Stack>
          </DialogContent>
          <DialogActions sx={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
            <Button
              color="error"
              onClick={() => setDeleteDialogOpen(true)}
              disabled={!selectedWorkspace || deleteWorkspaceMutation.isLoading}
            >
              Delete workspace
            </Button>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button
                onClick={handleManageDialogClose}
                disabled={updateWorkspaceDetailsMutation.isLoading}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                variant="contained"
                disabled={updateWorkspaceDetailsMutation.isLoading}
              >
                {updateWorkspaceDetailsMutation.isLoading ? 'Saving…' : 'Save changes'}
              </Button>
            </Box>
          </DialogActions>
        </Box>
      </Dialog>
      <ConfirmDialog
        open={deleteDialogOpen}
        title="Delete workspace?"
        description={
          deleteWorkspaceMutation.isError
            ? extractErrorMessage(deleteWorkspaceMutation.error, cascadeWarning)
            : cascadeWarning
        }
        confirmLabel="Delete"
        onClose={() => {
          setDeleteDialogOpen(false);
          deleteWorkspaceMutation.reset();
        }}
        onConfirm={handleDeleteConfirmed}
        loading={deleteWorkspaceMutation.isLoading}
        confirmDisabled={!selectedWorkspace}
      />
    </Box>
  );
};

export default WorkspaceMenu;
