import { useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tooltip,
  Typography
} from '@mui/material';
import EditIcon from '@mui/icons-material/EditOutlined';
import DeleteIcon from '@mui/icons-material/DeleteOutline';

import PageHeader from '../components/common/PageHeader';
import ConfirmDialog from '../components/common/ConfirmDialog';
import LookupFormDialog, {
  LookupFormValues
} from '../components/application-settings/LookupFormDialog';
import { useLegalRequirements } from '../hooks/useLegalRequirements';
import { LegalRequirement } from '../types/data';
import { useAuth } from '../context/AuthContext';

const formatStatus = (status: string) =>
  status
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');

const getStatusColor = (status: string): 'default' | 'success' | 'warning' | 'error' => {
  switch (status) {
    case 'active':
      return 'success';
    case 'inactive':
      return 'warning';
    case 'deprecated':
      return 'error';
    default:
      return 'default';
  }
};

const LegalRequirementsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    legalRequirementsQuery,
    createLegalRequirement,
    updateLegalRequirement,
    deleteLegalRequirement,
    creating,
    updating,
    deleting
  } = useLegalRequirements();

  const { data = [], isLoading, isError, error, isFetching } = legalRequirementsQuery;

  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogMode, setDialogMode] = useState<'create' | 'edit'>('create');
  const [selected, setSelected] = useState<LegalRequirement | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const rows = useMemo(() => data, [data]);

  const busy = creating || updating || deleting;

  const handleCreateClick = () => {
    setDialogMode('create');
    setSelected(null);
    setDialogOpen(true);
  };

  const handleEditClick = (item: LegalRequirement) => {
    setDialogMode('edit');
    setSelected(item);
    setDialogOpen(true);
  };

  const handleDeleteClick = (item: LegalRequirement) => {
    setSelected(item);
    setConfirmOpen(true);
  };

  const handleDialogClose = () => {
    if (busy) return;
    setDialogOpen(false);
  };

  const handleDialogSubmit = async (values: LookupFormValues) => {
    try {
      if (dialogMode === 'create') {
        await createLegalRequirement({
          name: values.name,
          description: values.description,
          status: values.status,
          displayOrder: values.displayOrder ?? undefined
        });
      } else if (selected) {
        await updateLegalRequirement({
          id: selected.id,
          input: {
            name: values.name,
            description: values.description,
            status: values.status,
            displayOrder: values.displayOrder ?? undefined
          }
        });
      }
      setDialogOpen(false);
      setSelected(null);
    } catch (err) {
      // toast handled in hook
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteLegalRequirement(selected.id);
      setConfirmOpen(false);
      setSelected(null);
    } catch (err) {
      // toast handled in hook
    }
  };

  const errorMessage = isError
    ? error instanceof Error
      ? error.message
      : 'Unable to load requirements.'
    : null;

  return (
    <Box>
      <PageHeader
        title="Legal & Regulatory Requirements"
        subtitle="Maintain the reference list surfaced in data definition workflows."
        actions={
          canManage
            ? (
              <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
                New Requirement
              </Button>
            )
            : undefined
        }
      />

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      <Paper elevation={0} sx={{ p: 3, borderRadius: 3, mb: 3 }}>
        {isLoading && !rows.length ? (
          <Stack alignItems="center" justifyContent="center" spacing={2} sx={{ minHeight: 200 }}>
            <CircularProgress />
            <Typography variant="body2" color="text.secondary">
              Loading requirements...
            </Typography>
          </Stack>
        ) : rows.length === 0 ? (
          <Stack spacing={1} alignItems="center" sx={{ py: 6 }}>
            <Typography variant="h6">No requirements yet</Typography>
            <Typography variant="body2" color="text.secondary">
              Add legal or regulatory requirements to make them available when defining fields.
            </Typography>
            {canManage && (
              <Button variant="contained" onClick={handleCreateClick} disabled={busy}>
                Create Requirement
              </Button>
            )}
          </Stack>
        ) : (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="right">Display Order</TableCell>
                {canManage && <TableCell align="right">Actions</TableCell>}
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.id} hover>
                  <TableCell sx={{ fontWeight: 600 }}>{row.name}</TableCell>
                  <TableCell sx={{ maxWidth: 420 }}>
                    {row.description ? (
                      <Typography variant="body2" color="text.secondary">
                        {row.description}
                      </Typography>
                    ) : (
                      <Typography variant="body2" color="text.disabled">
                        Not provided
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={formatStatus(row.status)}
                      color={getStatusColor(row.status)}
                      size="small"
                    />
                  </TableCell>
                  <TableCell align="right">{row.displayOrder ?? '—'}</TableCell>
                  {canManage && (
                    <TableCell align="right" sx={{ whiteSpace: 'nowrap' }}>
                      <Tooltip title="Edit">
                        <span>
                          <IconButton
                            size="small"
                            onClick={() => handleEditClick(row)}
                            disabled={busy}
                          >
                            <EditIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <span>
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => handleDeleteClick(row)}
                            disabled={busy}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </TableCell>
                  )}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Paper>

      <LookupFormDialog
        open={dialogOpen}
        title={dialogMode === 'create' ? 'Create Requirement' : 'Edit Requirement'}
        initialValues={selected ? {
          name: selected.name,
          description: selected.description ?? null,
          status: selected.status,
          displayOrder: selected.displayOrder ?? null
        } : null}
        loading={busy}
        onClose={handleDialogClose}
        onSubmit={handleDialogSubmit}
        confirmLabel={dialogMode === 'create' ? 'Create' : 'Save'}
      />

      <ConfirmDialog
        open={confirmOpen}
        title="Delete Requirement"
        description={`Are you sure you want to delete "${selected?.name ?? ''}"? This cannot be undone.`}
        confirmLabel="Delete"
        onClose={() => setConfirmOpen(false)}
        onConfirm={handleConfirmDelete}
        loading={deleting}
      />

      {isFetching && !isLoading && (
        <Box sx={{ display: 'none' }}>{/* keep query fresh indicator */}</Box>
      )}
    </Box>
  );
};

export default LegalRequirementsPage;
