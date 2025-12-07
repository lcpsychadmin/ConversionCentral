import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  IconButton,
  MenuItem,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import RefreshIcon from '@mui/icons-material/Refresh';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import RuleIcon from '@mui/icons-material/Rule';
import { Link as RouterLink } from 'react-router-dom';

import PageHeader from '@components/common/PageHeader';
import ConfirmDialog from '@components/common/ConfirmDialog';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import { useWorkspaceScope } from '@hooks/useWorkspaceScope';
import {
  createDataQualityTestSuite,
  deleteDataQualityTestSuite,
  fetchDataQualityTestSuites,
  fetchDatasetHierarchy,
  updateDataQualityTestSuite
} from '@services/dataQualityService';
import {
  DataQualityDatasetApplication,
  DataQualityDatasetProductTeam,
  DataQualityDatasetObject,
  DataQualityDatasetDefinition,
  DataQualityTestSuite,
  DataQualityTestSuiteInput,
  DataQualityTestSuiteUpdate,
  DataQualityTestSuiteSeverity
} from '@cc-types/data';

const TEST_SUITES_QUERY_KEY = ['data-quality', 'test-suites'] as const;
const datasetHierarchyQueryKey = (workspaceId: string | null) =>
  ['data-quality', 'dataset-hierarchy', workspaceId ?? 'auto'] as const;

const SEVERITY_OPTIONS: { value: DataQualityTestSuiteSeverity; label: string; description: string }[] = [
  { value: 'low', label: 'Low', description: 'Advisory coverage; informative checks with minimal operational risk.' },
  { value: 'medium', label: 'Medium', description: 'Important rules that should be addressed, but do not block delivery.' },
  { value: 'high', label: 'High', description: 'Significant issues that warrant rapid attention and may pause downstream use.' },
  { value: 'critical', label: 'Critical', description: 'Blocking failures that halt promotion until resolved.' }
];

interface ScopeOption {
  id: string;
  label: string;
  productTeamId: string;
  applicationId: string;
  dataObjectId: string;
  dataDefinitionIds: string[];
}

interface FormState {
  name: string;
  description: string;
  severity: DataQualityTestSuiteSeverity;
  scope: ScopeOption | null;
}

const DEFAULT_FORM: FormState = {
  name: '',
  description: '',
  severity: 'medium',
  scope: null
};

const normalizeSuiteKey = (value?: string | null): string | null => {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const lowered = trimmed.toLowerCase();
  if (lowered === 'undefined' || lowered === 'null') {
    return null;
  }
  return trimmed;
};

const buildScopeOptions = (hierarchy: DataQualityDatasetProductTeam[] | undefined): ScopeOption[] => {
  if (!hierarchy) {
    return [];
  }

  const options: ScopeOption[] = [];

  hierarchy.forEach((team) => {
    team.applications.forEach((application: DataQualityDatasetApplication) => {
      application.dataObjects.forEach((dataObject: DataQualityDatasetObject) => {
        const definitions: DataQualityDatasetDefinition[] = dataObject.dataDefinitions ?? [];
        const definitionIds = definitions.map((definition) => definition.dataDefinitionId);
        const label = [team.name, application.name, dataObject.name]
          .filter(Boolean)
          .join(' > ');
        options.push({
          id: `${team.productTeamId}:${application.applicationId}:${dataObject.dataObjectId}`,
          label,
          productTeamId: String(team.productTeamId),
          applicationId: String(application.applicationId),
          dataObjectId: String(dataObject.dataObjectId),
          dataDefinitionIds: definitionIds.map((id) => String(id))
        });
      });
    });
  });

  return options.sort((left, right) => left.label.localeCompare(right.label));
};

const severityChipColor = (severity?: string | null) => {
  switch (severity) {
    case 'critical':
      return 'error';
    case 'high':
      return 'warning';
    case 'medium':
      return 'info';
    case 'low':
    default:
      return 'default';
  }
};

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
};

const mapSuiteToInput = (form: FormState): DataQualityTestSuiteInput => ({
  name: form.name.trim(),
  description: form.description.trim() || null,
  severity: form.severity,
  productTeamId: form.scope?.productTeamId ?? null,
  applicationId: form.scope?.applicationId ?? null,
  dataObjectId: form.scope?.dataObjectId ?? null,
  dataDefinitionId: form.scope?.dataDefinitionIds?.[0] ?? null
});

const DataQualityTestSuitesPage = () => {
  const { workspaceId } = useWorkspaceScope();
  const queryClient = useQueryClient();
  const { snackbar, showSuccess, showError } = useSnackbarFeedback();

  const [formState, setFormState] = useState<FormState>(DEFAULT_FORM);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingSuite, setEditingSuite] = useState<DataQualityTestSuite | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DataQualityTestSuite | null>(null);

  const suitesQuery = useQuery(TEST_SUITES_QUERY_KEY, () => fetchDataQualityTestSuites(), {
    staleTime: 2 * 60 * 1000,
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unable to load test suites.';
      showError(message);
    }
  });

  const hierarchyQuery = useQuery(datasetHierarchyQueryKey(workspaceId ?? null), () => fetchDatasetHierarchy(workspaceId ?? undefined), {
    staleTime: 5 * 60 * 1000
  });

  const scopeOptions = useMemo(() => buildScopeOptions(hierarchyQuery.data), [hierarchyQuery.data]);
  const scopeLookup = useMemo(() => {
    const map = new Map<string, ScopeOption>();
    const register = (key: string | null | undefined, option: ScopeOption) => {
      if (!key) {
        return;
      }
      map.set(key.toLowerCase(), option);
    };

    scopeOptions.forEach((option) => {
      register(option.dataObjectId, option);
      option.dataDefinitionIds.forEach((id) => register(id, option));

      if (option.productTeamId && option.applicationId && option.dataObjectId) {
        const composite = [option.productTeamId, option.applicationId, option.dataObjectId]
          .map((part) => part.toLowerCase())
          .join('|');
        register(composite, option);
      }
    });

    return map;
  }, [scopeOptions]);

  const resolveScopeForSuite = useCallback(
    (suite: DataQualityTestSuite): ScopeOption | null => {
      const candidates: string[] = [];

      if (suite.dataDefinitionId) {
        candidates.push(String(suite.dataDefinitionId).toLowerCase());
      }
      if (suite.dataObjectId) {
        candidates.push(String(suite.dataObjectId).toLowerCase());
      }
      if (suite.productTeamId && suite.applicationId && suite.dataObjectId) {
        candidates.push(
          [suite.productTeamId, suite.applicationId, suite.dataObjectId]
            .map((part) => String(part).toLowerCase())
            .join('|')
        );
      }

      for (const key of candidates) {
        const match = scopeLookup.get(key);
        if (match) {
          return match;
        }
      }

      return null;
    },
    [scopeLookup]
  );

  const resetForm = useCallback(() => {
    setFormState(DEFAULT_FORM);
    setEditingSuite(null);
  }, []);

  const handleCloseDialog = () => {
    setDialogOpen(false);
    resetForm();
  };

  const openCreateDialog = () => {
    resetForm();
    setDialogOpen(true);
  };

  const openEditDialog = (suite: DataQualityTestSuite) => {
    const suiteKey = normalizeSuiteKey(suite.testSuiteKey);
    if (!suiteKey) {
      showError('This suite is missing an identifier and cannot be edited. Create a new suite instead.');
      return;
    }

    const matchedScope = resolveScopeForSuite(suite);

    setFormState({
      name: suite.name,
      description: suite.description ?? '',
      severity: (suite.severity as DataQualityTestSuiteSeverity) ?? 'medium',
      scope: matchedScope
    });
    setEditingSuite(suite);
    setDialogOpen(true);
  };

  const createMutation = useMutation(createDataQualityTestSuite, {
    onSuccess: () => {
      showSuccess('Test suite created.');
      queryClient.invalidateQueries(TEST_SUITES_QUERY_KEY).catch(() => {});
      handleCloseDialog();
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unable to create test suite.';
      showError(message);
    }
  });

  const updateMutation = useMutation(
    ({ key, input }: { key: string; input: DataQualityTestSuiteUpdate }) =>
      updateDataQualityTestSuite(key, input),
    {
      onSuccess: () => {
        showSuccess('Test suite updated.');
        queryClient.invalidateQueries(TEST_SUITES_QUERY_KEY).catch(() => {});
        handleCloseDialog();
      },
      onError: (error) => {
        const message = error instanceof Error ? error.message : 'Unable to update test suite.';
        showError(message);
      }
    }
  );

  const deleteMutation = useMutation(deleteDataQualityTestSuite, {
    onSuccess: () => {
      showSuccess('Test suite deleted.');
      queryClient.invalidateQueries(TEST_SUITES_QUERY_KEY).catch(() => {});
      setDeleteTarget(null);
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unable to delete test suite.';
      showError(message);
    }
  });

  const handleDialogSubmit = () => {
    if (!formState.name.trim()) {
      showError('Please provide a test suite name.');
      return;
    }

    const payload = mapSuiteToInput(formState);

    if (editingSuite) {
      const updatePayload: DataQualityTestSuiteUpdate = {};

      if (payload.name && payload.name !== editingSuite.name) {
        updatePayload.name = payload.name;
      }

      if ((payload.description ?? null) !== (editingSuite.description ?? null)) {
        updatePayload.description = payload.description ?? null;
      }

      if ((payload.severity ?? null) !== (editingSuite.severity ?? null)) {
        updatePayload.severity = payload.severity ?? undefined;
      }

      if ((payload.productTeamId ?? null) !== (editingSuite.productTeamId ?? null)) {
        updatePayload.productTeamId = payload.productTeamId ?? null;
      }

      if ((payload.applicationId ?? null) !== (editingSuite.applicationId ?? null)) {
        updatePayload.applicationId = payload.applicationId ?? null;
      }

      if ((payload.dataObjectId ?? null) !== (editingSuite.dataObjectId ?? null)) {
        updatePayload.dataObjectId = payload.dataObjectId ?? null;
      }

      if ((payload.dataDefinitionId ?? null) !== (editingSuite.dataDefinitionId ?? null)) {
        updatePayload.dataDefinitionId = payload.dataDefinitionId ?? null;
      }

      if (Object.keys(updatePayload).length === 0) {
        showSuccess('No changes detected.');
        handleCloseDialog();
        return;
      }

      const suiteKey = normalizeSuiteKey(editingSuite.testSuiteKey);
      if (!suiteKey) {
        showError('Test suite identifier is missing. Create a new suite instead.');
        return;
      }

      updateMutation.mutate({ key: suiteKey, input: updatePayload });
    } else {
      createMutation.mutate(payload);
    }
  };

  const handleRefresh = async () => {
    try {
      await queryClient.invalidateQueries(TEST_SUITES_QUERY_KEY);
      showSuccess('Refreshed test suites.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to refresh test suites.';
      showError(message);
    }
  };

  const totalSuites = suitesQuery.data?.length ?? 0;
  const criticalCount = suitesQuery.data?.filter((suite) => suite.severity === 'critical').length ?? 0;
  const highCount = suitesQuery.data?.filter((suite) => suite.severity === 'high').length ?? 0;

  useEffect(() => {
    if (!dialogOpen) {
      return;
    }
    if (!editingSuite) {
      setFormState((prev) => ({ ...DEFAULT_FORM, severity: prev.severity }));
    }
  }, [dialogOpen, editingSuite]);

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Test Suites"
        subtitle="Define the reusable validation bundles that govern each dataset before runs execute."
      />

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
        <Paper elevation={1} sx={{ flex: 1, p: 3 }}>
          <Typography variant="overline" color="text.secondary">Total Suites</Typography>
          <Typography variant="h4" sx={{ fontWeight: 600 }}>{totalSuites}</Typography>
          <Typography variant="body2" color="text.secondary">
            Active collections of tests aligned to your applications and data objects.
          </Typography>
        </Paper>
        <Paper elevation={1} sx={{ flex: 1, p: 3 }}>
          <Typography variant="overline" color="text.secondary">High & Critical</Typography>
          <Typography variant="h4" sx={{ fontWeight: 600 }}>{highCount + criticalCount}</Typography>
          <Typography variant="body2" color="text.secondary">
            Suites marked as high priority that deserve closer monitoring.
          </Typography>
        </Paper>
      </Stack>

      <Paper elevation={1} sx={{ p: 3 }}>
        <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" spacing={2} alignItems={{ xs: 'stretch', md: 'center' }}>
          <Box>
            <Typography variant="h6">Test Suites</Typography>
            <Typography variant="body2" color="text.secondary">
              Manage suite definitions and keep their metadata aligned with your hierarchy.
            </Typography>
          </Box>
          <Stack direction="row" spacing={1}>
            <Button
              variant="outlined"
              startIcon={<RefreshIcon />}
              onClick={handleRefresh}
              disabled={suitesQuery.isFetching}
            >
              Refresh
            </Button>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={openCreateDialog}
              disabled={createMutation.isLoading || updateMutation.isLoading}
            >
              New Test Suite
            </Button>
          </Stack>
        </Stack>

        <Divider sx={{ my: 2 }} />

        {suitesQuery.isLoading ? (
          <Box display="flex" justifyContent="center" py={6}>
            <CircularProgress />
          </Box>
        ) : suitesQuery.isError ? (
          <Alert severity="error">
            Unable to load test suites at this time. Retry after verifying the Data Quality connection.
          </Alert>
        ) : suitesQuery.data && suitesQuery.data.length > 0 ? (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Application / Data Object</TableCell>
                <TableCell>Severity</TableCell>
                <TableCell>Updated</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {suitesQuery.data.map((suite) => {
                const matchedScope = resolveScopeForSuite(suite);
                const suiteKey = normalizeSuiteKey(suite.testSuiteKey);
                const hasSuiteKey = Boolean(suiteKey);

                return (
                  <TableRow key={suiteKey ?? suite.name} hover>
                    <TableCell sx={{ width: '28%' }}>
                      <Stack spacing={0.5}>
                        <Typography fontWeight={600}>{suite.name}</Typography>
                        {suite.description ? (
                          <Typography variant="body2" color="text.secondary">
                            {suite.description}
                          </Typography>
                        ) : null}
                      </Stack>
                    </TableCell>
                    <TableCell sx={{ width: '32%' }}>
                      {matchedScope ? (
                        <Typography>{matchedScope.label}</Typography>
                      ) : (
                        <Typography color="text.secondary">Not yet linked</Typography>
                      )}
                    </TableCell>
                    <TableCell sx={{ width: '12%' }}>
                      <Chip size="small" label={suite.severity ?? '—'} color={severityChipColor(suite.severity)} />
                    </TableCell>
                    <TableCell sx={{ width: '16%' }}>
                      <Typography variant="body2" color="text.secondary">
                        {formatDateTime(suite.updatedAt ?? suite.createdAt)}
                      </Typography>
                    </TableCell>
                    <TableCell align="right" sx={{ width: '12%' }}>
                      <Tooltip
                        title={hasSuiteKey ? 'Manage suite definition' : 'Suite identifier unavailable'}
                      >
                        <span>
                          {hasSuiteKey ? (
                            <IconButton
                              size="small"
                              component={RouterLink}
                              to={`/data-quality/test-suites/${suiteKey!}`}
                            >
                              <RuleIcon fontSize="small" />
                            </IconButton>
                          ) : (
                            <IconButton size="small" disabled>
                              <RuleIcon fontSize="small" />
                            </IconButton>
                          )}
                        </span>
                      </Tooltip>
                      <Tooltip title={hasSuiteKey ? 'Edit test suite' : 'Suite identifier unavailable'}>
                        <span>
                          <IconButton
                            size="small"
                            onClick={() => openEditDialog(suite)}
                            disabled={!hasSuiteKey}
                          >
                            <EditIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                      <Tooltip title={hasSuiteKey ? 'Delete test suite' : 'Suite identifier unavailable'}>
                        <span>
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => setDeleteTarget(suite)}
                            disabled={deleteMutation.isLoading || !hasSuiteKey}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        ) : (
          <Box textAlign="center" py={6}>
            <Typography variant="body1" fontWeight={500} gutterBottom>
              No test suites yet
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Start by creating a suite to group tests for a specific application and data object.
            </Typography>
            <Button variant="contained" startIcon={<AddIcon />} onClick={openCreateDialog}>
              Create your first suite
            </Button>
          </Box>
        )}
      </Paper>

      <Dialog open={dialogOpen} onClose={handleCloseDialog} fullWidth maxWidth="sm">
        <DialogTitle>{editingSuite ? 'Edit Test Suite' : 'Create Test Suite'}</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Test Suite Name"
              value={formState.name}
              onChange={(event) => setFormState((prev) => ({ ...prev, name: event.target.value }))}
              required
              fullWidth
            />
            <TextField
              label="Description"
              value={formState.description}
              onChange={(event) => setFormState((prev) => ({ ...prev, description: event.target.value }))}
              fullWidth
              multiline
              minRows={3}
            />
            <Autocomplete
              options={scopeOptions}
              value={formState.scope}
              onChange={(_, value) => setFormState((prev) => ({ ...prev, scope: value }))}
              getOptionLabel={(option) => option.label}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Application / Data Object"
                  placeholder="Select where this suite applies"
                  helperText="Choose the data object the suite should align with."
                />
              )}
              loading={hierarchyQuery.isLoading}
              disabled={hierarchyQuery.isError}
            />
            <TextField
              select
              label="Severity"
              value={formState.severity}
              onChange={(event) =>
                setFormState((prev) => ({ ...prev, severity: event.target.value as DataQualityTestSuiteSeverity }))
              }
            >
              {SEVERITY_OPTIONS.map((option) => (
                <MenuItem key={option.value} value={option.value}>
                  <Stack>
                    <Typography fontWeight={600}>{option.label}</Typography>
                    <Typography variant="body2" color="text.secondary">
                      {option.description}
                    </Typography>
                  </Stack>
                </MenuItem>
              ))}
            </TextField>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog} disabled={createMutation.isLoading || updateMutation.isLoading}>
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={handleDialogSubmit}
            disabled={createMutation.isLoading || updateMutation.isLoading}
          >
            {editingSuite ? 'Save Changes' : 'Create Test Suite'}
          </Button>
        </DialogActions>
      </Dialog>

      <ConfirmDialog
        open={Boolean(deleteTarget)}
        title="Delete Test Suite"
        description={
          deleteTarget
            ? `Are you sure you want to delete the "${deleteTarget.name}" test suite? This action cannot be undone.`
            : ''
        }
        onClose={() => setDeleteTarget(null)}
        onConfirm={() => {
          if (!deleteTarget) {
            return;
          }
          const suiteKey = normalizeSuiteKey(deleteTarget.testSuiteKey);
          if (!suiteKey) {
            showError('Test suite identifier is missing. This suite cannot be deleted.');
            setDeleteTarget(null);
            return;
          }
          deleteMutation.mutate(suiteKey);
        }}
        confirmColor="error"
        confirmLabel="Delete"
        loading={deleteMutation.isLoading}
      />

      {snackbar}
    </Stack>
  );
};

export default DataQualityTestSuitesPage;
