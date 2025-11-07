import { ReactNode, SyntheticEvent, useCallback, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { LoadingButton } from '@mui/lab';
import {
  Autocomplete,
  Box,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tab as MuiTab,
  Tabs as MuiTabs,
  TextField,
  Typography,
  IconButton,
  Chip,
  LinearProgress,
  Tooltip,
  Button
} from '@mui/material';
import { SxProps, Theme, alpha, useTheme } from '@mui/material/styles';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

import { useToast } from '../hooks/useToast';
import { useAuth } from '../context/AuthContext';
import {
  fetchProcessAreas,
  fetchDataObjects,
  fetchSystems,
  fetchAllConstructionDefinitions,
  fetchConstructedData,
  fetchConstructedFields,
  fetchValidationRules,
  ProcessArea,
  DataObject,
  System,
  DataDefinitionTable
} from '../services/constructedDataService';
import { deleteUploadedTable } from '../services/uploadDataService';
import ConstructedDataGrid from '../components/data-construction/ConstructedDataGridAgGrid';
import ValidationRulesManager from '../components/data-construction/ValidationRulesManager';

const getErrorMessage = (error: unknown, fallback: string) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

interface TabPanelProps {
  children?: ReactNode;
  index: number;
  value: number;
  sx?: SxProps<Theme>;
}

function TabPanel({ children, value, index, sx, ...other }: TabPanelProps) {
  const isActive = value === index;

  return (
    <Box
      role="tabpanel"
      hidden={!isActive}
      id={`table-details-tabpanel-${index}`}
      aria-labelledby={`table-details-tab-${index}`}
      sx={[
        {
          display: isActive ? 'flex' : 'none',
          flexDirection: 'column',
          flex: 1,
          minHeight: 0,
          overflow: 'hidden'
        },
        ...(Array.isArray(sx) ? sx : sx ? [sx] : [])
      ]}
      {...other}
    >
      {isActive ? (
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          {children}
        </Box>
      ) : null}
    </Box>
  );
}

type ConstructionTableRow = DataDefinitionTable & {
  dataObjectId: string;
  processAreaId?: string | null;
  systemId: string;
  dataObjectName?: string | null;
  processAreaName?: string | null;
  systemName?: string | null;
};

const DataConstructionPage = () => {
  const theme = useTheme();
  const toast = useToast();
  const queryClient = useQueryClient();
  const { hasRole } = useAuth();

  const canManage = hasRole('admin');

  const [tablePendingDelete, setTablePendingDelete] = useState<ConstructionTableRow | null>(null);

  // Filter State
  const [selectedProcessAreaId, setSelectedProcessAreaId] = useState<string | null>(null);
  const [selectedDataObjectId, setSelectedDataObjectId] = useState<string | null>(null);
  const [selectedSystemId, setSelectedSystemId] = useState<string | null>(null);
  const [searchText, setSearchText] = useState('');

  // Detail Dialog State
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [selectedTableId, setSelectedTableId] = useState<string | null>(null);
  const [detailTabValue, setDetailTabValue] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [hasUnsavedGridChanges, setHasUnsavedGridChanges] = useState(false);
  const [showCloseConfirm, setShowCloseConfirm] = useState(false);

  const { mutateAsync: deleteTable, isLoading: isDeleting } = useMutation(deleteUploadedTable);

  // Queries
  const { data: processAreas = [], isLoading: isLoadingProcessAreas } = useQuery(
    ['processAreas'],
    fetchProcessAreas,
    {
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load product teams'));
      }
    }
  );

  const { data: dataObjects = [], isLoading: isLoadingDataObjects } = useQuery(
    ['dataObjects', selectedProcessAreaId],
    () => {
      if (!selectedProcessAreaId) return Promise.resolve([]);
      return fetchDataObjects(selectedProcessAreaId);
    },
    {
      enabled: !!selectedProcessAreaId,
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load data objects'));
      }
    }
  );

  const { data: systems = [], isLoading: isLoadingSystems } = useQuery(
    ['systems'],
    fetchSystems,
    {
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load systems'));
      }
    }
  );

  // Fetch ALL data definitions (no filter) to show tables upfront
  const { data: allDefinitions = [], isLoading: isLoadingAllDefinitions } = useQuery(
    ['allConstructionDefinitions'],
    fetchAllConstructionDefinitions,
    {
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load construction tables'));
      }
    }
  );
  // Get all construction tables with metadata
  const processAreaLookup = useMemo(() => {
    const map = new Map<string, string>();
    processAreas.forEach((area) => map.set(area.id, area.name));
    return map;
  }, [processAreas]);

  const allConstructionTables = useMemo<ConstructionTableRow[]>(() => {
    return allDefinitions.flatMap((def) =>
      (def.tables ?? []).map<ConstructionTableRow>((table) => ({
        ...table,
        dataDefinitionId: def.id,
        dataObjectId: def.dataObjectId,
        processAreaId: def.dataObject?.processAreaId ?? null,
        systemId: def.systemId,
        dataObjectName: def.dataObject?.name ?? null,
        processAreaName:
          def.dataObject?.processArea?.name ??
          (def.dataObject?.processAreaId
            ? processAreaLookup.get(def.dataObject.processAreaId) ?? null
            : null),
        systemName: def.system?.name ?? null
      }))
    );
  }, [allDefinitions, processAreaLookup]);

  // Filter tables by all selected criteria
  const filteredTables = useMemo(() => {
    let result = allConstructionTables;

    // Filter by Product Team if selected
    if (selectedProcessAreaId) {
      result = result.filter(table =>
        table.processAreaId === selectedProcessAreaId
      );
    }

    // Filter by Data Object if selected
    if (selectedDataObjectId) {
      result = result.filter(table =>
        table.dataObjectId === selectedDataObjectId
      );
    }

    // Filter by System if selected
    if (selectedSystemId) {
      result = result.filter(table =>
        table.systemId === selectedSystemId
      );
    }

    // Filter by search text
    if (searchText.trim()) {
      const lowerSearch = searchText.toLowerCase();
      result = result.filter(table =>
        (table.alias?.toLowerCase().includes(lowerSearch) || false) ||
        (table.description?.toLowerCase().includes(lowerSearch) || false)
      );
    }

    return result;
  }, [allConstructionTables, selectedProcessAreaId, selectedDataObjectId, selectedSystemId, searchText]);

  // Selected table details (for detail modal)
  const selectedTableData = useMemo<ConstructionTableRow | null>(() => {
    if (!selectedTableId) return null;
    return allConstructionTables.find((table) => table.id === selectedTableId) ?? null;
  }, [allConstructionTables, selectedTableId]);

  // Queries for detailed table view
  const { data: fields = [], isLoading: isLoadingFields } = useQuery(
    ['constructedFields', selectedTableData?.constructedTableId],
    () => {
      if (!selectedTableData?.constructedTableId) return Promise.resolve([]);
      return fetchConstructedFields(selectedTableData.constructedTableId);
    },
    {
      enabled: !!selectedTableData?.constructedTableId,
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load fields'));
      }
    }
  );

  const { data: rows = [], isLoading: isLoadingRows, refetch: refetchRows } = useQuery(
    ['constructedData', selectedTableData?.constructedTableId],
    () => {
      if (!selectedTableData?.constructedTableId) return Promise.resolve([]);
      return fetchConstructedData(selectedTableData.constructedTableId);
    },
    {
      enabled: !!selectedTableData?.constructedTableId,
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load data'));
      }
    }
  );

  const { data: validationRules = [], isLoading: isLoadingRules } = useQuery(
    ['validationRules', selectedTableData?.constructedTableId],
    () => {
      if (!selectedTableData?.constructedTableId) return Promise.resolve([]);
      return fetchValidationRules(selectedTableData.constructedTableId);
    },
    {
      enabled: !!selectedTableData?.constructedTableId,
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load validation rules'));
      }
    }
  );

  // Get selected objects for display
  const selectedProcessArea = useMemo(
    () => processAreas.find((p) => p.id === selectedProcessAreaId),
    [processAreas, selectedProcessAreaId]
  );

  const selectedDataObject = useMemo(
    () => dataObjects.find((d) => d.id === selectedDataObjectId),
    [dataObjects, selectedDataObjectId]
  );

  const selectedSystem = useMemo(
    () => systems.find((s) => s.id === selectedSystemId),
    [systems, selectedSystemId]
  );

  // Handlers
  const handleOpenTableDetails = useCallback((table: ConstructionTableRow) => {
    setSelectedTableId(table.id);
    setDetailTabValue(0);
    setHasUnsavedGridChanges(false);
    setDetailDialogOpen(true);
  }, []);

  const handleCloseTableDetails = useCallback(() => {
    if (hasUnsavedGridChanges) {
      setShowCloseConfirm(true);
      return;
    }
    setDetailDialogOpen(false);
    setSelectedTableId(null);
    setHasUnsavedGridChanges(false);
  }, [hasUnsavedGridChanges]);

  const handleOpenDeleteDialog = useCallback((table: ConstructionTableRow) => {
    setTablePendingDelete(table);
  }, []);

  const handleCancelDelete = useCallback(() => {
    if (isDeleting) {
      return;
    }
    setTablePendingDelete(null);
  }, [isDeleting]);

  const handleConfirmDelete = useCallback(async () => {
    const tableToRemove = tablePendingDelete;
    if (!tableToRemove?.tableId) {
      return;
    }

    try {
      await deleteTable(tableToRemove.tableId);

      if (selectedTableId === tableToRemove.id) {
        setDetailDialogOpen(false);
        setSelectedTableId(null);
        setHasUnsavedGridChanges(false);
        setShowCloseConfirm(false);
      }

      const friendlyName = tableToRemove.alias || tableToRemove.constructedTableName || 'table';
      toast.showSuccess(`Deleted ${friendlyName}.`);

      setTablePendingDelete(null);

      await queryClient.invalidateQueries(['allConstructionDefinitions']);

      if (tableToRemove.constructedTableId) {
        queryClient.removeQueries(['constructedFields', tableToRemove.constructedTableId]);
        queryClient.removeQueries(['constructedData', tableToRemove.constructedTableId]);
        queryClient.removeQueries(['validationRules', tableToRemove.constructedTableId]);
      }
    } catch (error) {
      toast.showError(getErrorMessage(error, 'Failed to delete table.'));
    }
  }, [
    deleteTable,
    queryClient,
    selectedTableId,
    tablePendingDelete,
    toast
  ]);

  const handleDetailTabChange = (_event: SyntheticEvent, newValue: number) => {
    setDetailTabValue(newValue);
  };

  const handleConfirmCloseDetails = useCallback(() => {
    setShowCloseConfirm(false);
    setDetailDialogOpen(false);
    setSelectedTableId(null);
    setHasUnsavedGridChanges(false);
  }, []);

  const handleCancelCloseDetails = useCallback(() => {
    setShowCloseConfirm(false);
  }, []);

  const handleRefresh = useCallback(async () => {
    setIsRefreshing(true);
    try {
      await refetchRows();
      toast.showSuccess('Data refreshed');
    } finally {
      setIsRefreshing(false);
    }
  }, [refetchRows, toast]);

  const handleProcessAreaChange = useCallback((_event: SyntheticEvent, value: ProcessArea | null) => {
    // Only update Product Team - don't reset other filters
    setSelectedProcessAreaId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
    setHasUnsavedGridChanges(false);
  }, []);

  const handleDataObjectChange = useCallback((_event: SyntheticEvent, value: DataObject | null) => {
    // Only update Data Object - don't reset other filters
    setSelectedDataObjectId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
    setHasUnsavedGridChanges(false);
  }, []);

  const handleSystemChange = useCallback((_event: SyntheticEvent, value: System | null) => {
    // Only update System - don't reset other filters
    setSelectedSystemId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
    setHasUnsavedGridChanges(false);
  }, []);

  const isLoadingDetails = isLoadingFields || isLoadingRows || isLoadingRules;

  return (
    <Box sx={{ p: 3 }}>
      {/* Page Header */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" component="h1" sx={{ mb: 1, fontWeight: 'bold' }}>
          Manage Data
        </Typography>
        <Typography variant="body2" color="textSecondary">
          Manage and edit construction table data with validation rules
        </Typography>
      </Box>

      {/* Filters Section */}
      <Paper sx={{ p: 3, mb: 3, backgroundColor: alpha(theme.palette.primary.main, 0.02), border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}` }}>
        <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600, textTransform: 'uppercase', color: 'textSecondary' }}>
          Filters
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={6} md={3}>
            <Autocomplete
              fullWidth
              options={processAreas}
              getOptionLabel={(option) => option.name}
              value={selectedProcessArea ?? null}
              onChange={handleProcessAreaChange}
              loading={isLoadingProcessAreas}
              disabled={isLoadingProcessAreas}
              size="small"
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Product Team"
                  placeholder="Select..."
                  required
                />
              )}
            />
          </Grid>

          <Grid item xs={12} sm={6} md={3}>
            <Autocomplete
              fullWidth
              options={dataObjects}
              getOptionLabel={(option) => option.name}
              value={selectedDataObject ?? null}
              onChange={handleDataObjectChange}
              loading={isLoadingDataObjects}
              disabled={isLoadingDataObjects || !selectedProcessArea}
              size="small"
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Data Object"
                  placeholder="Select..."
                  required
                />
              )}
            />
          </Grid>

          <Grid item xs={12} sm={6} md={3}>
            <Autocomplete
              fullWidth
              options={systems}
              getOptionLabel={(option) => option.name}
              value={selectedSystem ?? null}
              onChange={handleSystemChange}
              loading={isLoadingSystems}
              disabled={isLoadingSystems}
              size="small"
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="System"
                  placeholder="Select..."
                  required
                />
              )}
            />
          </Grid>

          <Grid item xs={12} sm={6} md={3}>
            <TextField
              fullWidth
              size="small"
              placeholder="Search tables..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              InputProps={{
                startAdornment: <SearchIcon sx={{ mr: 1, color: 'action.disabled' }} />
              }}
            />
          </Grid>
        </Grid>
      </Paper>

      {/* Tables List */}
      <Paper>
        {isLoadingAllDefinitions ? (
          <LinearProgress />
        ) : filteredTables.length === 0 ? (
          <Box sx={{ p: 4, textAlign: 'center' }}>
            <Typography color="textSecondary" sx={{ mb: 2 }}>
              No construction tables found
            </Typography>
            <Typography variant="caption" color="textSecondary">
              {allConstructionTables.length === 0
                ? 'No construction tables available'
                : 'Try adjusting your search filters'}
            </Typography>
          </Box>
        ) : (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow sx={{ backgroundColor: alpha(theme.palette.primary.main, 0.08) }}>
                  <TableCell sx={{ fontWeight: 600 }}>Table Name</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>Description</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>Product Team</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>Data Object</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>System</TableCell>
                  <TableCell sx={{ fontWeight: 600 }} align="center">
                    Status
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }} align="right">
                    Actions
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredTables.map((table) => (
                  <TableRow
                    key={table.id}
                    hover
                    sx={{
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.05)
                      }
                    }}
                  >
                    <TableCell sx={{ fontWeight: 500 }}>
                      {table.constructedTableId ? (
                        <Button
                          variant="text"
                          color="primary"
                          size="small"
                          sx={{
                            fontWeight: 600,
                            textTransform: 'none',
                            p: 0,
                            minWidth: 0,
                            justifyContent: 'flex-start',
                            '&:hover': {
                              textDecoration: 'underline',
                              backgroundColor: 'transparent'
                            }
                          }}
                          onClick={() => handleOpenTableDetails(table)}
                        >
                          {table.alias || table.constructedTableName || '—'}
                        </Button>
                      ) : (
                        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                          {table.alias || table.constructedTableName || '—'}
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="textSecondary" sx={{ maxWidth: 300 }}>
                        {table.description || '—'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="textSecondary">
                        {table.processAreaName || '—'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="textSecondary">
                        {table.dataObjectName || '—'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="textSecondary">
                        {table.systemName || '—'}
                      </Typography>
                    </TableCell>
                    <TableCell align="center">
                      <Chip
                        label={table.constructedTableStatus || 'draft'}
                        size="small"
                        variant="outlined"
                        color={
                          table.constructedTableStatus === 'approved'
                            ? 'success'
                            : table.constructedTableStatus === 'rejected'
                            ? 'error'
                            : 'default'
                        }
                      />
                    </TableCell>
                    <TableCell align="right">
                      <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                        {canManage && table.tableId && (
                          <Tooltip title="Delete table">
                            <span>
                              <IconButton
                                size="small"
                                color="error"
                                onClick={() => handleOpenDeleteDialog(table)}
                                disabled={
                                  isDeleting && tablePendingDelete?.id === table.id
                                }
                              >
                                <DeleteOutlineIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        )}
                      </Stack>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Paper>

      {/* Table Details Modal */}
      <Dialog
        open={detailDialogOpen}
        onClose={() => {
          // Do nothing - only allow close button to close this modal
        }}
        maxWidth={false}
        fullWidth={true}
        disableEscapeKeyDown={true}
        PaperProps={{
          sx: {
            width: '95vw',
            height: '95vh',
            maxHeight: '95vh',
            m: 'auto',
            display: 'flex',
            flexDirection: 'column'
          }
        }}
      >
        {selectedTableData && (
          <>
            <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pb: 1 }}>
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {selectedTableData.alias || 'Construction Table'}
                </Typography>
                <Typography variant="caption" color="textSecondary">
                  {selectedTableData.description}
                </Typography>
              </Box>
              <Stack direction="row" spacing={1}>
                <IconButton
                  size="small"
                  onClick={handleRefresh}
                  disabled={isRefreshing}
                  title="Refresh data"
                >
                  <RefreshIcon fontSize="small" />
                </IconButton>
                <IconButton
                  size="small"
                  onClick={handleCloseTableDetails}
                  title="Close"
                >
                  <CloseIcon fontSize="small" />
                </IconButton>
              </Stack>
            </DialogTitle>
            <Divider />

            <DialogContent sx={{ p: 0, flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
              <MuiTabs
                value={detailTabValue}
                onChange={handleDetailTabChange}
                sx={{ borderBottom: 1, borderColor: 'divider', px: 4, pt: 2, position: 'sticky', top: 0, zIndex: 10, backgroundColor: 'background.paper' }}
              >
                <MuiTab label="Data Entry" id="table-details-tab-0" />
                <MuiTab label="Validation Rules" id="table-details-tab-1" />
              </MuiTabs>

              <TabPanel value={detailTabValue} index={0} sx={{ flex: 1, overflow: 'hidden', p: 4, pt: 3 }}>
                {isLoadingDetails ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                    <Typography color="textSecondary">Loading...</Typography>
                  </Box>
                ) : (
                  <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden', p: 2.5, pt: 1, pb: 3 }}>
                    {selectedTableData.constructedTableId && (
                      <ConstructedDataGrid
                        constructedTableId={selectedTableData.constructedTableId}
                        fields={fields}
                        rows={rows}
                        onDataChange={handleRefresh}
                        onDirtyStateChange={setHasUnsavedGridChanges}
                      />
                    )}
                  </Box>
                )}
              </TabPanel>

              <TabPanel value={detailTabValue} index={1} sx={{ flex: 1, overflow: 'auto', p: 4, pt: 3 }}>
                {isLoadingDetails ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                    <Typography color="textSecondary">Loading...</Typography>
                  </Box>
                ) : (
                  selectedTableData.constructedTableId && (
                    <ValidationRulesManager
                      constructedTableId={selectedTableData.constructedTableId}
                      fields={fields}
                      validationRules={validationRules}
                      onRulesChange={() => {
                        queryClient.refetchQueries(['validationRules', selectedTableData.constructedTableId]);
                      }}
                    />
                  )
                )}
              </TabPanel>
            </DialogContent>
          </>
        )}
      </Dialog>

      <Dialog
        open={Boolean(tablePendingDelete)}
        onClose={handleCancelDelete}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 3,
            overflow: 'hidden'
          }
        }}
      >
        <DialogTitle sx={{ pb: 0 }}>
          <Stack direction="row" spacing={2} alignItems="center">
            <Box
              sx={{
                width: 48,
                height: 48,
                borderRadius: '50%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                backgroundColor: alpha(theme.palette.error.main, 0.12),
                color: theme.palette.error.main
              }}
            >
              <WarningAmberIcon />
            </Box>
            <Box>
              <Typography variant="h6" sx={{ fontWeight: 600 }}>
                Delete constructed table?
              </Typography>
              <Typography variant="body2" color="text.secondary">
                This will drop the warehouse table and remove it from every linked data definition. The action cannot be undone.
              </Typography>
            </Box>
          </Stack>
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {tablePendingDelete && (
            <Stack spacing={2}>
              <Paper
                variant="outlined"
                sx={{
                  p: 2,
                  borderRadius: 2,
                  borderColor: alpha(theme.palette.error.main, 0.26),
                  backgroundColor: alpha(theme.palette.error.main, 0.04)
                }}
              >
                <Stack spacing={0.75}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                    {tablePendingDelete.alias || tablePendingDelete.constructedTableName || 'Construction Table'}
                  </Typography>
                  {tablePendingDelete.description && (
                    <Typography variant="body2" color="text.secondary">
                      {tablePendingDelete.description}
                    </Typography>
                  )}
                  <Typography variant="body2" color="text.secondary">
                    Product Team: {tablePendingDelete.processAreaName || '—'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Data Object: {tablePendingDelete.dataObjectName || '—'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    System: {tablePendingDelete.systemName || '—'}
                  </Typography>
                </Stack>
              </Paper>
              <Typography variant="caption" color="text.secondary">
                All constructed rows, validation rules, and warehouse data for this table will be deleted.
              </Typography>
            </Stack>
          )}
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button variant="outlined" onClick={handleCancelDelete} disabled={isDeleting}>
            Cancel
          </Button>
          <LoadingButton
            onClick={handleConfirmDelete}
            color="error"
            variant="contained"
            loading={isDeleting}
            disabled={!tablePendingDelete?.tableId}
          >
            Delete table
          </LoadingButton>
        </DialogActions>
      </Dialog>

      <Dialog open={showCloseConfirm} onClose={handleCancelCloseDetails} maxWidth="xs" fullWidth>
        <DialogTitle>Discard unsaved changes?</DialogTitle>
        <DialogContent>
          <Typography variant="body2">
            You have unsaved changes in this table. Closing will discard them. Are you sure you want to
            close?
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelCloseDetails}>Stay on Page</Button>
          <Button color="error" onClick={handleConfirmCloseDetails} variant="contained">
            Discard and Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DataConstructionPage;
