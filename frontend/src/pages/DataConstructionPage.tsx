import { useCallback, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useQuery, useQueryClient } from 'react-query';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Card,
  CardContent,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  Paper,
  Stack,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Tab as MuiTab,
  Tabs as MuiTabs,
  TextField,
  Typography,
  IconButton,
  Chip,
  LinearProgress,
  Tooltip
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import EditIcon from '@mui/icons-material/Edit';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';

import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';
import {
  fetchProcessAreas,
  fetchDataObjects,
  fetchSystems,
  fetchConstructionDefinitions,
  fetchAllConstructionDefinitions,
  fetchConstructedData,
  fetchConstructedFields,
  fetchValidationRules,
  batchSaveConstructedData,
  ProcessArea,
  DataObject,
  System,
  DataDefinition,
  DataDefinitionTable,
  ConstructedData,
  ConstructedField,
  ConstructedDataValidationRule
} from '../services/constructedDataService';
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
  children?: React.ReactNode;
  index: number;
  value: number;
  sx?: any;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, sx, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`table-details-tabpanel-${index}`}
      aria-labelledby={`table-details-tab-${index}`}
      {...other}
      style={{
        display: value === index ? 'flex' : 'none',
        flexDirection: 'column',
        flex: 1,
        overflow: 'auto',
        ...((sx as any) || {})
      }}
    >
      {value === index && <Box sx={{ pt: 3 }}>{children}</Box>}
    </div>
  );
}

const DataConstructionPage = () => {
  const theme = useTheme();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin') || hasRole('viewer');
  const toast = useToast();
  const queryClient = useQueryClient();

  // Filter State
  const [selectedProcessAreaId, setSelectedProcessAreaId] = useState<string | null>(null);
  const [selectedDataObjectId, setSelectedDataObjectId] = useState<string | null>(null);
  const [selectedSystemId, setSelectedSystemId] = useState<string | null>(null);
  const [searchText, setSearchText] = useState('');

  // Detail Dialog State
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [selectedTableId, setSelectedTableId] = useState<string | null>(null);
  const [detailTabValue, setDetailTabValue] = useState(0);
  const [tablePaginationPage, setTablePaginationPage] = useState(0);
  const [tablePaginationRowsPerPage, setTablePaginationRowsPerPage] = useState(10);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Queries
  const { data: processAreas = [], isLoading: isLoadingProcessAreas } = useQuery(
    ['processAreas'],
    fetchProcessAreas,
    {
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Failed to load process areas'));
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

  // Fetch filtered definitions (only when filters are selected)
  const { data: definitions = [], isLoading: isLoadingDefinitions } = useQuery(
    ['constructionDefinitions', selectedDataObjectId, selectedSystemId],
    () => {
      if (!selectedDataObjectId || !selectedSystemId) return Promise.resolve([]);
      return fetchConstructionDefinitions(selectedDataObjectId, selectedSystemId);
    },
    {
      enabled: !!selectedDataObjectId && !!selectedSystemId,
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

  const allConstructionTables = useMemo(() => {
    return allDefinitions.flatMap((def: any) =>
      (def.tables || []).map((table: any) => ({
        ...table,
        dataDefinitionId: def.id,
        dataObjectId: def.dataObjectId,
        processAreaId: def.dataObject?.processAreaId,
        systemId: def.systemId,
        dataObjectName: def.dataObject?.name,
        processAreaName:
          def.dataObject?.processArea?.name ??
          (def.dataObject?.processAreaId
            ? processAreaLookup.get(def.dataObject.processAreaId)
            : undefined),
        systemName: def.system?.name
      }))
    );
  }, [allDefinitions, processAreaLookup]);

  // Filter tables by all selected criteria
  const filteredTables = useMemo(() => {
    let result = allConstructionTables;

    // Filter by Process Area if selected
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
  const selectedTableData = useMemo(() => {
    if (!selectedTableId) return null;
    return allConstructionTables.find(t => t.id === selectedTableId);
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
  const handleOpenTableDetails = useCallback((table: DataDefinitionTable) => {
    setSelectedTableId(table.id);
    setDetailTabValue(0);
    setTablePaginationPage(0);
    setDetailDialogOpen(true);
  }, []);

  const handleCloseTableDetails = useCallback(() => {
    setDetailDialogOpen(false);
    setSelectedTableId(null);
    setTablePaginationRowsPerPage(10);
  }, []);

  const handleDetailTabChange = (_: React.SyntheticEvent, newValue: number) => {
    setDetailTabValue(newValue);
  };

  const handleTablePaginationChange = (event: unknown, newPage: number) => {
    setTablePaginationPage(newPage);
  };

  const handleRowsPerPageChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setTablePaginationRowsPerPage(parseInt(event.target.value, 10));
    setTablePaginationPage(0);
  };

  const handleRefresh = useCallback(async () => {
    setIsRefreshing(true);
    try {
      await refetchRows();
      toast.showSuccess('Data refreshed');
    } finally {
      setIsRefreshing(false);
    }
  }, [refetchRows, toast]);

  const handleProcessAreaChange = useCallback((_: any, value: ProcessArea | null) => {
    // Only update Process Area - don't reset other filters
    setSelectedProcessAreaId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

  const handleDataObjectChange = useCallback((_: any, value: DataObject | null) => {
    // Only update Data Object - don't reset other filters
    setSelectedDataObjectId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

  const handleSystemChange = useCallback((_: any, value: System | null) => {
    // Only update System - don't reset other filters
    setSelectedSystemId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

  const isLoadingTables = isLoadingAllDefinitions;
  const isLoadingDetails = isLoadingFields || isLoadingRows || isLoadingRules;

  return (
    <Box sx={{ p: 3 }}>
      {/* Page Header */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" component="h1" sx={{ mb: 1, fontWeight: 'bold' }}>
          Data Construction
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
                  label="Process Area"
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
                  <TableCell sx={{ fontWeight: 600 }}>Process Area</TableCell>
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
                      {table.alias || 'Unnamed Table'}
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
                        <Tooltip title="Open table">
                          <IconButton
                            size="small"
                            onClick={() => handleOpenTableDetails(table)}
                            disabled={!table.constructedTableId}
                          >
                            <OpenInNewIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
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
        onClose={handleCloseTableDetails}
        maxWidth={false}
        fullWidth={true}
        disableEscapeKeyDown={true}
        onBackdropClick={(event) => {
          // Prevent accidental closure by clicking outside the dialog
          event.preventDefault();
        }}
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
                sx={{ borderBottom: 1, borderColor: 'divider', px: 3 }}
              >
                <MuiTab label="Data Entry" id="table-details-tab-0" />
                <MuiTab label="Validation Rules" id="table-details-tab-1" />
              </MuiTabs>

              <TabPanel value={detailTabValue} index={0} sx={{ flex: 1, overflow: 'auto', px: 3 }}>
                {isLoadingDetails ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                    <Typography color="textSecondary">Loading...</Typography>
                  </Box>
                ) : (
                  <>
                    {selectedTableData.constructedTableId && (
                      <ConstructedDataGrid
                        constructedTableId={selectedTableData.constructedTableId}
                        fields={fields}
                        rows={rows}
                        onDataChange={handleRefresh}
                      />
                    )}
                  </>
                )}
              </TabPanel>

              <TabPanel value={detailTabValue} index={1} sx={{ flex: 1, overflow: 'auto', px: 3 }}>
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
    </Box>
  );
};

export default DataConstructionPage;
