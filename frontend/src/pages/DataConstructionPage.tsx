import { ReactNode, SyntheticEvent, useCallback, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useQuery, useQueryClient } from 'react-query';
import {
  Autocomplete,
  Box,
  Dialog,
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
  Tooltip
} from '@mui/material';
import { SxProps, Theme, alpha, useTheme } from '@mui/material/styles';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';

import { useToast } from '../hooks/useToast';
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
  return (
    <Box
      role="tabpanel"
      hidden={value !== index}
      id={`table-details-tabpanel-${index}`}
      aria-labelledby={`table-details-tab-${index}`}
      sx={[
        {
          display: value === index ? 'flex' : 'none',
          flexDirection: 'column',
          flex: 1,
          overflow: 'auto'
        },
        ...(Array.isArray(sx) ? sx : sx ? [sx] : [])
      ]}
      {...other}
    >
      {value === index && <Box sx={{ pt: 3 }}>{children}</Box>}
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
    setDetailDialogOpen(true);
  }, []);

  const handleCloseTableDetails = useCallback(() => {
    setDetailDialogOpen(false);
    setSelectedTableId(null);
  }, []);

  const handleDetailTabChange = (_event: SyntheticEvent, newValue: number) => {
    setDetailTabValue(newValue);
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

  const handleProcessAreaChange = useCallback((_event: SyntheticEvent, value: ProcessArea | null) => {
    // Only update Process Area - don't reset other filters
    setSelectedProcessAreaId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

  const handleDataObjectChange = useCallback((_event: SyntheticEvent, value: DataObject | null) => {
    // Only update Data Object - don't reset other filters
    setSelectedDataObjectId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

  const handleSystemChange = useCallback((_event: SyntheticEvent, value: System | null) => {
    // Only update System - don't reset other filters
    setSelectedSystemId(value?.id ?? null);
    // Close modal and clear search when changing filters
    setSelectedTableId(null);
    setDetailDialogOpen(false);
  }, []);

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

              <TabPanel value={detailTabValue} index={0} sx={{ flex: 1, overflow: 'auto', p: 4, pt: 3 }}>
                {isLoadingDetails ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                    <Typography color="textSecondary">Loading...</Typography>
                  </Box>
                ) : (
                  <Box sx={{ flex: 1, p: 2.5, pt: 1, pb: 3 }}>
                    {selectedTableData.constructedTableId && (
                      <ConstructedDataGrid
                        constructedTableId={selectedTableData.constructedTableId}
                        fields={fields}
                        rows={rows}
                        onDataChange={handleRefresh}
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
    </Box>
  );
};

export default DataConstructionPage;
