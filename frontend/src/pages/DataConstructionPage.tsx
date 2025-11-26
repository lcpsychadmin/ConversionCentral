import { ReactNode, SyntheticEvent, useCallback, useEffect, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { LoadingButton } from '@mui/lab';
import {
  Autocomplete,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  IconButton,
  InputAdornment,
  Paper,
  Stack,
  Tab as MuiTab,
  Tabs as MuiTabs,
  TextField,
  Typography
} from '@mui/material';
import { SxProps, Theme, alpha, useTheme } from '@mui/material/styles';
import {
  DataGrid,
  GridActionsCellItem,
  GridColDef,
  GridRenderCellParams,
  GridRowParams
} from '@mui/x-data-grid';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

import { useToast } from '../hooks/useToast';
import PageHeader from '../components/common/PageHeader';
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
import { getPanelSurface, getSectionSurface } from '../theme/surfaceStyles';
import { getDataGridStyles } from '../utils/tableStyles';

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

const MANAGED_DATABRICKS_PHYSICAL_NAME = 'databricks_warehouse';

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
  const isDarkMode = theme.palette.mode === 'dark';

  const filterPanelStyles = useMemo(() => {
    const surface = getSectionSurface(theme, { shadow: 'subtle' });
    return {
      background: surface.background,
      border: surface.border,
      boxShadow: surface.boxShadow,
      borderRadius: 3
    } as const;
  }, [theme]);

  const filterInputStyles = useMemo(() => {
    const labelColor = isDarkMode
      ? alpha(theme.palette.common.white, 0.88)
      : alpha(theme.palette.text.primary, 0.88);
    const inputBackground = isDarkMode
      ? alpha(theme.palette.background.paper, 0.72)
      : theme.palette.common.white;
    const hoverBackground = isDarkMode
      ? alpha(theme.palette.primary.main, 0.16)
      : alpha(theme.palette.primary.main, 0.08);
    const focusRing = alpha(theme.palette.primary.main, isDarkMode ? 0.4 : 0.25);

    return {
      '& .MuiOutlinedInput-root': {
        backgroundColor: inputBackground,
        borderRadius: 2,
        transition: 'background-color 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease',
        '& fieldset': {
          borderColor: alpha(theme.palette.primary.main, isDarkMode ? 0.45 : 0.25)
        },
        '&:hover fieldset': {
          borderColor: alpha(theme.palette.primary.main, isDarkMode ? 0.65 : 0.45)
        },
        '&:hover': {
          backgroundColor: hoverBackground
        },
        '&.Mui-focused fieldset': {
          borderColor: theme.palette.primary.main
        },
        '&.Mui-focused': {
          boxShadow: `0 0 0 3px ${focusRing}`,
          backgroundColor: isDarkMode
            ? alpha(theme.palette.background.paper, 0.85)
            : theme.palette.common.white
        },
        '&.Mui-disabled': {
          opacity: 0.85,
          backgroundColor: isDarkMode
            ? alpha(theme.palette.background.paper, 0.4)
            : alpha(theme.palette.action.disabledBackground, 0.4)
        }
      },
      '& .MuiOutlinedInput-input': {
        fontSize: '0.95rem'
      },
      '& .MuiInputLabel-root': {
        color: labelColor,
        fontWeight: 600,
        fontSize: '1rem',
        '&.MuiInputLabel-shrink': {
          fontSize: '0.85rem'
        }
      }
    } as const;
  }, [isDarkMode, theme]);

  const filterTitleColor = isDarkMode
    ? alpha(theme.palette.common.white, 0.9)
    : theme.palette.text.primary;

  const tablePanelStyles = useMemo(() => {
    const surface = getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' });
    return {
      background: surface.background,
      border: surface.border,
      boxShadow: surface.boxShadow,
      borderRadius: 3
    } as const;
  }, [isDarkMode, theme]);

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

  const applicationOptions = useMemo(
    () => systems.filter((system) => system.physicalName !== MANAGED_DATABRICKS_PHYSICAL_NAME),
    [systems]
  );

  useEffect(() => {
    if (selectedSystemId && !applicationOptions.some((system) => system.id === selectedSystemId)) {
      setSelectedSystemId(null);
    }
  }, [applicationOptions, selectedSystemId]);

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

  const selectedApplication = useMemo(
    () => applicationOptions.find((s) => s.id === selectedSystemId) ?? null,
    [applicationOptions, selectedSystemId]
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

  const tableColumns = useMemo<GridColDef<ConstructionTableRow>[]>(() => {
    const columns: GridColDef<ConstructionTableRow>[] = [
      {
        field: 'displayName',
        headerName: 'Table Name',
        flex: 1.2,
        minWidth: 220,
        sortable: true,
  renderCell: (params: GridRenderCellParams<ConstructionTableRow>) => {
          const { row } = params;
          const label = row.alias || row.constructedTableName || '—';

          if (row.constructedTableId) {
            return (
              <Button
                variant="text"
                color="primary"
                onClick={(event) => {
                  event.stopPropagation();
                  handleOpenTableDetails(row);
                }}
                sx={{
                  fontWeight: 700,
                  textTransform: 'none',
                  px: 0,
                  justifyContent: 'flex-start',
                  minWidth: 0,
                  '&:hover': {
                    textDecoration: 'underline'
                  }
                }}
              >
                {label}
              </Button>
            );
          }

          return (
            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
              {label}
            </Typography>
          );
        }
      },
      {
        field: 'description',
        headerName: 'Description',
        flex: 1.4,
        minWidth: 260,
  renderCell: (params: GridRenderCellParams<ConstructionTableRow>) => (
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{
              display: 'block',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap'
            }}
          >
            {params.row.description || '—'}
          </Typography>
        )
      },
      {
        field: 'processAreaName',
        headerName: 'Process Area',
        flex: 1,
        minWidth: 160,
        valueGetter: ({ row }) => row.processAreaName || '—'
      },
      {
        field: 'dataObjectName',
        headerName: 'Data Object',
        flex: 1,
        minWidth: 160,
        valueGetter: ({ row }) => row.dataObjectName || '—'
      },
      {
        field: 'systemName',
        headerName: 'System',
        flex: 0.9,
        minWidth: 140,
        valueGetter: ({ row }) => row.systemName || '—'
      },
      {
        field: 'constructedTableStatus',
        headerName: 'Status',
        flex: 0.6,
        minWidth: 120,
  renderCell: (params: GridRenderCellParams<ConstructionTableRow>) => {
          const status = params.row.constructedTableStatus || 'draft';
          const color =
            status === 'approved'
              ? 'success'
              : status === 'rejected'
              ? 'error'
              : 'default';

          return (
            <Chip
              label={status}
              color={color}
              size="small"
              variant={color === 'default' ? 'outlined' : 'filled'}
              sx={{ textTransform: 'capitalize', fontWeight: 600 }}
            />
          );
        }
      }
    ];

    if (canManage) {
      columns.push({
        field: 'actions',
        type: 'actions',
        headerName: 'Actions',
        flex: 0.4,
        minWidth: 100,
        getActions: (params: GridRowParams<ConstructionTableRow>) => {
          const row = params.row;

          if (!row.tableId) {
            return [];
          }

          return [
            <GridActionsCellItem
              key="delete"
              icon={<DeleteOutlineIcon fontSize="small" />}
              label="Delete table"
              onClick={(event) => {
                event.stopPropagation();
                handleOpenDeleteDialog(row);
              }}
              disabled={isDeleting && tablePendingDelete?.id === row.id}
              showInMenu
            />
          ];
        }
      });
    }

    return columns;
  }, [canManage, handleOpenDeleteDialog, handleOpenTableDetails, isDeleting, tablePendingDelete?.id]);

  const NoRowsOverlay = () => (
    <Stack alignItems="center" justifyContent="center" spacing={1.5} sx={{ py: 6 }}>
      <Typography color="text.secondary" sx={{ fontWeight: 600 }}>
        No construction tables found
      </Typography>
      <Typography variant="caption" color="text.secondary">
        {allConstructionTables.length === 0
          ? 'No construction tables available'
          : 'Try adjusting your search filters'}
      </Typography>
    </Stack>
  );

  const handleProcessAreaChange = useCallback((_event: SyntheticEvent, value: ProcessArea | null) => {
    // Only update Process Area - don't reset other filters
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
      <PageHeader
        title="Manage Data"
        subtitle="Manage and edit construction table data with validation rules"
      />

      {/* Filters Section */}
      <Paper elevation={0} sx={{ p: 3, mb: 3, ...filterPanelStyles }}>
        <Typography
          variant="subtitle1"
          sx={{
            fontWeight: 700,
            color: filterTitleColor,
            mb: 2.5,
            letterSpacing: 0.5,
            fontSize: '1.05rem'
          }}
        >
          Filter by Process Area, Data Object & Application
        </Typography>
        <Grid container spacing={2.5}>
          <Grid item xs={12} md={6} lg={3}>
            <Autocomplete
              fullWidth
              options={processAreas}
              getOptionLabel={(option) => option.name}
              value={selectedProcessArea ?? null}
              onChange={handleProcessAreaChange}
              loading={isLoadingProcessAreas}
              disabled={isLoadingProcessAreas}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Process Area"
                  placeholder={isLoadingProcessAreas ? 'Loading...' : 'Select process area'}
                  sx={filterInputStyles}
                />
              )}
            />
          </Grid>

          <Grid item xs={12} md={6} lg={3}>
            <Autocomplete
              fullWidth
              options={dataObjects}
              getOptionLabel={(option) => option.name}
              value={selectedDataObject ?? null}
              onChange={handleDataObjectChange}
              loading={isLoadingDataObjects}
              disabled={isLoadingDataObjects || !selectedProcessArea}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Data Object"
                  placeholder={isLoadingDataObjects ? 'Loading...' : 'Select data object'}
                  sx={filterInputStyles}
                />
              )}
            />
          </Grid>

          <Grid item xs={12} md={6} lg={3}>
            <Autocomplete
              fullWidth
              options={applicationOptions}
              getOptionLabel={(option) => option.name}
              value={selectedApplication}
              onChange={handleSystemChange}
              loading={isLoadingSystems}
              disabled={isLoadingSystems}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Application"
                  placeholder={isLoadingSystems ? 'Loading...' : 'Select application'}
                  sx={filterInputStyles}
                />
              )}
            />
          </Grid>

          <Grid item xs={12} md={6} lg={3}>
            <TextField
              fullWidth
              label="Search tables"
              placeholder="Search tables"
              value={searchText}
              onChange={(event) => setSearchText(event.target.value)}
              sx={filterInputStyles}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon sx={{ color: alpha(theme.palette.text.primary, 0.55) }} />
                  </InputAdornment>
                )
              }}
            />
          </Grid>
        </Grid>
      </Paper>

      {/* Tables List */}
      <Paper elevation={0} sx={{ p: 3, mb: 3, ...tablePanelStyles }}>
        <Box sx={{ height: 540, width: '100%' }}>
          <DataGrid
            rows={filteredTables}
            columns={tableColumns}
            getRowId={(row) => row.id}
            disableRowSelectionOnClick
            hideFooter
            loading={isLoadingAllDefinitions}
            onRowDoubleClick={(params) => {
              const row = params.row as ConstructionTableRow;
              if (row.constructedTableId) {
                handleOpenTableDetails(row);
              }
            }}
            slots={{ noRowsOverlay: NoRowsOverlay }}
            sx={getDataGridStyles(theme)}
          />
        </Box>
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
                    Process Area: {tablePendingDelete.processAreaName || '—'}
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
