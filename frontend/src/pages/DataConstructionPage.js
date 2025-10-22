import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useQuery, useQueryClient } from 'react-query';
import { Autocomplete, Box, Button, Dialog, DialogContent, DialogTitle, Divider, Grid, Paper, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tab as MuiTab, Tabs as MuiTabs, TextField, Typography, IconButton, Chip, LinearProgress, Tooltip } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import AddIcon from '@mui/icons-material/Add';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';
import { fetchProcessAreas, fetchDataObjects, fetchSystems, fetchConstructionDefinitions, fetchAllConstructionDefinitions, fetchConstructedData, fetchConstructedFields, fetchValidationRules, batchSaveConstructedData } from '../services/constructedDataService';
import ConstructedDataGrid from '../components/data-construction/ConstructedDataGrid';
import ValidationRulesManager from '../components/data-construction/ValidationRulesManager';
import AddRowDialog from '../components/data-construction/AddRowDialog';
const getErrorMessage = (error, fallback) => {
    if (error instanceof AxiosError) {
        return error.response?.data?.detail ?? error.message;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
function TabPanel(props) {
    const { children, value, index, ...other } = props;
    return (_jsx("div", { role: "tabpanel", hidden: value !== index, id: `table-details-tabpanel-${index}`, "aria-labelledby": `table-details-tab-${index}`, ...other, children: value === index && _jsx(Box, { sx: { pt: 3 }, children: children }) }));
}
const DataConstructionPage = () => {
    const theme = useTheme();
    const { hasRole } = useAuth();
    const canManage = hasRole('admin') || hasRole('viewer');
    const toast = useToast();
    const queryClient = useQueryClient();
    // Filter State
    const [selectedProcessAreaId, setSelectedProcessAreaId] = useState(null);
    const [selectedDataObjectId, setSelectedDataObjectId] = useState(null);
    const [selectedSystemId, setSelectedSystemId] = useState(null);
    const [searchText, setSearchText] = useState('');
    // Detail Dialog State
    const [detailDialogOpen, setDetailDialogOpen] = useState(false);
    const [selectedTableId, setSelectedTableId] = useState(null);
    const [detailTabValue, setDetailTabValue] = useState(0);
    const [tablePaginationPage, setTablePaginationPage] = useState(0);
    const [tablePaginationRowsPerPage, setTablePaginationRowsPerPage] = useState(10);
    const [addRowDialogOpen, setAddRowDialogOpen] = useState(false);
    const [isRefreshing, setIsRefreshing] = useState(false);
    // Queries
    const { data: processAreas = [], isLoading: isLoadingProcessAreas } = useQuery(['processAreas'], fetchProcessAreas, {
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load process areas'));
        }
    });
    const { data: dataObjects = [], isLoading: isLoadingDataObjects } = useQuery(['dataObjects', selectedProcessAreaId], () => {
        if (!selectedProcessAreaId)
            return Promise.resolve([]);
        return fetchDataObjects(selectedProcessAreaId);
    }, {
        enabled: !!selectedProcessAreaId,
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load data objects'));
        }
    });
    const { data: systems = [], isLoading: isLoadingSystems } = useQuery(['systems'], fetchSystems, {
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load systems'));
        }
    });
    // Fetch ALL data definitions (no filter) to show tables upfront
    const { data: allDefinitions = [], isLoading: isLoadingAllDefinitions } = useQuery(['allConstructionDefinitions'], fetchAllConstructionDefinitions, {
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load construction tables'));
        }
    });
    // Fetch filtered definitions (only when filters are selected)
    const { data: definitions = [], isLoading: isLoadingDefinitions } = useQuery(['constructionDefinitions', selectedDataObjectId, selectedSystemId], () => {
        if (!selectedDataObjectId || !selectedSystemId)
            return Promise.resolve([]);
        return fetchConstructionDefinitions(selectedDataObjectId, selectedSystemId);
    }, {
        enabled: !!selectedDataObjectId && !!selectedSystemId,
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load construction tables'));
        }
    });
    // Get all construction tables with metadata
    const processAreaLookup = useMemo(() => {
        const map = new Map();
        processAreas.forEach((area) => map.set(area.id, area.name));
        return map;
    }, [processAreas]);
    const allConstructionTables = useMemo(() => {
        return allDefinitions.flatMap((def) => (def.tables || []).map((table) => ({
            ...table,
            dataDefinitionId: def.id,
            dataObjectId: def.dataObjectId,
            processAreaId: def.dataObject?.processAreaId,
            systemId: def.systemId,
            dataObjectName: def.dataObject?.name,
            processAreaName: def.dataObject?.processArea?.name ??
                (def.dataObject?.processAreaId
                    ? processAreaLookup.get(def.dataObject.processAreaId)
                    : undefined),
            systemName: def.system?.name
        })));
    }, [allDefinitions, processAreaLookup]);
    // Filter tables by all selected criteria
    const filteredTables = useMemo(() => {
        let result = allConstructionTables;
        // Filter by Process Area if selected
        if (selectedProcessAreaId) {
            result = result.filter(table => table.processAreaId === selectedProcessAreaId);
        }
        // Filter by Data Object if selected
        if (selectedDataObjectId) {
            result = result.filter(table => table.dataObjectId === selectedDataObjectId);
        }
        // Filter by System if selected
        if (selectedSystemId) {
            result = result.filter(table => table.systemId === selectedSystemId);
        }
        // Filter by search text
        if (searchText.trim()) {
            const lowerSearch = searchText.toLowerCase();
            result = result.filter(table => (table.alias?.toLowerCase().includes(lowerSearch) || false) ||
                (table.description?.toLowerCase().includes(lowerSearch) || false));
        }
        return result;
    }, [allConstructionTables, selectedProcessAreaId, selectedDataObjectId, selectedSystemId, searchText]);
    // Selected table details (for detail modal)
    const selectedTableData = useMemo(() => {
        if (!selectedTableId)
            return null;
        return allConstructionTables.find(t => t.id === selectedTableId);
    }, [allConstructionTables, selectedTableId]);
    // Queries for detailed table view
    const { data: fields = [], isLoading: isLoadingFields } = useQuery(['constructedFields', selectedTableData?.constructedTableId], () => {
        if (!selectedTableData?.constructedTableId)
            return Promise.resolve([]);
        return fetchConstructedFields(selectedTableData.constructedTableId);
    }, {
        enabled: !!selectedTableData?.constructedTableId,
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load fields'));
        }
    });
    const { data: rows = [], isLoading: isLoadingRows, refetch: refetchRows } = useQuery(['constructedData', selectedTableData?.constructedTableId], () => {
        if (!selectedTableData?.constructedTableId)
            return Promise.resolve([]);
        return fetchConstructedData(selectedTableData.constructedTableId);
    }, {
        enabled: !!selectedTableData?.constructedTableId,
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load data'));
        }
    });
    const { data: validationRules = [], isLoading: isLoadingRules } = useQuery(['validationRules', selectedTableData?.constructedTableId], () => {
        if (!selectedTableData?.constructedTableId)
            return Promise.resolve([]);
        return fetchValidationRules(selectedTableData.constructedTableId);
    }, {
        enabled: !!selectedTableData?.constructedTableId,
        onError: (error) => {
            toast.showError(getErrorMessage(error, 'Failed to load validation rules'));
        }
    });
    // Get selected objects for display
    const selectedProcessArea = useMemo(() => processAreas.find((p) => p.id === selectedProcessAreaId), [processAreas, selectedProcessAreaId]);
    const selectedDataObject = useMemo(() => dataObjects.find((d) => d.id === selectedDataObjectId), [dataObjects, selectedDataObjectId]);
    const selectedSystem = useMemo(() => systems.find((s) => s.id === selectedSystemId), [systems, selectedSystemId]);
    // Handlers
    const handleOpenTableDetails = useCallback((table) => {
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
    const handleDetailTabChange = (_, newValue) => {
        setDetailTabValue(newValue);
    };
    const handleTablePaginationChange = (event, newPage) => {
        setTablePaginationPage(newPage);
    };
    const handleRowsPerPageChange = (event) => {
        setTablePaginationRowsPerPage(parseInt(event.target.value, 10));
        setTablePaginationPage(0);
    };
    const handleAddRow = useCallback(() => {
        setAddRowDialogOpen(true);
    }, []);
    const handleRefresh = useCallback(async () => {
        setIsRefreshing(true);
        try {
            await refetchRows();
            toast.showSuccess('Data refreshed');
        }
        finally {
            setIsRefreshing(false);
        }
    }, [refetchRows, toast]);
    const handleProcessAreaChange = useCallback((_, value) => {
        // Only update Process Area - don't reset other filters
        setSelectedProcessAreaId(value?.id ?? null);
        // Close modal and clear search when changing filters
        setSelectedTableId(null);
        setDetailDialogOpen(false);
    }, []);
    const handleDataObjectChange = useCallback((_, value) => {
        // Only update Data Object - don't reset other filters
        setSelectedDataObjectId(value?.id ?? null);
        // Close modal and clear search when changing filters
        setSelectedTableId(null);
        setDetailDialogOpen(false);
    }, []);
    const handleSystemChange = useCallback((_, value) => {
        // Only update System - don't reset other filters
        setSelectedSystemId(value?.id ?? null);
        // Close modal and clear search when changing filters
        setSelectedTableId(null);
        setDetailDialogOpen(false);
    }, []);
    const isLoadingTables = isLoadingAllDefinitions;
    const isLoadingDetails = isLoadingFields || isLoadingRows || isLoadingRules;
    return (_jsxs(Box, { sx: { p: 3 }, children: [_jsxs(Box, { sx: { mb: 4 }, children: [_jsx(Typography, { variant: "h4", component: "h1", sx: { mb: 1, fontWeight: 'bold' }, children: "Data Construction" }), _jsx(Typography, { variant: "body2", color: "textSecondary", children: "Manage and edit construction table data with validation rules" })] }), _jsxs(Paper, { sx: { p: 3, mb: 3, backgroundColor: alpha(theme.palette.primary.main, 0.02), border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}` }, children: [_jsx(Typography, { variant: "subtitle2", sx: { mb: 2, fontWeight: 600, textTransform: 'uppercase', color: 'textSecondary' }, children: "Filters" }), _jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 12, sm: 6, md: 3, children: _jsx(Autocomplete, { fullWidth: true, options: processAreas, getOptionLabel: (option) => option.name, value: selectedProcessArea ?? null, onChange: handleProcessAreaChange, loading: isLoadingProcessAreas, disabled: isLoadingProcessAreas, size: "small", renderInput: (params) => (_jsx(TextField, { ...params, label: "Process Area", placeholder: "Select...", required: true })) }) }), _jsx(Grid, { item: true, xs: 12, sm: 6, md: 3, children: _jsx(Autocomplete, { fullWidth: true, options: dataObjects, getOptionLabel: (option) => option.name, value: selectedDataObject ?? null, onChange: handleDataObjectChange, loading: isLoadingDataObjects, disabled: isLoadingDataObjects || !selectedProcessArea, size: "small", renderInput: (params) => (_jsx(TextField, { ...params, label: "Data Object", placeholder: "Select...", required: true })) }) }), _jsx(Grid, { item: true, xs: 12, sm: 6, md: 3, children: _jsx(Autocomplete, { fullWidth: true, options: systems, getOptionLabel: (option) => option.name, value: selectedSystem ?? null, onChange: handleSystemChange, loading: isLoadingSystems, disabled: isLoadingSystems, size: "small", renderInput: (params) => (_jsx(TextField, { ...params, label: "System", placeholder: "Select...", required: true })) }) }), _jsx(Grid, { item: true, xs: 12, sm: 6, md: 3, children: _jsx(TextField, { fullWidth: true, size: "small", placeholder: "Search tables...", value: searchText, onChange: (e) => setSearchText(e.target.value), InputProps: {
                                        startAdornment: _jsx(SearchIcon, { sx: { mr: 1, color: 'action.disabled' } })
                                    } }) })] })] }), _jsx(Paper, { children: isLoadingAllDefinitions ? (_jsx(LinearProgress, {})) : filteredTables.length === 0 ? (_jsxs(Box, { sx: { p: 4, textAlign: 'center' }, children: [_jsx(Typography, { color: "textSecondary", sx: { mb: 2 }, children: "No construction tables found" }), _jsx(Typography, { variant: "caption", color: "textSecondary", children: allConstructionTables.length === 0
                                ? 'No construction tables available'
                                : 'Try adjusting your search filters' })] })) : (_jsx(TableContainer, { children: _jsxs(Table, { children: [_jsx(TableHead, { children: _jsxs(TableRow, { sx: { backgroundColor: alpha(theme.palette.primary.main, 0.08) }, children: [_jsx(TableCell, { sx: { fontWeight: 600 }, children: "Table Name" }), _jsx(TableCell, { sx: { fontWeight: 600 }, children: "Description" }), _jsx(TableCell, { sx: { fontWeight: 600 }, children: "Process Area" }), _jsx(TableCell, { sx: { fontWeight: 600 }, children: "Data Object" }), _jsx(TableCell, { sx: { fontWeight: 600 }, children: "System" }), _jsx(TableCell, { sx: { fontWeight: 600 }, align: "center", children: "Status" }), _jsx(TableCell, { sx: { fontWeight: 600 }, align: "right", children: "Actions" })] }) }), _jsx(TableBody, { children: filteredTables.map((table) => (_jsxs(TableRow, { hover: true, sx: {
                                        '&:hover': {
                                            backgroundColor: alpha(theme.palette.primary.main, 0.05)
                                        }
                                    }, children: [_jsx(TableCell, { sx: { fontWeight: 500 }, children: table.alias || 'Unnamed Table' }), _jsx(TableCell, { children: _jsx(Typography, { variant: "body2", color: "textSecondary", sx: { maxWidth: 300 }, children: table.description || '—' }) }), _jsx(TableCell, { children: _jsx(Typography, { variant: "body2", color: "textSecondary", children: table.processAreaName || '—' }) }), _jsx(TableCell, { children: _jsx(Typography, { variant: "body2", color: "textSecondary", children: table.dataObjectName || '—' }) }), _jsx(TableCell, { children: _jsx(Typography, { variant: "body2", color: "textSecondary", children: table.systemName || '—' }) }), _jsx(TableCell, { align: "center", children: _jsx(Chip, { label: table.constructedTableStatus || 'draft', size: "small", variant: "outlined", color: table.constructedTableStatus === 'approved'
                                                    ? 'success'
                                                    : table.constructedTableStatus === 'rejected'
                                                        ? 'error'
                                                        : 'default' }) }), _jsx(TableCell, { align: "right", children: _jsx(Stack, { direction: "row", spacing: 0.5, justifyContent: "flex-end", children: _jsx(Tooltip, { title: "Open table", children: _jsx(IconButton, { size: "small", onClick: () => handleOpenTableDetails(table), disabled: !table.constructedTableId, children: _jsx(OpenInNewIcon, { fontSize: "small" }) }) }) }) })] }, table.id))) })] }) })) }), _jsx(Dialog, { open: detailDialogOpen, onClose: handleCloseTableDetails, maxWidth: "lg", fullWidth: true, PaperProps: {
                    sx: { maxHeight: '90vh' }
                }, children: selectedTableData && (_jsxs(_Fragment, { children: [_jsxs(DialogTitle, { sx: { display: 'flex', alignItems: 'center', justifyContent: 'space-between' }, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "h6", sx: { fontWeight: 600 }, children: selectedTableData.alias || 'Construction Table' }), _jsx(Typography, { variant: "caption", color: "textSecondary", children: selectedTableData.description })] }), _jsx(Stack, { direction: "row", spacing: 1, children: _jsx(IconButton, { size: "small", onClick: handleRefresh, disabled: isRefreshing, title: "Refresh data", children: _jsx(RefreshIcon, { fontSize: "small" }) }) })] }), _jsx(Divider, {}), _jsxs(DialogContent, { sx: { p: 0 }, children: [_jsxs(MuiTabs, { value: detailTabValue, onChange: handleDetailTabChange, sx: { borderBottom: 1, borderColor: 'divider', px: 3 }, children: [_jsx(MuiTab, { label: "Data Entry", id: "table-details-tab-0" }), _jsx(MuiTab, { label: "Validation Rules", id: "table-details-tab-1" })] }), _jsx(TabPanel, { value: detailTabValue, index: 0, children: isLoadingDetails ? (_jsx(Box, { sx: { display: 'flex', justifyContent: 'center', py: 4 }, children: _jsx(Typography, { color: "textSecondary", children: "Loading..." }) })) : (_jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: "row", spacing: 1, justifyContent: "space-between", children: [_jsxs(Typography, { variant: "subtitle2", sx: { fontWeight: 600 }, children: ["Data Rows (", rows.length, ")"] }), _jsx(Button, { size: "small", startIcon: _jsx(AddIcon, {}), variant: "contained", onClick: handleAddRow, disabled: !canManage, children: "Add Row" })] }), selectedTableData.constructedTableId && (_jsx(ConstructedDataGrid, { constructedTableId: selectedTableData.constructedTableId, fields: fields, rows: rows, onDataChange: handleRefresh }))] })) }), _jsx(TabPanel, { value: detailTabValue, index: 1, children: isLoadingDetails ? (_jsx(Box, { sx: { display: 'flex', justifyContent: 'center', py: 4 }, children: _jsx(Typography, { color: "textSecondary", children: "Loading..." }) })) : (selectedTableData.constructedTableId && (_jsx(ValidationRulesManager, { constructedTableId: selectedTableData.constructedTableId, fields: fields, validationRules: validationRules, onRulesChange: () => {
                                            queryClient.refetchQueries(['validationRules', selectedTableData.constructedTableId]);
                                        } }))) })] })] })) }), selectedTableData?.constructedTableId && (_jsx(AddRowDialog, { open: addRowDialogOpen, fields: fields, onAdd: async (rowData) => {
                    try {
                        await batchSaveConstructedData(selectedTableData.constructedTableId, {
                            rows: [rowData],
                            validateOnly: false
                        });
                        await handleRefresh();
                        setAddRowDialogOpen(false);
                    }
                    catch (error) {
                        console.error('Failed to add row:', error);
                        throw error;
                    }
                }, onClose: () => setAddRowDialogOpen(false) }))] }));
};
export default DataConstructionPage;
