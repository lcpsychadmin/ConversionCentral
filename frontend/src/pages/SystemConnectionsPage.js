import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, Paper, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import SystemConnectionTable from '../components/system-connection/SystemConnectionTable';
import SystemConnectionDetailModal from '../components/system-connection/SystemConnectionDetailModal';
import ConnectionCatalogTable from '../components/system-connection/ConnectionCatalogTable';
import SystemConnectionForm from '../components/system-connection/SystemConnectionForm';
import ConnectionDataPreviewDialog from '../components/system-connection/ConnectionDataPreviewDialog';
import ConnectionIngestionPanel from '../components/system-connection/ConnectionIngestionPanel';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useSystems } from '../hooks/useSystems';
import { useAuth } from '../context/AuthContext';
import { fetchSystemConnectionCatalog, fetchConnectionTablePreview, updateSystemConnectionCatalogSelection } from '../services/systemConnectionService';
const getErrorMessage = (error, fallback) => {
    if (!error)
        return fallback;
    if (error instanceof Error)
        return error.message;
    if (typeof error === 'string')
        return error;
    if (typeof error === 'object') {
        const candidate = error.detail ??
            error.message;
        if (typeof candidate === 'string') {
            return candidate;
        }
    }
    return fallback;
};
const SystemConnectionsPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const theme = useTheme();
    const { systemsQuery: { data: systems = [], isLoading: systemsLoading, isError: systemsError, error: systemsErrorObj } } = useSystems();
    const { connectionsQuery, createConnection, updateConnection, deleteConnection, testConnection, creating, updating, deleting, testing } = useSystemConnections();
    const { data: connections = [], isLoading: connectionsLoading, isError: connectionsError, error: connectionsErrorObj } = connectionsQuery;
    const [selected, setSelected] = useState(null);
    const [formOpen, setFormOpen] = useState(false);
    const [formMode, setFormMode] = useState('create');
    const [confirmOpen, setConfirmOpen] = useState(false);
    const [catalogRows, setCatalogRows] = useState([]);
    const [catalogLoading, setCatalogLoading] = useState(false);
    const [catalogSaving, setCatalogSaving] = useState(false);
    const [catalogError, setCatalogError] = useState(null);
    const [previewOpen, setPreviewOpen] = useState(false);
    const [previewTarget, setPreviewTarget] = useState(null);
    const [previewConnectionId, setPreviewConnectionId] = useState(null);
    const [previewData, setPreviewData] = useState(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewError, setPreviewError] = useState(null);
    const [detailModalOpen, setDetailModalOpen] = useState(false);
    const systemLookup = useMemo(() => new Map(systems.map((system) => [system.id, system])), [systems]);
    const sortedConnections = useMemo(() => {
        return connections
            .slice()
            .sort((a, b) => {
            const systemA = systemLookup.get(a.systemId)?.name ?? '';
            const systemB = systemLookup.get(b.systemId)?.name ?? '';
            if (systemA !== systemB) {
                return systemA.localeCompare(systemB);
            }
            const connStringA = a.connectionString ?? '';
            const connStringB = b.connectionString ?? '';
            return connStringA.localeCompare(connStringB);
        });
    }, [connections, systemLookup]);
    useEffect(() => {
        if (selected && !connections.some((connection) => connection.id === selected.id)) {
            setSelected(null);
        }
    }, [connections, selected]);
    useEffect(() => {
        if (!selected ||
            (previewConnectionId && selected.id !== previewConnectionId)) {
            setPreviewOpen(false);
            setPreviewTarget(null);
            setPreviewConnectionId(null);
            setPreviewData(null);
            setPreviewError(null);
        }
    }, [selected, previewConnectionId]);
    const loadCatalog = useCallback(async (connectionId) => {
        setCatalogLoading(true);
        setCatalogError(null);
        try {
            const data = await fetchSystemConnectionCatalog(connectionId);
            setCatalogRows(data);
        }
        catch (error) {
            setCatalogError(getErrorMessage(error, 'Unable to load source catalog.'));
        }
        finally {
            setCatalogLoading(false);
        }
    }, []);
    const loadPreview = useCallback(async (connectionId, schemaName, tableName) => {
        setPreviewLoading(true);
        setPreviewError(null);
        try {
            const data = await fetchConnectionTablePreview(connectionId, schemaName, tableName);
            setPreviewData(data);
        }
        catch (error) {
            setPreviewError(getErrorMessage(error, 'Unable to load preview.'));
        }
        finally {
            setPreviewLoading(false);
        }
    }, []);
    useEffect(() => {
        if (!selected) {
            setCatalogRows([]);
            setCatalogError(null);
            setCatalogLoading(false);
            setCatalogSaving(false);
            return;
        }
        loadCatalog(selected.id);
    }, [selected, loadCatalog]);
    const handleCatalogSelectionChange = useCallback(async (nextSelection) => {
        if (!selected)
            return;
        const selectionSet = new Set(nextSelection);
        const payload = catalogRows
            .filter((row) => selectionSet.has(`${row.schemaName}.${row.tableName}`))
            .map((row) => ({
            schemaName: row.schemaName,
            tableName: row.tableName,
            tableType: row.tableType ?? undefined,
            columnCount: row.columnCount ?? undefined,
            estimatedRows: row.estimatedRows ?? undefined
        }));
        setCatalogSaving(true);
        setCatalogError(null);
        try {
            await updateSystemConnectionCatalogSelection(selected.id, payload);
            await loadCatalog(selected.id);
        }
        catch (error) {
            setCatalogError(getErrorMessage(error, 'Unable to save selection.'));
        }
        finally {
            setCatalogSaving(false);
        }
    }, [selected, catalogRows, loadCatalog]);
    const handleCatalogRefresh = useCallback(() => {
        if (selected) {
            loadCatalog(selected.id);
        }
    }, [selected, loadCatalog]);
    const handlePreviewRequest = useCallback((row) => {
        if (!selected)
            return;
        const schemaName = row.schemaName?.trim() ? row.schemaName : null;
        const target = {
            schemaName,
            tableName: row.tableName
        };
        setPreviewTarget(target);
        setPreviewData(null);
        setPreviewError(null);
        setPreviewOpen(true);
        setPreviewConnectionId(selected.id);
        loadPreview(selected.id, target.schemaName, target.tableName);
    }, [selected, loadPreview]);
    const handlePreviewRefresh = useCallback(() => {
        if (!selected || !previewTarget)
            return;
        loadPreview(selected.id, previewTarget.schemaName, previewTarget.tableName);
    }, [selected, previewTarget, loadPreview]);
    const handlePreviewClose = useCallback(() => {
        setPreviewOpen(false);
        setPreviewTarget(null);
        setPreviewConnectionId(null);
        setPreviewData(null);
        setPreviewError(null);
    }, []);
    const handleSelect = (connection) => {
        setSelected(connection);
    };
    const handleOpenDetail = (connection) => {
        setSelected(connection);
        setDetailModalOpen(true);
    };
    const handleCreateClick = () => {
        setFormMode('create');
        setSelected(null);
        setFormOpen(true);
    };
    const handleEdit = (connection) => {
        setFormMode('edit');
        setSelected(connection);
        setFormOpen(true);
    };
    const handleDelete = (connection) => {
        setSelected(connection);
        setConfirmOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values, connectionString) => {
        const payload = {
            systemId: values.systemId,
            connectionType: 'jdbc',
            connectionString,
            authMethod: 'username_password',
            notes: values.notes ?? null,
            active: values.active,
            ingestionEnabled: values.ingestionEnabled
        };
        try {
            if (formMode === 'create') {
                await createConnection(payload);
            }
            else if (selected) {
                await updateConnection({
                    id: selected.id,
                    input: payload
                });
            }
            setFormOpen(false);
        }
        catch (error) {
            // notifications handled in hooks
        }
    };
    const handleFormTest = async (_values, connectionString) => {
        try {
            await testConnection({
                connectionType: 'jdbc',
                connectionString
            });
        }
        catch (error) {
            // toast handled in hook
        }
    };
    const handleConfirmDelete = async () => {
        if (!selected)
            return;
        try {
            await deleteConnection(selected.id);
            setConfirmOpen(false);
            setSelected(null);
        }
        catch (error) {
            // toast handled in hook
        }
    };
    const handleTestConnection = async (connection) => {
        if (testing)
            return;
        try {
            await testConnection({
                connectionType: connection.connectionType,
                connectionString: connection.connectionString
            });
        }
        catch (error) {
            // toast handled in hook
        }
    };
    const loading = systemsLoading || connectionsLoading;
    const saving = creating || updating;
    const busy = saving || deleting;
    const primaryError = systemsError
        ? systemsErrorObj
        : connectionsError
            ? connectionsErrorObj
            : null;
    const errorMessage = primaryError
        ? getErrorMessage(primaryError, 'Unable to load connections.')
        : null;
    const detailSystem = selected ? systemLookup.get(selected.systemId) : null;
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Data Source Connections" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Register JDBC connections for relational sources. Use these connections when ingesting tables." })] }), canManage && (_jsx(Box, { sx: { mb: 3, display: 'flex', justifyContent: 'flex-end' }, children: _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy || systems.length === 0, children: "New Connection" }) })), canManage && !loading && systems.length === 0 && (_jsx(Alert, { severity: "info", sx: { mb: 3 }, children: "Add a system before registering a connection." })), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), _jsx(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                }, children: _jsx(SystemConnectionTable, { data: sortedConnections, systems: systems, loading: loading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onViewDetail: handleOpenDetail, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined, onTest: canManage ? handleTestConnection : undefined }) }), selected && (_jsxs(_Fragment, { children: [_jsx(Paper, { elevation: 3, sx: {
                            p: 3,
                            mb: 3,
                            background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                        }, children: _jsx(ConnectionCatalogTable, { rows: catalogRows, loading: catalogLoading, saving: catalogSaving, error: catalogError, onRefresh: handleCatalogRefresh, onSelectionChange: handleCatalogSelectionChange, onPreview: handlePreviewRequest }) }), selected.ingestionEnabled && (_jsx(Paper, { elevation: 3, sx: {
                            p: 3,
                            mb: 3,
                            background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
                        }, children: _jsx(ConnectionIngestionPanel, { connection: selected, system: detailSystem, catalogRows: catalogRows }) })), _jsx(ConnectionDataPreviewDialog, { open: previewOpen, schemaName: previewTarget?.schemaName ?? null, tableName: previewTarget?.tableName ?? 'Table', loading: previewLoading, error: previewError, preview: previewData, onClose: handlePreviewClose, onRefresh: handlePreviewRefresh })] })), canManage && (_jsx(SystemConnectionForm, { open: formOpen, title: formMode === 'create' ? 'Create Connection' : 'Edit Connection', systems: systems, initialValues: formMode === 'edit' ? selected : null, loading: saving, testing: testing, onClose: handleFormClose, onSubmit: handleFormSubmit, onTest: handleFormTest })), canManage && (_jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Connection", description: `Are you sure you want to delete this connection? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete, loading: deleting })), _jsx(SystemConnectionDetailModal, { open: detailModalOpen, connection: selected, system: detailSystem, onClose: () => setDetailModalOpen(false) })] }));
};
export default SystemConnectionsPage;
