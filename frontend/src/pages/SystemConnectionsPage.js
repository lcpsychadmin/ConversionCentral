import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, Divider, Grid, Paper, Stack, Typography } from '@mui/material';
import SystemConnectionTable from '../components/system-connection/SystemConnectionTable';
import SystemConnectionForm from '../components/system-connection/SystemConnectionForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useSystems } from '../hooks/useSystems';
import { formatConnectionSummary, parseJdbcConnectionString } from '../utils/connectionString';
import { useAuth } from '../context/AuthContext';
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
    const { systemsQuery: { data: systems = [], isLoading: systemsLoading, isError: systemsError, error: systemsErrorObj } } = useSystems();
    const { connectionsQuery, createConnection, updateConnection, deleteConnection, testConnection, creating, updating, deleting, testing } = useSystemConnections();
    const { data: connections = [], isLoading: connectionsLoading, isError: connectionsError, error: connectionsErrorObj } = connectionsQuery;
    const [selected, setSelected] = useState(null);
    const [formOpen, setFormOpen] = useState(false);
    const [formMode, setFormMode] = useState('create');
    const [confirmOpen, setConfirmOpen] = useState(false);
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
            return a.connectionString.localeCompare(b.connectionString);
        });
    }, [connections, systemLookup]);
    useEffect(() => {
        if (selected && !connections.some((connection) => connection.id === selected.id)) {
            setSelected(null);
        }
    }, [connections, selected]);
    const handleSelect = (connection) => {
        setSelected(connection);
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
            active: values.active
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
    const detailParsed = selected ? parseJdbcConnectionString(selected.connectionString) : null;
    const detailSystem = selected ? systemLookup.get(selected.systemId) : null;
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", mb: 3, children: [_jsxs("div", { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Data Source Connections" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Register JDBC connections for relational sources. Use these connections when ingesting tables." })] }), canManage && (_jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: busy || systems.length === 0, children: "New Connection" }))] }), _jsx(Paper, { elevation: 1, sx: { p: 2, mb: 3 }, children: _jsx(SystemConnectionTable, { data: sortedConnections, systems: systems, loading: loading, selectedId: selected?.id ?? null, canManage: canManage, onSelect: handleSelect, onEdit: canManage ? handleEdit : undefined, onDelete: canManage ? handleDelete : undefined, onTest: canManage ? handleTestConnection : undefined }) }), canManage && !loading && systems.length === 0 && (_jsx(Alert, { severity: "info", sx: { mb: 3 }, children: "Add a system before registering a connection." })), errorMessage && (_jsx(Alert, { severity: "error", sx: { mb: 3 }, children: errorMessage })), selected && (_jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsxs(Grid, { container: true, spacing: 2, children: [_jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Connection Details" }), _jsxs(Stack, { spacing: 1.5, children: [_jsx(DetailLine, { label: "System", value: detailSystem?.name ?? '—' }), _jsx(DetailLine, { label: "Endpoint", value: formatConnectionSummary(selected.connectionString) }), _jsx(DetailLine, { label: "Database", value: detailParsed ? detailParsed.database : '—' }), _jsx(DetailLine, { label: "Host", value: detailParsed ? `${detailParsed.host}${detailParsed.port ? `:${detailParsed.port}` : ''}` : '—' })] })] }), _jsxs(Grid, { item: true, xs: 12, md: 6, children: [_jsx(Typography, { variant: "h6", gutterBottom: true, children: "Access & Metadata" }), _jsxs(Stack, { spacing: 1.5, children: [_jsx(DetailLine, { label: "Username", value: detailParsed?.username ? detailParsed.username : '—' }), _jsx(DetailLine, { label: "Status", value: selected.active ? 'Active' : 'Disabled' }), _jsx(DetailLine, { label: "Notes", value: selected.notes ?? '—' }), _jsx(DetailLine, { label: "Last Updated", value: selected.updatedAt ? new Date(selected.updatedAt).toLocaleString() : '—' })] })] })] }) })), canManage && (_jsx(SystemConnectionForm, { open: formOpen, title: formMode === 'create' ? 'Create Connection' : 'Edit Connection', systems: systems, initialValues: formMode === 'edit' ? selected : null, loading: saving, testing: testing, onClose: handleFormClose, onSubmit: handleFormSubmit, onTest: handleFormTest })), canManage && (_jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Connection", description: `Are you sure you want to delete this connection? This action cannot be undone.`, confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleConfirmDelete, loading: deleting }))] }));
};
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: label }), _jsx(Typography, { variant: "body1", children: value }), _jsx(Divider, { sx: { my: 1.5 } })] }));
export default SystemConnectionsPage;
