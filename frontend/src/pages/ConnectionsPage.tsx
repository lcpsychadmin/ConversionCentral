import { useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, Grid, Paper, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';

import PageHeader from '../components/common/PageHeader';
import SystemConnectionTable from '../components/system-connection/SystemConnectionTable';
import SystemConnectionDetailModal from '../components/system-connection/SystemConnectionDetailModal';
import SystemConnectionForm from '../components/system-connection/SystemConnectionForm';
import DatabricksConnectionDialog from '../components/system-connection/DatabricksConnectionDialog';
import ConnectionTypePickerDialog from '../components/system-connection/ConnectionTypePickerDialog';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useAuth } from '../context/AuthContext';
import {
  RelationalDatabaseType,
  System,
  SystemConnection,
  SystemConnectionFormValues,
  SystemConnectionInput
} from '../types/data';
import { getSectionSurface } from '../theme/surfaceStyles';
import { parseJdbcConnectionString } from '../utils/connectionString';

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (!error) return fallback;
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (typeof error === 'object') {
    const candidate = (error as { detail?: unknown; message?: unknown }).detail ??
      (error as { detail?: unknown; message?: unknown }).message;
    if (typeof candidate === 'string') {
      return candidate;
    }
  }
  return fallback;
};

const ConnectionsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const sectionSurface = useMemo(
    () => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }),
    [isDarkMode, theme]
  );

  const { systemsQuery } = useSystems();
  const {
    data: systems = [],
    isLoading: systemsLoading,
    isError: systemsError,
    error: systemsErrorObj
  } = systemsQuery;

  const {
    connectionsQuery,
    createConnection,
    updateConnection,
    deleteConnection,
    testConnection,
    creating: connectionCreating,
    updating: connectionUpdating,
    deleting: connectionDeleting,
    testing
  } = useSystemConnections();

  const {
    data: connections = [],
    isLoading: connectionsLoading,
    isError: connectionsError,
    error: connectionsErrorObj
  } = connectionsQuery;

  const [selectedConnection, setSelectedConnection] = useState<SystemConnection | null>(null);
  const [connectionFormOpen, setConnectionFormOpen] = useState(false);
  const [connectionFormMode, setConnectionFormMode] = useState<'create' | 'edit'>('create');
  const [connectionTypePickerOpen, setConnectionTypePickerOpen] = useState(false);
  const [pendingDatabaseType, setPendingDatabaseType] = useState<RelationalDatabaseType | null>(null);
  const [connectionConfirmOpen, setConnectionConfirmOpen] = useState(false);
  const [detailModalOpen, setDetailModalOpen] = useState(false);

  const managedSystemIds = useMemo(
    () => systems.filter((system) => system.systemType === 'warehouse').map((system) => system.id),
    [systems]
  );

  const systemLookup = useMemo(
    () => new Map<string, System>(systems.map((system) => [system.id, system])),
    [systems]
  );

  const visibleConnections = useMemo(
    () =>
      connections.filter((connection) => {
        if (!connection.systemId) {
          return true;
        }
        return !managedSystemIds.includes(connection.systemId);
      }),
    [connections, managedSystemIds]
  );

  const sortedConnections = useMemo(() => {
    return visibleConnections
      .slice()
      .sort((a, b) => {
        const systemA = a.systemId
          ? systemLookup.get(a.systemId)?.name ?? 'Unknown application'
          : 'Global connection';
        const systemB = b.systemId
          ? systemLookup.get(b.systemId)?.name ?? 'Unknown application'
          : 'Global connection';
        if (systemA !== systemB) {
          return systemA.localeCompare(systemB);
        }
        const connStringA = a.connectionString ?? '';
        const connStringB = b.connectionString ?? '';
        return connStringA.localeCompare(connStringB);
      });
  }, [systemLookup, visibleConnections]);

  useEffect(() => {
    if (
      selectedConnection &&
      !sortedConnections.some((connection) => connection.id === selectedConnection.id)
    ) {
      setSelectedConnection(null);
    }
  }, [sortedConnections, selectedConnection]);

  const handleConnectionSelect = (connection: SystemConnection | null) => {
    setSelectedConnection(connection);
  };

  const handleConnectionOpenDetail = (connection: SystemConnection) => {
    setSelectedConnection(connection);
    setDetailModalOpen(true);
  };

  const handleConnectionTypeSelected = (databaseType: RelationalDatabaseType) => {
    setPendingDatabaseType(databaseType);
    setConnectionTypePickerOpen(false);
    setConnectionFormOpen(true);
  };

  const handleConnectionTypePickerClose = () => {
    setConnectionTypePickerOpen(false);
    if (!connectionFormOpen) {
      setPendingDatabaseType(null);
    }
  };

  const handleConnectionCreateClick = () => {
    setConnectionFormMode('create');
    setSelectedConnection(null);
    setPendingDatabaseType(null);
    setConnectionTypePickerOpen(true);
  };

  const handleConnectionEdit = (connection: SystemConnection) => {
    setConnectionFormMode('edit');
    setSelectedConnection(connection);
    setPendingDatabaseType(null);
    setConnectionFormOpen(true);
  };

  const handleConnectionDeleteRequest = (connection: SystemConnection) => {
    setSelectedConnection(connection);
    setConnectionConfirmOpen(true);
  };

  const handleConnectionFormClose = () => {
    setConnectionFormOpen(false);
    if (connectionFormMode === 'create') {
      setPendingDatabaseType(null);
    }
  };

  const handleConnectionFormSubmit = async (
    values: SystemConnectionFormValues,
    connectionString: string | null
  ) => {
    const payload: SystemConnectionInput = {
      name: values.name,
      connectionType: 'jdbc' as const,
      authMethod: 'username_password' as const,
      notes: values.notes ?? null,
      active: values.active
    };

    if (connectionString !== null) {
      payload.connectionString = connectionString;
    }

    try {
      if (connectionFormMode === 'create') {
        await createConnection(payload);
      } else if (selectedConnection) {
        await updateConnection({
          id: selectedConnection.id,
          input: payload
        });
      }
      setConnectionFormOpen(false);
      if (connectionFormMode === 'create') {
        setPendingDatabaseType(null);
      }
    } catch (error) {
      // notifications handled in hooks
    }
  };

  const handleConnectionFormTest = async (
    _values: SystemConnectionFormValues,
    connectionString: string | null
  ) => {
    if (!connectionString) {
      return;
    }
    try {
      await testConnection({
        connectionType: 'jdbc',
        connectionString
      });
    } catch (error) {
      // toast handled in hook
    }
  };

  const handleConnectionConfirmDelete = async () => {
    if (!selectedConnection) return;
    try {
      await deleteConnection(selectedConnection.id);
      setConnectionConfirmOpen(false);
      setSelectedConnection(null);
    } catch (error) {
      // toast handled in hook
    }
  };

  const handleTestConnection = async (connection: SystemConnection) => {
    if (testing) return;
    try {
      await testConnection({
        connectionType: connection.connectionType,
        connectionString: connection.connectionString
      });
    } catch (error) {
      // toast handled in hook
    }
  };

  const combinedLoading = systemsLoading || connectionsLoading;
  const connectionSaving = connectionCreating || connectionUpdating;
  const connectionBusy = connectionSaving || connectionDeleting;

  const systemErrorMessage = systemsError
    ? getErrorMessage(systemsErrorObj, 'Unable to load applications. Connections may be unavailable.')
    : null;

  const connectionErrorMessage = connectionsError
    ? getErrorMessage(connectionsErrorObj, 'Unable to load connections.')
    : null;

  const detailSystem =
    selectedConnection && selectedConnection.systemId
      ? systemLookup.get(selectedConnection.systemId) ?? null
      : null;
  const newConnectionDisabled = connectionBusy;
  const isDatabricksSelectedConnection = useMemo(() => {
    if (connectionFormMode !== 'edit' || !selectedConnection) {
      return false;
    }
    const parsed = parseJdbcConnectionString(selectedConnection.connectionString);
    return parsed?.databaseType === 'databricks';
  }, [connectionFormMode, selectedConnection]);

  const shouldUseDatabricksDialog =
    (connectionFormMode === 'create' && pendingDatabaseType === 'databricks') ||
    (connectionFormMode === 'edit' && isDatabricksSelectedConnection);

  return (
    <Box>
      <PageHeader
        title="Connections"
        subtitle="Register and maintain source connections. Manage table catalogs from the Tables page."
      />

      {systemErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {systemErrorMessage}
        </Alert>
      )}

      {connectionErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {connectionErrorMessage}
        </Alert>
      )}

      <Grid container spacing={3} alignItems="flex-start">
        <Grid item xs={12}>
          <Paper
            elevation={0}
            sx={{
              p: 3,
              borderRadius: 3,
              ...sectionSurface
            }}
          >
            <Box display="flex" alignItems="center" justifyContent="space-between" mb={3}>
              <Typography
                variant="h5"
                sx={{
                  color: isDarkMode ? theme.palette.common.white : theme.palette.primary.dark,
                  fontWeight: 700
                }}
              >
                Connections
              </Typography>
              {canManage && (
                <Button
                  variant="contained"
                  onClick={handleConnectionCreateClick}
                  disabled={newConnectionDisabled}
                >
                  New Connection
                </Button>
              )}
            </Box>

            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Manage every registered connection below. Select a row to review connection details or run connectivity tests. Source table selection now lives on the Tables page.
            </Typography>

            <SystemConnectionTable
              data={sortedConnections}
              loading={combinedLoading}
              selectedId={selectedConnection?.id ?? null}
              canManage={canManage}
              onSelect={handleConnectionSelect}
              onViewDetail={handleConnectionOpenDetail}
              onEdit={canManage ? handleConnectionEdit : undefined}
              onDelete={canManage ? handleConnectionDeleteRequest : undefined}
              onTest={canManage ? handleTestConnection : undefined}
            />
          </Paper>
        </Grid>
      </Grid>

      <SystemConnectionDetailModal
        open={detailModalOpen}
        connection={selectedConnection}
        system={detailSystem}
        onClose={() => setDetailModalOpen(false)}
      />

      {canManage && (
        <>
          <ConnectionTypePickerDialog
            open={connectionTypePickerOpen}
            onClose={handleConnectionTypePickerClose}
            onSelect={handleConnectionTypeSelected}
          />
          {shouldUseDatabricksDialog ? (
            <DatabricksConnectionDialog
              open={connectionFormOpen}
              title={
                connectionFormMode === 'create' ? 'Connect Databricks' : 'Edit Databricks Connection'
              }
              initialValues={connectionFormMode === 'edit' ? selectedConnection : null}
              loading={connectionSaving}
              testing={testing}
              onClose={handleConnectionFormClose}
              onSubmit={handleConnectionFormSubmit}
              onTest={handleConnectionFormTest}
            />
          ) : (
            <SystemConnectionForm
              open={connectionFormOpen}
              title={connectionFormMode === 'create' ? 'Create Connection' : 'Edit Connection'}
              initialValues={connectionFormMode === 'edit' ? selectedConnection : null}
              loading={connectionSaving}
              testing={testing}
              onClose={handleConnectionFormClose}
              onSubmit={handleConnectionFormSubmit}
              onTest={handleConnectionFormTest}
              forcedDatabaseType={connectionFormMode === 'create' ? pendingDatabaseType : null}
              showDatabaseTypeSelector={connectionFormMode === 'edit' || !pendingDatabaseType}
            />
          )}

          <ConfirmDialog
            open={connectionConfirmOpen}
            title="Delete Connection"
            description="Are you sure you want to delete this connection? This action cannot be undone."
            confirmLabel="Delete"
            onClose={() => setConnectionConfirmOpen(false)}
            onConfirm={handleConnectionConfirmDelete}
            loading={connectionDeleting}
          />
        </>
      )}
    </Box>
  );
};

export default ConnectionsPage;
