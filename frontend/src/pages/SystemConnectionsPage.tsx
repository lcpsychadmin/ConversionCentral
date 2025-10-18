import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Divider,
  Grid,
  Paper,
  Stack,
  Typography
} from '@mui/material';

import SystemConnectionTable from '../components/system-connection/SystemConnectionTable';
import SystemConnectionForm from '../components/system-connection/SystemConnectionForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useSystems } from '../hooks/useSystems';
import { System, SystemConnection, SystemConnectionFormValues } from '../types/data';
import { formatConnectionSummary, parseJdbcConnectionString } from '../utils/connectionString';
import { useAuth } from '../context/AuthContext';

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

const SystemConnectionsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const {
    systemsQuery: { data: systems = [], isLoading: systemsLoading, isError: systemsError, error: systemsErrorObj }
  } = useSystems();

  const {
    connectionsQuery,
    createConnection,
    updateConnection,
    deleteConnection,
    testConnection,
    creating,
    updating,
    deleting,
    testing
  } = useSystemConnections();

  const {
    data: connections = [],
    isLoading: connectionsLoading,
    isError: connectionsError,
    error: connectionsErrorObj
  } = connectionsQuery;

  const [selected, setSelected] = useState<SystemConnection | null>(null);
  const [formOpen, setFormOpen] = useState(false);
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
  const [confirmOpen, setConfirmOpen] = useState(false);

  const systemLookup = useMemo(
    () => new Map<string, System>(systems.map((system) => [system.id, system])),
    [systems]
  );

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

  const handleSelect = (connection: SystemConnection | null) => {
    setSelected(connection);
  };

  const handleCreateClick = () => {
    setFormMode('create');
    setSelected(null);
    setFormOpen(true);
  };

  const handleEdit = (connection: SystemConnection) => {
    setFormMode('edit');
    setSelected(connection);
    setFormOpen(true);
  };

  const handleDelete = (connection: SystemConnection) => {
    setSelected(connection);
    setConfirmOpen(true);
  };

  const handleFormClose = () => {
    setFormOpen(false);
  };

  const handleFormSubmit = async (values: SystemConnectionFormValues, connectionString: string) => {
    const payload = {
      systemId: values.systemId,
      connectionType: 'jdbc' as const,
      connectionString,
      authMethod: 'username_password' as const,
      notes: values.notes ?? null,
      active: values.active
    };

    try {
      if (formMode === 'create') {
        await createConnection(payload);
      } else if (selected) {
        await updateConnection({
          id: selected.id,
          input: payload
        });
      }
      setFormOpen(false);
    } catch (error) {
      // notifications handled in hooks
    }
  };

  const handleFormTest = async (_values: SystemConnectionFormValues, connectionString: string) => {
    try {
      await testConnection({
        connectionType: 'jdbc',
        connectionString
      });
    } catch (error) {
      // toast handled in hook
    }
  };

  const handleConfirmDelete = async () => {
    if (!selected) return;
    try {
      await deleteConnection(selected.id);
      setConfirmOpen(false);
      setSelected(null);
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

  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" gutterBottom>
            Data Source Connections
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Register JDBC connections for relational sources. Use these connections when ingesting tables.
          </Typography>
        </div>
        {canManage && (
          <Button
            variant="contained"
            onClick={handleCreateClick}
            disabled={busy || systems.length === 0}
          >
            New Connection
          </Button>
        )}
      </Stack>

      <Paper elevation={1} sx={{ p: 2, mb: 3 }}>
        <SystemConnectionTable
          data={sortedConnections}
          systems={systems}
          loading={loading}
          selectedId={selected?.id ?? null}
          canManage={canManage}
          onSelect={handleSelect}
          onEdit={canManage ? handleEdit : undefined}
          onDelete={canManage ? handleDelete : undefined}
          onTest={canManage ? handleTestConnection : undefined}
        />
      </Paper>

      {canManage && !loading && systems.length === 0 && (
        <Alert severity="info" sx={{ mb: 3 }}>
          Add a system before registering a connection.
        </Alert>
      )}

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {selected && (
        <Paper elevation={1} sx={{ p: 3 }}>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Connection Details
              </Typography>
              <Stack spacing={1.5}>
                <DetailLine label="System" value={detailSystem?.name ?? '—'} />
                <DetailLine
                  label="Endpoint"
                  value={formatConnectionSummary(selected.connectionString)}
                />
                <DetailLine
                  label="Database"
                  value={detailParsed ? detailParsed.database : '—'}
                />
                <DetailLine
                  label="Host"
                  value={detailParsed ? `${detailParsed.host}${detailParsed.port ? `:${detailParsed.port}` : ''}` : '—'}
                />
              </Stack>
            </Grid>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" gutterBottom>
                Access & Metadata
              </Typography>
              <Stack spacing={1.5}>
                <DetailLine
                  label="Username"
                  value={detailParsed?.username ? detailParsed.username : '—'}
                />
                <DetailLine label="Status" value={selected.active ? 'Active' : 'Disabled'} />
                <DetailLine label="Notes" value={selected.notes ?? '—'} />
                <DetailLine
                  label="Last Updated"
                  value={selected.updatedAt ? new Date(selected.updatedAt).toLocaleString() : '—'}
                />
              </Stack>
            </Grid>
          </Grid>
        </Paper>
      )}

      {canManage && (
        <SystemConnectionForm
          open={formOpen}
          title={formMode === 'create' ? 'Create Connection' : 'Edit Connection'}
          systems={systems}
          initialValues={formMode === 'edit' ? selected : null}
          loading={saving}
          testing={testing}
          onClose={handleFormClose}
          onSubmit={handleFormSubmit}
          onTest={handleFormTest}
        />
      )}

      {canManage && (
        <ConfirmDialog
          open={confirmOpen}
          title="Delete Connection"
          description={`Are you sure you want to delete this connection? This action cannot be undone.`}
          confirmLabel="Delete"
          onClose={() => setConfirmOpen(false)}
          onConfirm={handleConfirmDelete}
          loading={deleting}
        />
      )}
    </Box>
  );
};

interface DetailLineProps {
  label: string;
  value: string;
}

const DetailLine = ({ label, value }: DetailLineProps) => (
  <Box>
    <Typography variant="subtitle2" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="body1">{value}</Typography>
    <Divider sx={{ my: 1.5 }} />
  </Box>
);

export default SystemConnectionsPage;
