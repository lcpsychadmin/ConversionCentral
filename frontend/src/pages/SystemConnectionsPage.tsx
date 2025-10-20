import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Paper,
  Stack,
  Typography
} from '@mui/material';
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
import {
  ConnectionCatalogSelectionInput,
  ConnectionCatalogTable as CatalogRow,
  ConnectionTablePreview,
  System,
  SystemConnection,
  SystemConnectionFormValues
} from '../types/data';
import { useAuth } from '../context/AuthContext';
import {
  fetchSystemConnectionCatalog,
  fetchConnectionTablePreview,
  updateSystemConnectionCatalogSelection
} from '../services/systemConnectionService';

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
  const theme = useTheme();

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
  const [catalogRows, setCatalogRows] = useState<CatalogRow[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogSaving, setCatalogSaving] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewTarget, setPreviewTarget] = useState<{ schemaName: string | null; tableName: string } | null>(null);
  const [previewConnectionId, setPreviewConnectionId] = useState<string | null>(null);
  const [previewData, setPreviewData] = useState<ConnectionTablePreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [detailModalOpen, setDetailModalOpen] = useState(false);

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

  useEffect(() => {
    if (
      !selected ||
      (previewConnectionId && selected.id !== previewConnectionId)
    ) {
      setPreviewOpen(false);
      setPreviewTarget(null);
      setPreviewConnectionId(null);
      setPreviewData(null);
      setPreviewError(null);
    }
  }, [selected, previewConnectionId]);

  const loadCatalog = useCallback(
    async (connectionId: string) => {
      setCatalogLoading(true);
      setCatalogError(null);
      try {
        const data = await fetchSystemConnectionCatalog(connectionId);
        setCatalogRows(data);
      } catch (error) {
        setCatalogError(getErrorMessage(error, 'Unable to load source catalog.'));
      } finally {
        setCatalogLoading(false);
      }
    },
    []
  );

  const loadPreview = useCallback(
    async (connectionId: string, schemaName: string | null, tableName: string) => {
      setPreviewLoading(true);
      setPreviewError(null);
      try {
        const data = await fetchConnectionTablePreview(connectionId, schemaName, tableName);
        setPreviewData(data);
      } catch (error) {
        setPreviewError(getErrorMessage(error, 'Unable to load preview.'));
      } finally {
        setPreviewLoading(false);
      }
    },
    []
  );

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

  const handleCatalogSelectionChange = useCallback(
    async (nextSelection: string[]) => {
      if (!selected) return;

      const selectionSet = new Set(nextSelection);
      const payload: ConnectionCatalogSelectionInput[] = catalogRows
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
      } catch (error) {
        setCatalogError(getErrorMessage(error, 'Unable to save selection.'));
      } finally {
        setCatalogSaving(false);
      }
    },
    [selected, catalogRows, loadCatalog]
  );

  const handleCatalogRefresh = useCallback(() => {
    if (selected) {
      loadCatalog(selected.id);
    }
  }, [selected, loadCatalog]);

  const handlePreviewRequest = useCallback(
    (row: CatalogRow) => {
      if (!selected) return;
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
    },
    [selected, loadPreview]
  );

  const handlePreviewRefresh = useCallback(() => {
    if (!selected || !previewTarget) return;
    loadPreview(selected.id, previewTarget.schemaName, previewTarget.tableName);
  }, [selected, previewTarget, loadPreview]);

  const handlePreviewClose = useCallback(() => {
    setPreviewOpen(false);
    setPreviewTarget(null);
    setPreviewConnectionId(null);
    setPreviewData(null);
    setPreviewError(null);
  }, []);

  const handleSelect = (connection: SystemConnection | null) => {
    setSelected(connection);
  };

  const handleOpenDetail = (connection: SystemConnection) => {
    setSelected(connection);
    setDetailModalOpen(true);
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
      active: values.active,
      ingestionEnabled: values.ingestionEnabled
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

  const detailSystem = selected ? systemLookup.get(selected.systemId) : null;

  return (
    <Box>
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
          borderBottom: `3px solid ${theme.palette.primary.main}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
        }}
      >
        <Typography variant="h4" gutterBottom sx={{ color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }}>
          Data Source Connections
        </Typography>
        <Typography variant="body2" sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}>
          Register JDBC connections for relational sources. Use these connections when ingesting tables.
        </Typography>
      </Box>

      {canManage && (
        <Box sx={{ mb: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            variant="contained"
            onClick={handleCreateClick}
            disabled={busy || systems.length === 0}
          >
            New Connection
          </Button>
        </Box>
      )}

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

      <Paper elevation={3} sx={{ 
        p: 3, 
        mb: 3,
        background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
      }}>
        <SystemConnectionTable
          data={sortedConnections}
          systems={systems}
          loading={loading}
          selectedId={selected?.id ?? null}
          canManage={canManage}
          onSelect={handleSelect}
          onViewDetail={handleOpenDetail}
          onEdit={canManage ? handleEdit : undefined}
          onDelete={canManage ? handleDelete : undefined}
          onTest={canManage ? handleTestConnection : undefined}
        />
      </Paper>

      {selected && (
        <>
          <Paper
            elevation={3}
            sx={{
              p: 3,
              mb: 3,
              background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
            }}
          >
            <ConnectionCatalogTable
              rows={catalogRows}
              loading={catalogLoading}
              saving={catalogSaving}
              error={catalogError}
              onRefresh={handleCatalogRefresh}
              onSelectionChange={handleCatalogSelectionChange}
              onPreview={handlePreviewRequest}
            />
          </Paper>

          {selected.ingestionEnabled && (
            <Paper
              elevation={3}
              sx={{
                p: 3,
                mb: 3,
                background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.08)} 0%, ${alpha(theme.palette.info.main, 0.04)} 100%)`
              }}
            >
              <ConnectionIngestionPanel
                connection={selected}
                system={detailSystem}
                catalogRows={catalogRows}
              />
            </Paper>
          )}

          <ConnectionDataPreviewDialog
            open={previewOpen}
            schemaName={previewTarget?.schemaName ?? null}
            tableName={previewTarget?.tableName ?? 'Table'}
            loading={previewLoading}
            error={previewError}
            preview={previewData}
            onClose={handlePreviewClose}
            onRefresh={handlePreviewRefresh}
          />
        </>
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

      <SystemConnectionDetailModal
        open={detailModalOpen}
        connection={selected}
        system={detailSystem}
        onClose={() => setDetailModalOpen(false)}
      />
    </Box>
  );
};

export default SystemConnectionsPage;
