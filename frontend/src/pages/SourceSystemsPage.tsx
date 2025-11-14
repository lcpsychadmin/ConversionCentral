import { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, MenuItem, Paper, TextField, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';

import PageHeader from '../components/common/PageHeader';
import SystemTable from '../components/system/SystemTable';
import SystemForm from '../components/system/SystemForm';
import SystemConnectionTable from '../components/system-connection/SystemConnectionTable';
import SystemConnectionDetailModal from '../components/system-connection/SystemConnectionDetailModal';
import ConnectionCatalogTable from '../components/system-connection/ConnectionCatalogTable';
import SystemConnectionForm from '../components/system-connection/SystemConnectionForm';
import ConnectionDataPreviewDialog from '../components/system-connection/ConnectionDataPreviewDialog';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useAuth } from '../context/AuthContext';
import {
  fetchSystemConnectionCatalog,
  fetchConnectionTablePreview,
  updateSystemConnectionCatalogSelection
} from '../services/systemConnectionService';
import {
  ConnectionCatalogSelectionInput,
  ConnectionCatalogTable as CatalogRow,
  ConnectionTablePreview,
  System,
  SystemConnection,
  SystemConnectionFormValues,
  SystemFormValues
} from '../types/data';
import { getPanelSurface, getSectionSurface } from '../theme/surfaceStyles';

const IGNORED_SYSTEM_PHYSICAL_NAMES = new Set(['databricks_warehouse']);

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

const SourceSystemsPage = () => {
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const sectionSurface = useMemo(
    () => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }),
    [isDarkMode, theme]
  );
  const catalogSurface = useMemo(
    () => getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }),
    [isDarkMode, theme]
  );

  // Systems (applications)
  const {
    systemsQuery,
    createSystem,
    updateSystem,
    deleteSystem,
    creating: systemsCreating,
    updating: systemsUpdating,
    deleting: systemsDeleting
  } = useSystems();

  const {
    data: systems = [],
    isLoading: systemsLoading,
    isError: systemsError,
    error: systemsErrorObj
  } = systemsQuery;

  const ignoredSystemIds = useMemo(() => {
    return new Set(
      systems
        .filter((system) => {
          const physical = system.physicalName?.trim().toLowerCase();
          return Boolean(physical && IGNORED_SYSTEM_PHYSICAL_NAMES.has(physical));
        })
        .map((system) => system.id)
    );
  }, [systems]);

  const visibleSystems = useMemo(
    () => systems.filter((system) => !ignoredSystemIds.has(system.id)),
    [ignoredSystemIds, systems]
  );

  const [selectedSystem, setSelectedSystem] = useState<System | null>(null);
  const [systemFormMode, setSystemFormMode] = useState<'create' | 'edit'>('create');
  const [systemFormOpen, setSystemFormOpen] = useState(false);
  const [systemConfirmOpen, setSystemConfirmOpen] = useState(false);

  const sortedSystems = useMemo(
    () => visibleSystems.slice().sort((a, b) => (a.name ?? '').localeCompare(b.name ?? '')),
    [visibleSystems]
  );

  useEffect(() => {
    if (selectedSystem && !visibleSystems.some((system) => system.id === selectedSystem.id)) {
      setSelectedSystem(null);
    }
  }, [selectedSystem, visibleSystems]);

  const handleSystemSelect = (system: System | null) => {
    setSelectedSystem(system);
  };

  const handleSystemCreateClick = () => {
    setSystemFormMode('create');
    setSelectedSystem(null);
    setSystemFormOpen(true);
  };

  const handleSystemEdit = (system: System) => {
    setSystemFormMode('edit');
    setSelectedSystem(system);
    setSystemFormOpen(true);
  };

  const handleSystemDeleteRequest = (system: System) => {
    setSelectedSystem(system);
    setSystemConfirmOpen(true);
  };

  const handleSystemFormClose = () => {
    setSystemFormOpen(false);
  };

  const handleSystemFormSubmit = async (values: SystemFormValues) => {
    try {
      if (systemFormMode === 'create') {
        await createSystem({
          name: values.name,
          physicalName: values.physicalName,
          description: values.description ?? null,
          systemType: values.systemType ?? null,
          status: values.status,
          securityClassification: values.securityClassification ?? null
        });
      } else if (selectedSystem) {
        await updateSystem({
          id: selectedSystem.id,
          input: {
            name: values.name,
            physicalName: values.physicalName,
            description: values.description ?? null,
            systemType: values.systemType ?? null,
            status: values.status,
            securityClassification: values.securityClassification ?? null
          }
        });
      }
      setSystemFormOpen(false);
    } catch (error) {
      // notifications handled in hook
    }
  };

  const handleSystemConfirmDelete = async () => {
    if (!selectedSystem) return;
    try {
      await deleteSystem(selectedSystem.id);
      setSystemConfirmOpen(false);
      setSelectedSystem(null);
    } catch (error) {
      // notifications handled in hook
    }
  };

  const systemBusy = systemsCreating || systemsUpdating || systemsDeleting;
  const systemErrorMessage = systemsError
    ? getErrorMessage(systemsErrorObj, 'Unable to load applications.')
    : null;

  // Connections
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
  const [connectionConfirmOpen, setConnectionConfirmOpen] = useState(false);
  const [catalogRows, setCatalogRows] = useState<CatalogRow[]>([]);
  const [catalogCache, setCatalogCache] = useState<Record<string, CatalogRow[]>>({});
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
  const [schemaFilter, setSchemaFilter] = useState<string>('__ALL__');

  const systemLookup = useMemo(
    () => new Map<string, System>(visibleSystems.map((system) => [system.id, system])),
    [visibleSystems]
  );

  const visibleConnections = useMemo(
    () => connections.filter((connection) => !ignoredSystemIds.has(connection.systemId)),
    [connections, ignoredSystemIds]
  );

  const sortedConnections = useMemo(() => {
    return visibleConnections
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
  }, [systemLookup, visibleConnections]);

  const filteredConnections = useMemo(() => {
    if (selectedSystem) {
      return sortedConnections.filter((connection) => connection.systemId === selectedSystem.id);
    }
    return sortedConnections;
  }, [sortedConnections, selectedSystem]);

  const availableSchemaFilters = useMemo(() => {
    if (!selectedSystem) {
      return [] as string[];
    }
    const schemaSet = new Set<string>();
    const relevantConnections = sortedConnections.filter((connection) => connection.systemId === selectedSystem.id);
    relevantConnections.forEach((connection) => {
      if (connection.connectionType !== 'jdbc') return;
      const catalog = catalogCache[connection.id];
      if (!catalog) return;
      catalog.forEach((row) => {
        const schema = row.schemaName?.trim() || '(No Schema)';
        schemaSet.add(schema);
      });
    });
    return Array.from(schemaSet).sort((a, b) => a.localeCompare(b));
  }, [catalogCache, selectedSystem, sortedConnections]);

  const filteredConnectionsBySchema = useMemo(() => {
    if (!selectedSystem) {
      return filteredConnections;
    }
    if (!schemaFilter || schemaFilter === '__ALL__') {
      return filteredConnections;
    }
    const normalizedFilter = schemaFilter === '__NO_SCHEMA__' ? null : schemaFilter;
    return filteredConnections.filter((connection) => {
      if (connection.connectionType !== 'jdbc') {
        return false;
      }
      const selection = catalogCache[connection.id];
      if (!selection || selection.length === 0) {
        return false;
      }
      return selection.some((row) => {
        const schema = row.schemaName?.trim() || null;
        if (!schema && normalizedFilter === null) {
          return true;
        }
        return schema === normalizedFilter;
      });
    });
  }, [catalogCache, filteredConnections, schemaFilter, selectedSystem]);

  useEffect(() => {
    if (
      selectedConnection &&
      !filteredConnectionsBySchema.some((connection) => connection.id === selectedConnection.id)
    ) {
      setSelectedConnection(null);
    }
  }, [filteredConnectionsBySchema, selectedConnection]);

  useEffect(() => {
    if (selectedSystem) {
      const available = availableSchemaFilters;
      if (available.length === 0) {
        setSchemaFilter('__ALL__');
        return;
      }
      if (
        schemaFilter !== '__ALL__' &&
        schemaFilter !== '__NO_SCHEMA__' &&
        !available.includes(schemaFilter)
      ) {
        setSchemaFilter('__ALL__');
      }
    } else {
      setSchemaFilter('__ALL__');
    }
  }, [availableSchemaFilters, schemaFilter, selectedSystem]);

  useEffect(() => {
    if (!selectedConnection || (previewConnectionId && selectedConnection.id !== previewConnectionId)) {
      setPreviewOpen(false);
      setPreviewTarget(null);
      setPreviewConnectionId(null);
      setPreviewData(null);
      setPreviewError(null);
    }
  }, [selectedConnection, previewConnectionId]);

  const loadCatalog = useCallback(
    async (connectionId: string) => {
      setCatalogLoading(true);
      setCatalogError(null);
      try {
        const data = await fetchSystemConnectionCatalog(connectionId);
        setCatalogRows(data);
        setCatalogCache((prev) => ({ ...prev, [connectionId]: data }));
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

  const canBrowseCatalog = useMemo(() => {
    if (!selectedConnection) return false;
    return selectedConnection.connectionType === 'jdbc';
  }, [selectedConnection]);

  useEffect(() => {
    if (!selectedConnection || !canBrowseCatalog) {
      setCatalogRows([]);
      setCatalogError(null);
      setCatalogLoading(false);
      setCatalogSaving(false);
      return;
    }
    loadCatalog(selectedConnection.id);
  }, [selectedConnection, canBrowseCatalog, loadCatalog]);

  const handleCatalogSelectionChange = useCallback(
    async (nextSelection: string[]) => {
      if (!selectedConnection) return;

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
        await updateSystemConnectionCatalogSelection(selectedConnection.id, payload);
        await loadCatalog(selectedConnection.id);
      } catch (error) {
        setCatalogError(getErrorMessage(error, 'Unable to save selection.'));
      } finally {
        setCatalogSaving(false);
      }
    },
    [selectedConnection, catalogRows, loadCatalog]
  );

  const handleCatalogRefresh = useCallback(() => {
    if (selectedConnection && canBrowseCatalog) {
      loadCatalog(selectedConnection.id);
    }
  }, [selectedConnection, canBrowseCatalog, loadCatalog]);

  const handlePreviewRequest = useCallback(
    (row: CatalogRow) => {
      if (!selectedConnection || !canBrowseCatalog) return;
      const schemaName = row.schemaName?.trim() ? row.schemaName : null;
      const target = {
        schemaName,
        tableName: row.tableName
      };
      setPreviewTarget(target);
      setPreviewData(null);
      setPreviewError(null);
      setPreviewOpen(true);
      setPreviewConnectionId(selectedConnection.id);
      loadPreview(selectedConnection.id, target.schemaName, target.tableName);
    },
    [selectedConnection, canBrowseCatalog, loadPreview]
  );

  const handlePreviewRefresh = useCallback(() => {
    if (!selectedConnection || !previewTarget || !canBrowseCatalog) return;
    loadPreview(selectedConnection.id, previewTarget.schemaName, previewTarget.tableName);
  }, [selectedConnection, previewTarget, canBrowseCatalog, loadPreview]);

  const handlePreviewClose = useCallback(() => {
    setPreviewOpen(false);
    setPreviewTarget(null);
    setPreviewConnectionId(null);
    setPreviewData(null);
    setPreviewError(null);
  }, []);

  const handleConnectionSelect = (connection: SystemConnection | null) => {
    setSelectedConnection(connection);
  };

  const handleConnectionOpenDetail = (connection: SystemConnection) => {
    setSelectedConnection(connection);
    setDetailModalOpen(true);
  };

  const handleConnectionCreateClick = () => {
    setConnectionFormMode('create');
    setSelectedConnection(null);
    setConnectionFormOpen(true);
  };

  const handleConnectionEdit = (connection: SystemConnection) => {
    setConnectionFormMode('edit');
    setSelectedConnection(connection);
    setConnectionFormOpen(true);
  };

  const handleConnectionDeleteRequest = (connection: SystemConnection) => {
    setSelectedConnection(connection);
    setConnectionConfirmOpen(true);
  };

  const handleConnectionFormClose = () => {
    setConnectionFormOpen(false);
  };

  const handleConnectionFormSubmit = async (values: SystemConnectionFormValues, connectionString: string) => {
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
      if (connectionFormMode === 'create') {
        await createConnection(payload);
      } else if (selectedConnection) {
        await updateConnection({
          id: selectedConnection.id,
          input: payload
        });
      }
      setConnectionFormOpen(false);
    } catch (error) {
      // notifications handled in hooks
    }
  };

  const handleConnectionFormTest = async (_values: SystemConnectionFormValues, connectionString: string) => {
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

  const connectionErrorMessage = connectionsError
    ? getErrorMessage(connectionsErrorObj, 'Unable to load connections.')
    : null;

  const detailSystem = selectedConnection ? systemLookup.get(selectedConnection.systemId) : null;

  return (
    <Box>
      <PageHeader
        title="Source Systems"
        subtitle="Manage the catalog of applications alongside their source connections and ingestion metadata."
      />

      {systemErrorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {systemErrorMessage}
        </Alert>
      )}

      <Paper
        elevation={0}
        sx={{
          p: 3,
          mb: 3,
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
            Applications
          </Typography>
          {canManage && (
            <Button variant="contained" onClick={handleSystemCreateClick} disabled={systemBusy}>
              New Application
            </Button>
          )}
        </Box>

        <SystemTable
          data={sortedSystems}
          loading={systemsLoading}
          selectedId={selectedSystem?.id ?? null}
          canManage={canManage}
          onSelect={handleSystemSelect}
          onEdit={canManage ? handleSystemEdit : undefined}
          onDelete={canManage ? handleSystemDeleteRequest : undefined}
        />
      </Paper>

      <Box
        sx={{
          mt: selectedSystem ? 3 : 5
        }}
      >
  {canManage && !combinedLoading && visibleSystems.length === 0 && (
          <Alert severity="info" sx={{ mb: 3 }}>
            Add an application before registering a connection.
          </Alert>
        )}

        {connectionErrorMessage && (
          <Alert severity="error" sx={{ mb: 3 }}>
            {connectionErrorMessage}
          </Alert>
        )}

        <Paper
          elevation={0}
          sx={{
            p: 3,
            mb: 3,
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
                disabled={connectionBusy || visibleSystems.length === 0}
              >
                New Connection
              </Button>
            )}
          </Box>

          <SystemConnectionTable
            data={filteredConnectionsBySchema}
            systems={visibleSystems}
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
      </Box>

      {selectedConnection && (
        <>
          {canBrowseCatalog ? (
            <>
              {!selectedConnection.ingestionEnabled && (
                <Alert severity="info" sx={{ mb: 2 }}>
                  This connection is configured as read-only. Only SELECT access is used when browsing or previewing tables.
                </Alert>
              )}
              {selectedSystem && availableSchemaFilters.length > 0 && (
                <Box display="flex" alignItems="center" gap={2} mb={3}>
                  <TextField
                    select
                    label="Filter by schema"
                    size="small"
                    value={schemaFilter}
                    onChange={(event) => {
                      const value = event.target.value;
                      setSchemaFilter(value);
                      setSelectedConnection(null);
                    }}
                    sx={{ minWidth: 220 }}
                  >
                    <MenuItem value="__ALL__">All Schemas</MenuItem>
                    {availableSchemaFilters.map((schema) => (
                      <MenuItem
                        key={schema === '(No Schema)' ? '__NO_SCHEMA__' : schema}
                        value={schema === '(No Schema)' ? '__NO_SCHEMA__' : schema}
                      >
                        {schema}
                      </MenuItem>
                    ))}
                  </TextField>
                </Box>
              )}
              <Paper
                elevation={0}
                sx={{
                  p: 3,
                  mb: 3,
                  borderRadius: 3,
                  ...catalogSurface
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
            </>
          ) : (
            <Alert severity="info" sx={{ mb: 3 }}>
              Catalog browsing is disabled for this connection.
            </Alert>
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
        <>
          <SystemForm
            open={systemFormOpen}
            title={systemFormMode === 'create' ? 'Create Application' : 'Edit Application'}
            initialValues={systemFormMode === 'edit' ? selectedSystem : null}
            loading={systemBusy}
            onClose={handleSystemFormClose}
            onSubmit={handleSystemFormSubmit}
          />

          <ConfirmDialog
            open={systemConfirmOpen}
            title="Delete Application"
            description={`Are you sure you want to delete "${selectedSystem?.name ?? ''}"? This action cannot be undone.`}
            confirmLabel="Delete"
            onClose={() => setSystemConfirmOpen(false)}
            onConfirm={handleSystemConfirmDelete}
            loading={systemsDeleting}
          />

          <SystemConnectionForm
            open={connectionFormOpen}
            title={connectionFormMode === 'create' ? 'Create Connection' : 'Edit Connection'}
            systems={visibleSystems}
            initialValues={connectionFormMode === 'edit' ? selectedConnection : null}
            loading={connectionSaving}
            testing={testing}
            onClose={handleConnectionFormClose}
            onSubmit={handleConnectionFormSubmit}
            onTest={handleConnectionFormTest}
          />

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

      <SystemConnectionDetailModal
        open={detailModalOpen}
        connection={selectedConnection}
        system={detailSystem}
        onClose={() => setDetailModalOpen(false)}
      />
    </Box>
  );
};

export default SourceSystemsPage;
