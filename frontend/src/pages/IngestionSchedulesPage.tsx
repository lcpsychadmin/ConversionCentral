import { SyntheticEvent, useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Stack,
  Typography,
  Chip
} from '@mui/material';
import { SelectChangeEvent } from '@mui/material/Select';
import { useTheme } from '@mui/material/styles';
import TreeView from '@mui/lab/TreeView';
import TreeItem from '@mui/lab/TreeItem';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';

import ConnectionIngestionPanel from '../components/system-connection/ConnectionIngestionPanel';
import { useSystemConnections } from '../hooks/useSystemConnections';
import { useSystems } from '../hooks/useSystems';
import { useAuth } from '../context/AuthContext';
import { ConnectionCatalogTable, System } from '../types/data';
import { fetchSystemConnectionCatalog } from '../services/systemConnectionService';
import { getPanelSurface, getSectionSurface } from '../theme/surfaceStyles';
import PageHeader from '../components/common/PageHeader';

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

const SCHEMA_NO_VALUE = '__NO_SCHEMA__';

const buildSystemNodeId = (systemId: string) => `system:${systemId}`;
const buildConnectionNodeId = (connectionId: string) => `connection:${connectionId}`;
const buildSchemaNodeId = (connectionId: string, schemaKey: string) => `schema:${connectionId}:${schemaKey}`;

const encodeSchemaKey = (schemaName: string | null | undefined): string => {
  if (!schemaName || schemaName.trim().length === 0) {
    return SCHEMA_NO_VALUE;
  }
  return encodeURIComponent(schemaName.trim());
};

const decodeSchemaKey = (schemaKey: string): string | null => {
  if (schemaKey === SCHEMA_NO_VALUE) {
    return null;
  }
  try {
    return decodeURIComponent(schemaKey);
  } catch {
    return schemaKey;
  }
};

const formatSchemaLabel = (schemaKey: string): string => {
  const decoded = decodeSchemaKey(schemaKey);
  return decoded === null ? '(No Schema)' : decoded;
};

const IngestionSchedulesPage = () => {
  const theme = useTheme();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');
  const isDarkMode = theme.palette.mode === 'dark';
  const sectionSurface = useMemo(() => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);
  const panelSurface = useMemo(() => getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);

  const {
    systemsQuery: { data: systems = [], isLoading: systemsLoading, isError: systemsError, error: systemsErrorObj }
  } = useSystems();

  const {
    connectionsQuery: {
      data: connections = [],
      isLoading: connectionsLoading,
      isError: connectionsError,
      error: connectionsErrorObj
    }
  } = useSystemConnections();

  const schedulableConnections = useMemo(() => {
    const systemMap = new Map<string, string>(systems.map((system) => [system.id, system.name]));
    return connections
      .filter((connection) => connection.ingestionEnabled && connection.connectionType === 'jdbc')
      .slice()
      .sort((a, b) => {
        const systemA = systemMap.get(a.systemId) ?? '';
        const systemB = systemMap.get(b.systemId) ?? '';
        if (systemA !== systemB) {
          return systemA.localeCompare(systemB);
        }
        const connA = a.connectionString ?? '';
        const connB = b.connectionString ?? '';
        return connA.localeCompare(connB);
      });
  }, [connections, systems]);

  const systemsWithConnections = useMemo(() => {
    const sortedSystems = systems
      .slice()
      .sort((a, b) => (a.name ?? '').localeCompare(b.name ?? ''));

    return sortedSystems
      .map((system) => {
        const related = schedulableConnections
          .filter((connection) => connection.systemId === system.id)
          .slice()
          .sort((a, b) => (a.connectionString ?? '').localeCompare(b.connectionString ?? ''));
        return { system, connections: related };
      })
      .filter((entry) => entry.connections.length > 0);
  }, [schedulableConnections, systems]);

  const [selectedConnectionId, setSelectedConnectionId] = useState<string>('');
  const [catalogRows, setCatalogRows] = useState<ConnectionCatalogTable[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [catalogRefreshIndex, setCatalogRefreshIndex] = useState(0);
  const [catalogCache, setCatalogCache] = useState<Record<string, ConnectionCatalogTable[]>>({});
  const [expandedNodes, setExpandedNodes] = useState<string[]>([]);
  const [treeSelection, setTreeSelection] = useState<string | null>(null);
  const [selectedSchemaKey, setSelectedSchemaKey] = useState<string | null>(null);

  const selectedConnection = useMemo(
    () => schedulableConnections.find((connection) => connection.id === selectedConnectionId) ?? null,
    [schedulableConnections, selectedConnectionId]
  );

  useEffect(() => {
    if (schedulableConnections.length === 0) {
      setSelectedConnectionId('');
      setTreeSelection(null);
      setSelectedSchemaKey(null);
      return;
    }
    if (!selectedConnectionId || !schedulableConnections.some((connection) => connection.id === selectedConnectionId)) {
      setSelectedConnectionId(schedulableConnections[0].id);
      setCatalogRefreshIndex((previous) => previous + 1);
    }
  }, [schedulableConnections, selectedConnectionId]);

  useEffect(() => {
    if (!selectedConnection) {
      setCatalogRows([]);
      setCatalogError(null);
      setCatalogLoading(false);
      setSelectedSchemaKey(null);
      return;
    }

    let cancelled = false;
    const load = async () => {
      setCatalogLoading(true);
      setCatalogError(null);
      try {
        const data = await fetchSystemConnectionCatalog(selectedConnection.id);
        if (!cancelled) {
          setCatalogRows(data);
          setCatalogCache((previous) => ({ ...previous, [selectedConnection.id]: data }));
        }
      } catch (error) {
        if (!cancelled) {
          setCatalogError(getErrorMessage(error, 'Unable to load the catalog for this connection.'));
          setCatalogRows([]);
        }
      } finally {
        if (!cancelled) {
          setCatalogLoading(false);
        }
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [selectedConnection, catalogRefreshIndex]);

  const systemLookup = useMemo(() => new Map<string, System>(systems.map((system) => [system.id, system])), [systems]);
  const detailSystem = selectedConnection ? systemLookup.get(selectedConnection.systemId) ?? null : null;

  useEffect(() => {
    if (!selectedConnectionId) {
      setTreeSelection(null);
      return;
    }
    if (selectedSchemaKey) {
      setTreeSelection(buildSchemaNodeId(selectedConnectionId, selectedSchemaKey));
      return;
    }
    setTreeSelection(buildConnectionNodeId(selectedConnectionId));
  }, [selectedConnectionId, selectedSchemaKey]);

  useEffect(() => {
    if (!selectedConnection) {
      return;
    }
    const systemNodeId = buildSystemNodeId(selectedConnection.systemId);
    const connectionNodeId = buildConnectionNodeId(selectedConnection.id);
    setExpandedNodes((previous) => {
      const next = new Set(previous);
      next.add(systemNodeId);
      next.add(connectionNodeId);
      return Array.from(next);
    });
  }, [selectedConnection]);

  useEffect(() => {
    if (selectedSchemaKey === null) {
      return;
    }
    const exists = catalogRows.some((row) => encodeSchemaKey(row.schemaName) === selectedSchemaKey);
    if (!exists) {
      setSelectedSchemaKey(null);
    }
  }, [catalogRows, selectedSchemaKey]);

  const resolvedSchema = selectedSchemaKey === null ? undefined : decodeSchemaKey(selectedSchemaKey);

  const filteredCatalogRows = useMemo(() => {
    if (resolvedSchema === undefined) {
      return catalogRows;
    }
    return catalogRows.filter((row) => {
      const normalized = row.schemaName?.trim();
      if (!normalized) {
        return resolvedSchema === null;
      }
      return resolvedSchema !== null && normalized === resolvedSchema;
    });
  }, [catalogRows, resolvedSchema]);

  const activeSchemaLabel = useMemo(() => {
    if (selectedSchemaKey === null) {
      return null;
    }
    return formatSchemaLabel(selectedSchemaKey);
  }, [selectedSchemaKey]);

  const loading = systemsLoading || connectionsLoading;
  const primaryError = systemsError ? systemsErrorObj : connectionsError ? connectionsErrorObj : null;
  const errorMessage = primaryError ? getErrorMessage(primaryError, 'Unable to load ingestion schedules.') : null;

  const handleConnectionChange = (event: SelectChangeEvent<string>) => {
    const value = event.target.value ?? '';
    if (!value) {
      setSelectedConnectionId('');
      setSelectedSchemaKey(null);
      return;
    }
    if (value !== selectedConnectionId) {
      setSelectedConnectionId(value);
      setCatalogRefreshIndex((previous) => previous + 1);
    }
    setSelectedSchemaKey(null);
  };

  const handleRefreshCatalog = () => {
    setCatalogRefreshIndex((previous) => previous + 1);
  };

  const handleTreeNodeSelect = useCallback((_: SyntheticEvent, nodeId: string) => {
    if (nodeId.startsWith('system:')) {
      setTreeSelection(nodeId);
      return;
    }
    if (nodeId.startsWith('connection:')) {
      const [, connectionId] = nodeId.split(':');
      if (connectionId && connectionId !== selectedConnectionId) {
        setSelectedConnectionId(connectionId);
        setCatalogRefreshIndex((previous) => previous + 1);
      }
      setSelectedSchemaKey(null);
      setTreeSelection(nodeId);
      return;
    }
    if (nodeId.startsWith('schema:')) {
      const [, connectionId, schemaKey] = nodeId.split(':');
      if (connectionId && connectionId !== selectedConnectionId) {
        setSelectedConnectionId(connectionId);
        setCatalogRefreshIndex((previous) => previous + 1);
      }
      if (schemaKey) {
        setSelectedSchemaKey(schemaKey);
      }
      setTreeSelection(nodeId);
    }
  }, [selectedConnectionId]);

  const handleTreeNodeToggle = useCallback((_: SyntheticEvent, nodeIds: string[]) => {
    setExpandedNodes(nodeIds);
  }, []);

  return (
    <Box>
      <PageHeader
        title="Ingestion Schedules"
        subtitle="Manage recurring ingestion for JDBC connections. Configure table selections on the Connections page, then schedule loads here."
      />

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {loading && (
        <Paper elevation={2} sx={{ p: 4, display: 'flex', justifyContent: 'center', mb: 3 }}>
          <CircularProgress />
        </Paper>
      )}

      {!loading && schedulableConnections.length === 0 && (
        <Alert severity="info">
          No ingestion-enabled JDBC connections found. Enable ingestion on a connection from the Connections page to begin scheduling loads.
        </Alert>
      )}

      {!loading && schedulableConnections.length > 0 && (
        <Paper
          elevation={0}
          sx={{
            p: 3,
            mb: 3,
            borderRadius: 3,
            ...sectionSurface
          }}
        >
          <Stack direction={{ xs: 'column', xl: 'row' }} spacing={3} alignItems={{ xs: 'stretch', xl: 'flex-start' }}>
            <Box sx={{ flex: 1, minWidth: { xs: '100%', xl: 280 } }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                Browse connections
              </Typography>
              <Paper
                variant="outlined"
                sx={{
                  borderRadius: 2,
                  maxHeight: 320,
                  overflowY: 'auto',
                  p: 1
                }}
              >
                <TreeView
                  aria-label="Source system tree"
                  defaultCollapseIcon={<ExpandMoreIcon fontSize="small" />}
                  defaultExpandIcon={<ChevronRightIcon fontSize="small" />}
                  selected={treeSelection ?? undefined}
                  expanded={expandedNodes}
                  onNodeSelect={handleTreeNodeSelect}
                  onNodeToggle={handleTreeNodeToggle}
                  sx={{
                    minHeight: 200,
                    '& .MuiTreeItem-label': {
                      fontSize: 14,
                      color: isDarkMode ? theme.palette.grey[100] : theme.palette.text.primary
                    },
                    '& .MuiTreeItem-iconContainer svg': {
                      color: isDarkMode ? theme.palette.grey[400] : theme.palette.text.secondary
                    },
                    '& .MuiTreeItem-content.Mui-selected > .MuiTreeItem-label': {
                      fontWeight: 600,
                      color: isDarkMode ? theme.palette.primary.light : theme.palette.primary.main
                    }
                  }}
                >
                  {systemsWithConnections.length === 0 && (
                    <TreeItem nodeId="empty" label="No ingestion-enabled connections found" disabled />
                  )}
                  {systemsWithConnections.map(({ system, connections: relatedConnections }) => (
                    <TreeItem
                      key={system.id}
                      nodeId={buildSystemNodeId(system.id)}
                      label={system.name ?? 'Unnamed System'}
                    >
                      {relatedConnections.map((connection) => {
                        const cachedRows = catalogCache[connection.id];
                        const schemaAggregates = cachedRows
                          ? Array.from(
                              cachedRows.reduce((acc, row) => {
                                const key = encodeSchemaKey(row.schemaName);
                                acc.set(key, (acc.get(key) ?? 0) + 1);
                                return acc;
                              }, new Map<string, number>())
                            ).sort((a, b) => formatSchemaLabel(a[0]).localeCompare(formatSchemaLabel(b[0])))
                          : null;
                        const connectionLabel = connection.connectionString || 'Connection';
                        return (
                          <TreeItem
                            key={connection.id}
                            nodeId={buildConnectionNodeId(connection.id)}
                            label={connectionLabel}
                          >
                            {schemaAggregates && schemaAggregates.length > 0 &&
                              schemaAggregates.map(([schemaKey, count]) => (
                                <TreeItem
                                  key={`${connection.id}-${schemaKey}`}
                                  nodeId={buildSchemaNodeId(connection.id, schemaKey)}
                                  label={`${formatSchemaLabel(schemaKey)} (${count})`}
                                />
                              ))}
                            {schemaAggregates && schemaAggregates.length === 0 && (
                              <TreeItem
                                key={`${connection.id}-empty`}
                                nodeId={`empty:${connection.id}`}
                                label="No catalog tables available"
                                disabled
                              />
                            )}
                            {schemaAggregates === null && (
                              <TreeItem
                                key={`${connection.id}-loading`}
                                nodeId={`loading:${connection.id}`}
                                label={
                                  selectedConnection?.id === connection.id && catalogLoading
                                    ? 'Loading schemas...'
                                    : 'Schemas load after selecting the connection'
                                }
                                disabled
                              />
                            )}
                          </TreeItem>
                        );
                      })}
                    </TreeItem>
                  ))}
                </TreeView>
              </Paper>
            </Box>

            <Stack spacing={2} sx={{ flex: 1, minWidth: 0 }}>
              <Box>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                  Active filters
                </Typography>
                <Stack direction="row" spacing={1} flexWrap="wrap">
                  {selectedConnection && (
                    <Chip
                      label={`Connection: ${selectedConnection.connectionString}`}
                      size="small"
                      color="primary"
                    />
                  )}
                  {activeSchemaLabel && (
                    <Chip
                      label={`Schema: ${activeSchemaLabel}`}
                      size="small"
                      onDelete={() => setSelectedSchemaKey(null)}
                    />
                  )}
                  {!selectedConnection && (
                    <Typography variant="body2" color="text.secondary">
                      Select a connection to view available schedules.
                    </Typography>
                  )}
                </Stack>
              </Box>

              <FormControl fullWidth>
                <InputLabel id="ingestion-connection-select-label">Connection</InputLabel>
                <Select
                  labelId="ingestion-connection-select-label"
                  label="Connection"
                  value={selectedConnectionId}
                  onChange={handleConnectionChange}
                >
                  {schedulableConnections.map((connection) => {
                    const system = systemLookup.get(connection.systemId);
                    const systemName = system?.name ?? 'Unknown system';
                    return (
                      <MenuItem key={connection.id} value={connection.id}>
                        {systemName} - {connection.connectionString}
                      </MenuItem>
                    );
                  })}
                </Select>
              </FormControl>

              <Button
                variant="outlined"
                onClick={handleRefreshCatalog}
                disabled={!selectedConnection || catalogLoading}
              >
                {catalogLoading ? 'Refreshing...' : 'Refresh Catalog'}
              </Button>
            </Stack>
          </Stack>
        </Paper>
      )}

      {catalogError && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {catalogError}
        </Alert>
      )}

      {selectedConnection && (
        <>
          {catalogLoading && (
            <Paper elevation={2} sx={{ p: 4, display: 'flex', justifyContent: 'center', mb: 3 }}>
              <CircularProgress />
            </Paper>
          )}

          {!catalogLoading && (
            <Paper
              elevation={0}
              sx={{
                p: 3,
                borderRadius: 3,
                ...panelSurface
              }}
            >
              {!canManage && (
                <Alert severity="info" sx={{ mb: 2 }}>
                  You have read-only access. Only administrators can create or modify ingestion schedules.
                </Alert>
              )}

              <ConnectionIngestionPanel
                connection={selectedConnection}
                system={detailSystem}
                catalogRows={filteredCatalogRows}
              />
            </Paper>
          )}
        </>
      )}
    </Box>
  );
};

export default IngestionSchedulesPage;
