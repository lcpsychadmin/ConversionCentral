import { useEffect, useMemo, useState } from 'react';
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
  Stack
} from '@mui/material';
import { SelectChangeEvent } from '@mui/material/Select';
import { useTheme } from '@mui/material/styles';

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

  const [selectedConnectionId, setSelectedConnectionId] = useState<string>('');
  const [catalogRows, setCatalogRows] = useState<ConnectionCatalogTable[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [catalogRefreshIndex, setCatalogRefreshIndex] = useState(0);

  const selectedConnection = useMemo(
    () => schedulableConnections.find((connection) => connection.id === selectedConnectionId) ?? null,
    [schedulableConnections, selectedConnectionId]
  );

  useEffect(() => {
    if (schedulableConnections.length === 0) {
      setSelectedConnectionId('');
      return;
    }
    if (!selectedConnectionId || !schedulableConnections.some((connection) => connection.id === selectedConnectionId)) {
      setSelectedConnectionId(schedulableConnections[0].id);
    }
  }, [schedulableConnections, selectedConnectionId]);

  useEffect(() => {
    if (!selectedConnection) {
      setCatalogRows([]);
      setCatalogError(null);
      setCatalogLoading(false);
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

  const loading = systemsLoading || connectionsLoading;
  const primaryError = systemsError ? systemsErrorObj : connectionsError ? connectionsErrorObj : null;
  const errorMessage = primaryError ? getErrorMessage(primaryError, 'Unable to load ingestion schedules.') : null;

  const handleConnectionChange = (event: SelectChangeEvent<string>) => {
    const value = event.target.value ?? '';
    setSelectedConnectionId(value);
    setCatalogRefreshIndex((previous) => previous + 1);
  };

  const handleRefreshCatalog = () => {
    setCatalogRefreshIndex((previous) => previous + 1);
  };

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
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ xs: 'stretch', md: 'flex-end' }}>
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
                catalogRows={catalogRows}
              />
            </Paper>
          )}
        </>
      )}
    </Box>
  );
};

export default IngestionSchedulesPage;
