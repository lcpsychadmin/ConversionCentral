import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  CircularProgress,
  FormControl,
  Grid,
  InputLabel,
  List,
  ListItem,
  ListItemText,
  MenuItem,
  Paper,
  Select,
  Stack,
  Typography
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { useQuery } from 'react-query';
import {
  fetchDataQualityConnections,
  fetchDataQualityProjects,
  fetchDataQualityTableGroups,
  fetchDataQualityTables
} from '@services/dataQualityService';
import {
  DataQualityConnection,
  DataQualityProject,
  DataQualityTable,
  DataQualityTableGroup
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';
const formatLabel = (value?: string | null) => (value && value.trim().length > 0 ? value : '—');

const TableGroupPanel = ({
  group,
  onError
}: {
  group: DataQualityTableGroup;
  onError?: (error: unknown) => void;
}) => {
  const tablesQuery = useQuery<DataQualityTable[]>(
    ['data-quality', 'tables', group.tableGroupId],
    () => fetchDataQualityTables(group.tableGroupId),
    {
      staleTime: 2 * 60 * 1000,
      onError
    }
  );

  return (
    <Accordion sx={{ bgcolor: 'background.default' }}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Stack spacing={0.5}>
          <Typography fontWeight={600}>{group.name}</Typography>
          <Typography variant="body2" color="text.secondary">
            {group.description ?? 'No description provided.'}
          </Typography>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        {tablesQuery.isLoading ? (
          <Box display="flex" justifyContent="center" py={2}>
            <CircularProgress size={24} />
          </Box>
        ) : tablesQuery.isError ? (
          <Alert severity="error">Failed to load tables.</Alert>
        ) : tablesQuery.data && tablesQuery.data.length > 0 ? (
          <List dense disablePadding>
            {tablesQuery.data.map((table) => (
              <ListItem key={table.tableId} divider>
                <ListItemText
                  primary={`${table.schemaName ?? 'default'}.${table.tableName}`}
                  secondary={
                    table.sourceTableId
                      ? `Linked to source table ${table.sourceTableId}`
                      : 'Derived from ingestion selections'
                  }
                />
              </ListItem>
            ))}
          </List>
        ) : (
          <Typography color="text.secondary">No tables registered yet.</Typography>
        )}
      </AccordionDetails>
    </Accordion>
  );
};

const ConnectionPanel = ({
  connection,
  onError
}: {
  connection: DataQualityConnection;
  onError?: (context: 'groups' | 'tables', error: unknown) => void;
}) => {
  const groupsQuery = useQuery<DataQualityTableGroup[]>(
    ['data-quality', 'table-groups', connection.connectionId],
    () => fetchDataQualityTableGroups(connection.connectionId),
    {
      staleTime: 2 * 60 * 1000,
      onError: (error) => onError?.('groups', error)
    }
  );

  return (
    <Accordion>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Stack spacing={0.5}>
          <Typography fontWeight={600}>{connection.name}</Typography>
          <Typography variant="body2" color="text.secondary">
            Catalog: {formatLabel(connection.catalog)} · Schema: {formatLabel(connection.schemaName)}
          </Typography>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        {groupsQuery.isLoading ? (
          <Box display="flex" justifyContent="center" py={2}>
            <CircularProgress size={24} />
          </Box>
        ) : groupsQuery.isError ? (
          <Alert severity="error">Failed to load table groups.</Alert>
        ) : groupsQuery.data && groupsQuery.data.length > 0 ? (
          <Stack spacing={1.5}>
            {groupsQuery.data.map((group) => (
              <TableGroupPanel
                key={group.tableGroupId}
                group={group}
                onError={(error) => onError?.('tables', error)}
              />
            ))}
          </Stack>
        ) : (
          <Typography color="text.secondary">No table groups registered.</Typography>
        )}
      </AccordionDetails>
    </Accordion>
  );
};

const useProjects = (onError?: (error: unknown) => void) =>
  useQuery<DataQualityProject[]>(['data-quality', 'projects'], fetchDataQualityProjects, {
    staleTime: 5 * 60 * 1000,
    onError
  });

const useConnections = (projectKey: string | null, onError?: (error: unknown) => void) =>
  useQuery<DataQualityConnection[]>(
    ['data-quality', 'connections', projectKey],
    () => fetchDataQualityConnections(projectKey ?? ''),
    {
      enabled: Boolean(projectKey),
      staleTime: 2 * 60 * 1000,
      onError
    }
  );

const DataQualityDatasetsPage = () => {
  const { snackbar, showError } = useSnackbarFeedback();

  const handleProjectsError = useCallback(
    (error: unknown) => {
      showError(`Unable to load TestGen projects: ${resolveErrorMessage(error)}`);
    },
    [showError]
  );

  const handleConnectionsError = useCallback(
    (error: unknown) => {
      showError(`Failed to load connections for the selected project: ${resolveErrorMessage(error)}`);
    },
    [showError]
  );

  const projectsQuery = useProjects(handleProjectsError);
  const [selectedProjectKey, setSelectedProjectKey] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedProjectKey && projectsQuery.data && projectsQuery.data.length > 0) {
      setSelectedProjectKey(projectsQuery.data[0].projectKey);
    }
  }, [projectsQuery.data, selectedProjectKey]);

  const connectionsQuery = useConnections(selectedProjectKey, handleConnectionsError);

  const handleNestedError = useCallback(
    (context: 'groups' | 'tables', error: unknown) => {
      const prefix = context === 'groups' ? 'Failed to load table groups' : 'Failed to load tables';
      showError(`${prefix}: ${resolveErrorMessage(error)}`);
    },
    [showError]
  );

  const selectedProject = useMemo(() => {
    if (!projectsQuery.data || !selectedProjectKey) {
      return null;
    }
    return projectsQuery.data.find((project) => project.projectKey === selectedProjectKey) ?? null;
  }, [projectsQuery.data, selectedProjectKey]);

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h4" gutterBottom>
          Data Quality Datasets
        </Typography>
        <Typography color="text.secondary">
          Explore the TestGen project hierarchy, connections, and registered tables sourced from
          your ingestion selections.
        </Typography>
      </Box>

      <Paper elevation={1} sx={{ p: 3 }}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6} lg={4}>
            <FormControl fullWidth>
              <InputLabel id="dq-project-select-label">Project</InputLabel>
              <Select
                labelId="dq-project-select-label"
                label="Project"
                value={selectedProjectKey ?? ''}
                onChange={(event) => setSelectedProjectKey(event.target.value || null)}
                disabled={projectsQuery.isLoading || !projectsQuery.data?.length}
              >
                {projectsQuery.data?.map((project) => (
                  <MenuItem key={project.projectKey} value={project.projectKey}>
                    {project.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={6} lg={8}>
            <Typography variant="body1" sx={{ mt: { xs: 2, md: 0 } }}>
              {selectedProject?.description ?? 'Select a project to inspect its datasets.'}
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {projectsQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : projectsQuery.isError ? (
        <Alert severity="error">Unable to load TestGen projects.</Alert>
      ) : connectionsQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : connectionsQuery.isError ? (
        <Alert severity="error">Failed to load connections for the selected project.</Alert>
      ) : connectionsQuery.data && connectionsQuery.data.length > 0 ? (
        <Stack spacing={1.5}>
          {connectionsQuery.data.map((connection) => (
            <ConnectionPanel
              key={connection.connectionId}
              connection={connection}
              onError={handleNestedError}
            />
          ))}
        </Stack>
      ) : (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Typography color="text.secondary">
            No active connections have been synchronized for this project yet.
          </Typography>
        </Paper>
      )}
      {snackbar}
    </Stack>
  );
};

export default DataQualityDatasetsPage;
