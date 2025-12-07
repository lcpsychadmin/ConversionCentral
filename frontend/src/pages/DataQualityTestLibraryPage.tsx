import {
  Box,
  CircularProgress,
  Paper,
  Stack,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Alert,
  Button
} from '@mui/material';
import { useCallback } from 'react';
import { useQuery } from 'react-query';
import {
  fetchDataQualityProjects,
  fetchRecentTestRuns
} from '@services/dataQualityService';
import { DataQualityProject, DataQualityTestRun } from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import PageHeader from '../components/common/PageHeader';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
};

const ProjectTestRuns = ({
  project,
  onError,
  onRefreshSuccess
}: {
  project: DataQualityProject;
  onError: (error: unknown) => void;
  onRefreshSuccess: () => void;
}) => {
  const { data, isLoading, isError, refetch } = useQuery<DataQualityTestRun[]>(
    ['data-quality', 'test-library', project.projectKey],
    () => fetchRecentTestRuns(project.projectKey, 25),
    {
      staleTime: 2 * 60 * 1000,
      onError
    }
  );

  const handleRetry = async () => {
    try {
      const result = await refetch();
      if (result.error) {
        onError(result.error);
        return;
      }
      onRefreshSuccess();
    } catch (err) {
      onError(err);
    }
  };

  return (
    <Paper elevation={1} sx={{ p: 3 }}>
      <Stack spacing={1.5}>
        <Box>
          <Typography variant="h6">{project.name}</Typography>
          <Typography variant="body2" color="text.secondary">
            {project.description ?? 'No description provided.'}
          </Typography>
        </Box>
        {isLoading ? (
          <Box display="flex" justifyContent="center" py={2}>
            <CircularProgress size={24} />
          </Box>
        ) : isError ? (
          <Alert
            severity="error"
            action={
              <Button color="inherit" size="small" onClick={handleRetry}>
                Retry
              </Button>
            }
          >
            Unable to load test runs for this project.
          </Alert>
        ) : data && data.length > 0 ? (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Run ID</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Started</TableCell>
                <TableCell>Completed</TableCell>
                <TableCell align="right">Failed</TableCell>
                <TableCell align="right">Total</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.map((run) => (
                <TableRow key={run.testRunId} hover>
                  <TableCell>{run.testRunId}</TableCell>
                  <TableCell>{run.status}</TableCell>
                  <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                  <TableCell>{formatDateTime(run.completedAt)}</TableCell>
                  <TableCell align="right">{run.failedTests ?? 0}</TableCell>
                  <TableCell align="right">{run.totalTests ?? 0}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Typography color="text.secondary">No test executions captured yet.</Typography>
        )}
      </Stack>
    </Paper>
  );
};

const DataQualityTestLibraryPage = () => {
  const { snackbar, showSuccess, showError } = useSnackbarFeedback();

  const projectsQuery = useQuery<DataQualityProject[]>(
    ['data-quality', 'projects'],
    fetchDataQualityProjects,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) =>
        showError(`Unable to load TestGen projects: ${resolveErrorMessage(error)}`)
    }
  );

  const handleProjectRunsError = useCallback(
    (projectName: string, error: unknown) => {
      showError(`Unable to load test runs for ${projectName}: ${resolveErrorMessage(error)}`);
    },
    [showError]
  );

  const handleProjectRunsRefreshSuccess = useCallback(
    (projectName: string) => {
      showSuccess(`Refreshed test runs for ${projectName}.`);
    },
    [showSuccess]
  );

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Test Library"
        subtitle="Review recent TestGen executions per project while automated rule coverage continues to expand."
      />

      {projectsQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : projectsQuery.isError ? (
        <Alert severity="error">Unable to load TestGen projects.</Alert>
      ) : projectsQuery.data && projectsQuery.data.length > 0 ? (
        <Stack spacing={2}>
          {projectsQuery.data.map((project) => (
            <ProjectTestRuns
              key={project.projectKey}
              project={project}
              onError={(error) => handleProjectRunsError(project.name, error)}
              onRefreshSuccess={() => handleProjectRunsRefreshSuccess(project.name)}
            />
          ))}
        </Stack>
      ) : (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Typography color="text.secondary">
            No TestGen projects configured yet. Synchronize source
            systems to begin generating tests.
          </Typography>
        </Paper>
      )}
      {snackbar}
    </Stack>
  );
};

export default DataQualityTestLibraryPage;
