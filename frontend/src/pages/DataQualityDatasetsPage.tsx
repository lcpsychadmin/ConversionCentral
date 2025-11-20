import { MouseEvent, useCallback, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  CircularProgress,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { useMutation, useQuery } from 'react-query';
import { Link as RouterLink } from 'react-router-dom';
import { fetchDatasetHierarchy, startDataObjectProfileRuns } from '@services/dataQualityService';
import {
  DataQualityDatasetApplication,
  DataQualityDatasetDefinition,
  DataQualityDatasetObject,
  DataQualityDatasetProductTeam,
  DataQualityDatasetTable
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import PageHeader from '../components/common/PageHeader';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

const formatDescription = (value?: string | null) =>
  value && value.trim().length > 0 ? value : 'No description provided.';

const formatTablePrimary = (table: DataQualityDatasetTable) => {
  const schemaPrefix = table.schemaName ? `${table.schemaName}.` : '';
  return `${schemaPrefix}${table.tableName}`;
};

const formatTableSecondary = (table: DataQualityDatasetTable) => {
  const details: string[] = [];
  if (table.alias && table.alias !== table.tableName) {
    details.push(`Alias: ${table.alias}`);
  }
  if (table.description) {
    details.push(table.description);
  }
  if (table.tableType) {
    details.push(`Type: ${table.tableType}`);
  }
  if (table.isConstructed) {
    details.push('Constructed table');
  }
  if (table.loadOrder !== undefined && table.loadOrder !== null) {
    details.push(`Load order ${table.loadOrder}`);
  }
  return details.join(' • ');
};

const TablesList = ({ tables }: { tables: DataQualityDatasetTable[] }) => {
  if (!tables.length) {
    return <Typography color="text.secondary">No tables registered yet.</Typography>;
  }

  return (
    <List dense disablePadding>
      {tables.map((table) => (
        <ListItem key={table.dataDefinitionTableId} divider>
          <ListItemText
            primary={formatTablePrimary(table)}
            secondary={formatTableSecondary(table) || undefined}
          />
        </ListItem>
      ))}
    </List>
  );
};

const DataDefinitionSection = ({ definition }: { definition: DataQualityDatasetDefinition }) => (
  <Paper variant="outlined" sx={{ p: 2 }}>
    <Stack spacing={1.5}>
      <Stack spacing={0.5}>
        <Typography variant="subtitle1" fontWeight={600}>
          Data Definition
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {formatDescription(definition.description)}
        </Typography>
      </Stack>
      <TablesList tables={definition.tables} />
    </Stack>
  </Paper>
);

interface DataObjectAccordionProps {
  dataObject: DataQualityDatasetObject;
  onRunProfiling?: (dataObjectId: string) => void;
  isProfiling?: boolean;
}

const DataObjectAccordion = ({ dataObject, onRunProfiling, isProfiling }: DataObjectAccordionProps) => {
  const hasTables = dataObject.dataDefinitions.some((definition) => definition.tables.length > 0);

  const handleRunProfiling = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!hasTables || !onRunProfiling) {
      return;
    }
    onRunProfiling(dataObject.dataObjectId);
  };

  return (
    <Accordion>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" width="100%">
          <Stack spacing={0.5} pr={2}>
            <Typography fontWeight={600}>{dataObject.name}</Typography>
            <Typography variant="body2" color="text.secondary">
              {formatDescription(dataObject.description)}
            </Typography>
          </Stack>
          {onRunProfiling ? (
            <Button
              size="small"
              variant="contained"
              onClick={handleRunProfiling}
              disabled={!hasTables || isProfiling}
              startIcon={isProfiling ? <CircularProgress size={14} color="inherit" /> : undefined}
            >
              {isProfiling ? 'Running…' : 'Run Profiling'}
            </Button>
          ) : null}
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <Stack spacing={2}>
          {dataObject.dataDefinitions.length > 0 ? (
            dataObject.dataDefinitions.map((definition) => (
              <DataDefinitionSection key={definition.dataDefinitionId} definition={definition} />
            ))
          ) : (
            <Typography color="text.secondary">No data definitions registered.</Typography>
          )}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

interface ApplicationAccordionProps {
  application: DataQualityDatasetApplication;
  onRunProfiling?: (dataObjectId: string) => void;
  profilingState?: { activeId: string | null; isLoading: boolean };
}

const ApplicationAccordion = ({ application, onRunProfiling, profilingState }: ApplicationAccordionProps) => (
  <Accordion>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Stack spacing={0.5}>
        <Typography fontWeight={600}>{application.name}</Typography>
        <Typography variant="body2" color="text.secondary">
          {`Physical name: ${application.physicalName}`} ·{' '}
          {`${application.dataObjects.length} data object${application.dataObjects.length === 1 ? '' : 's'}`}
        </Typography>
        {application.description ? (
          <Typography variant="body2" color="text.secondary">
            {application.description}
          </Typography>
        ) : null}
      </Stack>
    </AccordionSummary>
    <AccordionDetails>
      <Stack spacing={1.5}>
        {application.dataObjects.length > 0 ? (
          application.dataObjects.map((dataObject) => (
            <DataObjectAccordion
              key={dataObject.dataObjectId}
              dataObject={dataObject}
              onRunProfiling={onRunProfiling}
              isProfiling={
                Boolean(profilingState?.isLoading && profilingState?.activeId === dataObject.dataObjectId)
              }
            />
          ))
        ) : (
          <Typography color="text.secondary">No data objects mapped yet.</Typography>
        )}
      </Stack>
    </AccordionDetails>
  </Accordion>
);

interface ProductTeamAccordionProps {
  productTeam: DataQualityDatasetProductTeam;
  onRunProfiling?: (dataObjectId: string) => void;
  profilingState?: { activeId: string | null; isLoading: boolean };
}

const ProductTeamAccordion = ({
  productTeam,
  onRunProfiling,
  profilingState
}: ProductTeamAccordionProps) => (
  <Accordion defaultExpanded>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Stack spacing={0.5}>
        <Typography fontWeight={600}>{productTeam.name}</Typography>
        <Typography variant="body2" color="text.secondary">
          {formatDescription(productTeam.description)}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {`${productTeam.applications.length} application${productTeam.applications.length === 1 ? '' : 's'}`}
        </Typography>
      </Stack>
    </AccordionSummary>
    <AccordionDetails>
      <Stack spacing={1.5}>
        {productTeam.applications.length > 0 ? (
          productTeam.applications.map((application) => (
            <ApplicationAccordion
              key={application.applicationId}
              application={application}
              onRunProfiling={onRunProfiling}
              profilingState={profilingState}
            />
          ))
        ) : (
          <Typography color="text.secondary">No applications configured yet.</Typography>
        )}
      </Stack>
    </AccordionDetails>
  </Accordion>
);

const DataQualityDatasetsPage = () => {
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();
  const [profilingTargetId, setProfilingTargetId] = useState<string | null>(null);

  const runProfilingMutation = useMutation(startDataObjectProfileRuns, {
    onMutate: (dataObjectId: string) => {
      setProfilingTargetId(dataObjectId);
    },
    onSuccess: (result) => {
      const started = result.profileRuns.length;
      const skipped = result.skippedTableIds.length;
      const messageParts = [
        started > 0
          ? `Profiling started for ${started} table group${started === 1 ? '' : 's'}.`
          : 'No profiling runs were started.'
      ];
      if (skipped > 0) {
        messageParts.push(
          `${skipped} table${skipped === 1 ? '' : 's'} skipped due to missing connections.`
        );
      }
      showSuccess(messageParts.join(' '));
    },
    onError: (error) => {
      showError(`Unable to start profiling: ${resolveErrorMessage(error)}`);
    },
    onSettled: () => {
      setProfilingTargetId(null);
    }
  });

  const handleRunProfiling = useCallback(
    (dataObjectId: string) => {
      runProfilingMutation.mutate(dataObjectId);
    },
    [runProfilingMutation]
  );

  const profilingState = {
    activeId: profilingTargetId,
    isLoading: runProfilingMutation.isLoading
  };

  const hierarchyQuery = useQuery<DataQualityDatasetProductTeam[]>(
    ['data-quality', 'dataset-hierarchy'],
    fetchDatasetHierarchy,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        showError(`Unable to load dataset hierarchy: ${resolveErrorMessage(error)}`);
      }
    }
  );

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Data Quality Datasets"
        subtitle="Explore product teams, applications, data objects, and their registered data definitions synchronized from Conversion Central."
        actions={
          <Button component={RouterLink} to="/data-quality/profiling-runs" variant="outlined">
            View Profiling Runs
          </Button>
        }
      />

      {hierarchyQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : hierarchyQuery.isError ? (
        <Alert severity="error">Unable to load the dataset hierarchy.</Alert>
      ) : hierarchyQuery.data && hierarchyQuery.data.length > 0 ? (
        <Stack spacing={2}>
          {hierarchyQuery.data.map((productTeam) => (
            <ProductTeamAccordion
              key={productTeam.productTeamId}
              productTeam={productTeam}
              onRunProfiling={handleRunProfiling}
              profilingState={profilingState}
            />
          ))}
        </Stack>
      ) : (
        <Paper elevation={0} sx={{ p: 3 }}>
          <Typography color="text.secondary">
            No dataset metadata has been captured yet. Create data definitions to populate this view.
          </Typography>
        </Paper>
      )}
      {snackbar}
    </Stack>
  );
};

export default DataQualityDatasetsPage;
