import { MouseEvent, ReactNode, useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  InputAdornment,
  Paper,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import SearchIcon from '@mui/icons-material/Search';
import { SimpleTreeView } from '@mui/x-tree-view/SimpleTreeView';
import { TreeItem } from '@mui/x-tree-view/TreeItem';
import { useMutation, useQueries, UseQueryResult, useQuery } from 'react-query';
import { Link as RouterLink } from 'react-router-dom';
import {
  fetchDataQualityColumnProfile,
  fetchDataQualityProfileRuns,
  fetchDataQualityTableContext,
  fetchDatasetHierarchy,
  startDataObjectProfileRuns
} from '@services/dataQualityService';
import {
  DataQualityColumnProfile,
  DataQualityDatasetApplication,
  DataQualityDatasetDefinition,
  DataQualityDatasetField,
  DataQualityDatasetObject,
  DataQualityDatasetProductTeam,
  DataQualityDatasetTable,
  DataQualityDatasetTableContext,
  DataQualityProfileRunListResponse
} from '@cc-types/data';
import useSnackbarFeedback from '@hooks/useSnackbarFeedback';
import ColumnProfilePanel from '@components/data-quality/ColumnProfilePanel';
import PageHeader from '../components/common/PageHeader';

const resolveErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unexpected error encountered.';

const formatDescription = (value?: string | null) =>
  value && value.trim().length > 0 ? value : 'No description provided.';

const TREE_SEARCH_DEFAULT_HELPER = 'Press Enter to jump to the first match.';

const STATUS_RUNNING = ['running', 'queued', 'pending', 'in_progress', 'starting'];
const STATUS_FAILED = ['failed', 'error', 'errored', 'cancelled', 'canceled'];

const isTerminalStatus = (status?: string | null) => {
  const normalized = (status ?? '').toLowerCase();
  if (!normalized) {
    return false;
  }
  if (STATUS_RUNNING.includes(normalized)) {
    return false;
  }
  return true;
};

const formatStatusLabel = (status?: string | null) => {
  if (!status) {
    return 'Pending';
  }
  const normalized = status.replace(/_/g, ' ').trim();
  return normalized.length ? normalized[0].toUpperCase() + normalized.slice(1) : 'Pending';
};

const resolveStatusChipColor = (
  status?: string | null
): 'default' | 'info' | 'success' | 'warning' | 'error' => {
  const normalized = (status ?? '').toLowerCase();
  if (!normalized || STATUS_RUNNING.includes(normalized)) {
    return 'info';
  }
  if (STATUS_FAILED.includes(normalized)) {
    return 'error';
  }
  return 'success';
};

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '—';
  }
  return parsed.toLocaleString();
};

const describeStatusTimestamps = (startedAt?: string | null, completedAt?: string | null) => {
  if (startedAt && completedAt) {
    return `Started ${formatDateTime(startedAt)} · Completed ${formatDateTime(completedAt)}`;
  }
  if (startedAt) {
    return `Started ${formatDateTime(startedAt)} · Waiting for completion`;
  }
  return 'Waiting for Databricks job to launch…';
};

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

const getDataObjectTables = (
  dataObject: DataQualityDatasetObject
): { table: DataQualityDatasetTable; definition: DataQualityDatasetDefinition }[] => {
  return dataObject.dataDefinitions.flatMap((definition) =>
    definition.tables.map((table) => ({ table, definition }))
  );
};

const summarizeTableFields = (table: DataQualityDatasetTable) => ({
  fieldCount: table.fields.length
});

const summarizeDataObject = (dataObject: DataQualityDatasetObject) => {
  const tables = getDataObjectTables(dataObject);
  const fieldCount = tables.reduce((total, entry) => total + entry.table.fields.length, 0);
  return {
    definitionCount: dataObject.dataDefinitions.length,
    tableCount: tables.length,
    fieldCount
  };
};

const summarizeApplication = (application: DataQualityDatasetApplication) => {
  const dataObjects = application.dataObjects;
  const tables = dataObjects.flatMap((dataObject) => getDataObjectTables(dataObject));
  const fieldCount = tables.reduce((total, entry) => total + entry.table.fields.length, 0);
  return {
    dataObjectCount: dataObjects.length,
    tableCount: tables.length,
    fieldCount
  };
};

const summarizeProductTeam = (productTeam: DataQualityDatasetProductTeam) => {
  const applications = productTeam.applications;
  const dataObjects = applications.flatMap((application) => application.dataObjects);
  const tables = dataObjects.flatMap((dataObject) => getDataObjectTables(dataObject));
  const fieldCount = tables.reduce((total, entry) => total + entry.table.fields.length, 0);
  return {
    applicationCount: applications.length,
    dataObjectCount: dataObjects.length,
    tableCount: tables.length,
    fieldCount
  };
};

const formatFieldSubtitle = (field: DataQualityDatasetField) => {
  const parts: string[] = [];
  if (field.fieldType) {
    parts.push(field.fieldLength ? `${field.fieldType}(${field.fieldLength})` : field.fieldType);
  }
  if (field.isUnique) {
    parts.push('Unique');
  }
  if (field.referenceTable) {
    parts.push(`Ref: ${field.referenceTable}`);
  }
  return parts.join(' • ');
};

type DatasetNodeType = 'root' | 'productTeam' | 'application' | 'dataObject' | 'table' | 'field';

interface DatasetNodeContext {
  id: string;
  type: DatasetNodeType;
  productTeam?: DataQualityDatasetProductTeam;
  application?: DataQualityDatasetApplication;
  dataObject?: DataQualityDatasetObject;
  dataDefinition?: DataQualityDatasetDefinition;
  table?: DataQualityDatasetTable;
  field?: DataQualityDatasetField;
}

const ROOT_NODE_ID = 'root';
const buildProductTeamNodeId = (productTeamId: string) => `pt:${productTeamId}`;
const buildApplicationNodeId = (productTeamId: string, applicationId: string) =>
  `pt:${productTeamId}::app:${applicationId}`;
const buildDataObjectNodeId = (
  productTeamId: string,
  applicationId: string,
  dataObjectId: string
) => `pt:${productTeamId}::app:${applicationId}::obj:${dataObjectId}`;
const buildTableNodeId = (tableId: string) => `table:${tableId}`;
const buildFieldNodeId = (fieldId: string) => `field:${fieldId}`;

const NODE_SEARCH_PRIORITY: Record<DatasetNodeType, number> = {
  field: 1,
  table: 2,
  dataObject: 3,
  application: 4,
  productTeam: 5,
  root: 99
};

interface HierarchySummary {
  productTeamCount: number;
  applicationCount: number;
  dataObjectCount: number;
  tableCount: number;
  fieldCount: number;
}

const summarizeHierarchy = (productTeams: DataQualityDatasetProductTeam[]): HierarchySummary => {
  const summary: HierarchySummary = {
    productTeamCount: productTeams.length,
    applicationCount: 0,
    dataObjectCount: 0,
    tableCount: 0,
    fieldCount: 0
  };

  productTeams.forEach((productTeam) => {
    summary.applicationCount += productTeam.applications.length;
    productTeam.applications.forEach((application) => {
      summary.dataObjectCount += application.dataObjects.length;
      application.dataObjects.forEach((dataObject) => {
        const tables = getDataObjectTables(dataObject);
        summary.tableCount += tables.length;
        summary.fieldCount += tables.reduce((total, entry) => total + entry.table.fields.length, 0);
      });
    });
  });

  return summary;
};

const renderCountChip = (value: number, label: string) => (
  <Chip
    size="small"
    variant="outlined"
    label={`${value.toLocaleString()} ${label}${value === 1 ? '' : 's'}`}
  />
);

const nodeMatchesSearch = (node: DatasetNodeContext, normalized: string) => {
  const values: Array<string | null | undefined> = [];
  switch (node.type) {
    case 'productTeam':
      values.push(node.productTeam?.name, node.productTeam?.description);
      break;
    case 'application':
      values.push(node.application?.name, node.application?.description, node.application?.physicalName);
      break;
    case 'dataObject':
      values.push(node.dataObject?.name, node.dataObject?.description);
      break;
    case 'table':
      if (node.table) {
        values.push(formatTablePrimary(node.table), formatTableSecondary(node.table), node.table.description);
      }
      break;
    case 'field':
      if (node.field) {
        values.push(
          node.field.name,
          node.field.description,
          node.field.fieldType,
          node.field.referenceTable,
          formatFieldSubtitle(node.field)
        );
      }
      break;
    default:
      values.push('All Process Areas');
      break;
  }
  return values
    .filter((value): value is string => Boolean(value && value.trim().length > 0))
    .some((value) => value.toLowerCase().includes(normalized));
};

const describeNode = (node: DatasetNodeContext): string => {
  switch (node.type) {
    case 'productTeam':
      return `team "${node.productTeam?.name ?? 'Team'}"`;
    case 'application':
      return `application "${node.application?.name ?? 'Application'}"`;
    case 'dataObject':
      return `data object "${node.dataObject?.name ?? 'Data Object'}"`;
    case 'table':
      return `table "${node.table ? formatTablePrimary(node.table) : 'Table'}"`;
    case 'field':
      return `field "${node.field?.name ?? 'Field'}"`;
    default:
      return 'catalog overview';
  }
};

const collectAncestorIds = (node: DatasetNodeContext): string[] => {
  const ids = new Set<string>([ROOT_NODE_ID]);
  if (node.productTeam) {
    ids.add(buildProductTeamNodeId(node.productTeam.productTeamId));
  }
  if (node.application && node.productTeam) {
    ids.add(buildApplicationNodeId(node.productTeam.productTeamId, node.application.applicationId));
  }
  if (node.dataObject && node.productTeam && node.application) {
    ids.add(
      buildDataObjectNodeId(
        node.productTeam.productTeamId,
        node.application.applicationId,
        node.dataObject.dataObjectId
      )
    );
  }
  if (node.table) {
    ids.add(buildTableNodeId(node.table.dataDefinitionTableId));
  }
  return Array.from(ids);
};

const StatTile = ({ label, value }: { label: string; value: ReactNode }) => (
  <Stack
    spacing={0.25}
    sx={{
      border: '1px solid',
      borderColor: 'divider',
      borderRadius: 1,
      p: 1,
      minWidth: 120
    }}
  >
    <Typography variant="subtitle1" fontWeight={600}>
      {value}
    </Typography>
    <Typography variant="caption" color="text.secondary">
      {label}
    </Typography>
  </Stack>
);

const DetailRow = ({ label, value }: { label: string; value?: ReactNode }) => (
  <Stack spacing={0.25}>
    <Typography variant="caption" color="text.secondary" sx={{ textTransform: 'uppercase', letterSpacing: 0.2 }}>
      {label}
    </Typography>
    <Typography variant="body2">{value ?? '—'}</Typography>
  </Stack>
);

const DataQualityDatasetsPage = () => {
  const { snackbar, showError, showSuccess } = useSnackbarFeedback();
  const [profilingTargetId, setProfilingTargetId] = useState<string | null>(null);
  const [monitoredRuns, setMonitoredRuns] = useState<
    { tableGroupId: string; profileRunId: string; status?: string | null }[]
  >([]);

  const monitorQueries = useQueries(
    monitoredRuns.map((run) => ({
      queryKey: ['data-quality', 'profiling-runs', 'monitor', run.tableGroupId],
      queryFn: () =>
        fetchDataQualityProfileRuns({
          tableGroupId: run.tableGroupId,
          limit: 5,
          includeGroups: false
        }),
      enabled: Boolean(monitoredRuns.length),
      refetchInterval: (data?: DataQualityProfileRunListResponse) => {
        const match = data?.runs.find((entry) => entry.profileRunId === run.profileRunId);
        return match && isTerminalStatus(match.status) ? false : 5000;
      }
    }))
  ) as UseQueryResult<DataQualityProfileRunListResponse>[];

  const monitoredRunStatuses = useMemo(() => {
    return monitoredRuns.map((run, idx) => {
      const query = monitorQueries[idx];
      const entry = query?.data?.runs.find((candidate) => candidate.profileRunId === run.profileRunId);
      return {
        ...run,
        status: entry?.status ?? run.status ?? null,
        startedAt: entry?.startedAt ?? null,
        completedAt: entry?.completedAt ?? null,
        isLoading: query?.isFetching ?? query?.isLoading ?? false
      };
    });
  }, [monitorQueries, monitoredRuns]);

  const monitoredError = monitorQueries.find((query) => query?.isError);
  const monitorErrorMessage = monitoredError ? resolveErrorMessage(monitoredError.error) : null;
  const hasPendingMonitoredRuns = monitoredRunStatuses.some((run) => !isTerminalStatus(run.status));
  const monitorIsFetching = monitorQueries.some((query) => query?.isFetching);

  const runProfilingMutation = useMutation(startDataObjectProfileRuns, {
    onMutate: (dataObjectId: string) => {
      setProfilingTargetId(dataObjectId);
    },
    onSuccess: (result) => {
      setMonitoredRuns((previous) => {
        const dedup = new Map(previous.map((run) => [run.profileRunId, run]));
        result.profileRuns.forEach((run) => {
          dedup.set(run.profileRunId, { ...run, status: 'queued' });
        });
        const entries = Array.from(dedup.values());
        return entries.length > 10 ? entries.slice(entries.length - 10) : entries;
      });
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

  const handleClearMonitoredRuns = () => {
    setMonitoredRuns([]);
  };

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

  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedItems, setExpandedItems] = useState<string[]>([ROOT_NODE_ID]);
  const [treeSearch, setTreeSearch] = useState('');
  const [treeSearchHelper, setTreeSearchHelper] = useState(TREE_SEARCH_DEFAULT_HELPER);
  const [treeSearchHasError, setTreeSearchHasError] = useState(false);

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

  const hierarchyData = useMemo(() => hierarchyQuery.data ?? [], [hierarchyQuery.data]);

  const { nodeLookup, hierarchySummary, defaultSelection } = useMemo(() => {
    const lookup = new Map<string, DatasetNodeContext>();
    lookup.set(ROOT_NODE_ID, { id: ROOT_NODE_ID, type: 'root' });
    let firstSelectable: string | null = null;

    hierarchyData.forEach((productTeam) => {
      const productTeamId = buildProductTeamNodeId(productTeam.productTeamId);
      lookup.set(productTeamId, { id: productTeamId, type: 'productTeam', productTeam });
      if (!firstSelectable) {
        firstSelectable = productTeamId;
      }

      productTeam.applications.forEach((application) => {
        const applicationId = buildApplicationNodeId(productTeam.productTeamId, application.applicationId);
        lookup.set(applicationId, {
          id: applicationId,
          type: 'application',
          productTeam,
          application
        });
        if (!firstSelectable) {
          firstSelectable = applicationId;
        }

        application.dataObjects.forEach((dataObject) => {
          const dataObjectId = buildDataObjectNodeId(
            productTeam.productTeamId,
            application.applicationId,
            dataObject.dataObjectId
          );
          lookup.set(dataObjectId, {
            id: dataObjectId,
            type: 'dataObject',
            productTeam,
            application,
            dataObject
          });
          if (!firstSelectable) {
            firstSelectable = dataObjectId;
          }

          dataObject.dataDefinitions.forEach((definition) => {
            definition.tables.forEach((table) => {
              const tableId = buildTableNodeId(table.dataDefinitionTableId);
              lookup.set(tableId, {
                id: tableId,
                type: 'table',
                productTeam,
                application,
                dataObject,
                dataDefinition: definition,
                table
              });
              if (!firstSelectable) {
                firstSelectable = tableId;
              }

              table.fields.forEach((field) => {
                const fieldId = buildFieldNodeId(field.dataDefinitionFieldId);
                lookup.set(fieldId, {
                  id: fieldId,
                  type: 'field',
                  productTeam,
                  application,
                  dataObject,
                  dataDefinition: definition,
                  table,
                  field
                });
                if (!firstSelectable) {
                  firstSelectable = fieldId;
                }
              });
            });
          });
        });
      });
    });

    return {
      nodeLookup: lookup,
      hierarchySummary: summarizeHierarchy(hierarchyData),
      defaultSelection: firstSelectable ?? ROOT_NODE_ID
    };
  }, [hierarchyData]);

  useEffect(() => {
    if (!hierarchyData.length) {
      setSelectedNodeId(null);
      setExpandedItems([]);
      return;
    }

    setSelectedNodeId((prev) => {
      if (prev && nodeLookup.has(prev)) {
        return prev;
      }
      return defaultSelection;
    });

    setExpandedItems((prev) => {
      if (prev.length > 0) {
        return prev;
      }
      const defaults = [ROOT_NODE_ID, ...hierarchyData.map((team) => buildProductTeamNodeId(team.productTeamId))];
      return defaults;
    });
  }, [hierarchyData, nodeLookup, defaultSelection]);

  const selectedContext = selectedNodeId ? nodeLookup.get(selectedNodeId) : nodeLookup.get(ROOT_NODE_ID);
  const autoExpandedItems = useMemo(() => {
    if (!hierarchyData.length) {
      return [];
    }
    const ids: string[] = [ROOT_NODE_ID];
    hierarchyData.forEach((productTeam) => {
      ids.push(buildProductTeamNodeId(productTeam.productTeamId));
      productTeam.applications.forEach((application) => {
        ids.push(buildApplicationNodeId(productTeam.productTeamId, application.applicationId));
      });
    });
    return ids;
  }, [hierarchyData]);

  const hasHierarchyData = hierarchyData.length > 0;

  const findNodeBySearch = useCallback(
    (query: string): DatasetNodeContext | null => {
      const normalized = query.trim().toLowerCase();
      if (!normalized) {
        return null;
      }
      let bestMatch: DatasetNodeContext | null = null;
      let bestPriority = Number.POSITIVE_INFINITY;
      for (const node of nodeLookup.values()) {
        if (!node || node.id === ROOT_NODE_ID) {
          continue;
        }
        if (!nodeMatchesSearch(node, normalized)) {
          continue;
        }
        const priority = NODE_SEARCH_PRIORITY[node.type] ?? 99;
        if (!bestMatch || priority < bestPriority) {
          bestMatch = node;
          bestPriority = priority;
        }
      }
      return bestMatch;
    },
    [nodeLookup]
  );

  const jumpToNode = useCallback(
    (node: DatasetNodeContext) => {
      setSelectedNodeId(node.id);
      const ancestors = collectAncestorIds(node);
      setExpandedItems((prev) => {
        const next = new Set(prev);
        ancestors.forEach((id) => next.add(id));
        return Array.from(next);
      });
    },
    [setSelectedNodeId, setExpandedItems]
  );

  const handleTreeSearchSubmit = useCallback(() => {
    const normalized = treeSearch.trim();
    if (!normalized) {
      setTreeSearchHelper('Enter a search term to locate datasets.');
      setTreeSearchHasError(true);
      return;
    }
    const match = findNodeBySearch(normalized);
    if (match) {
      jumpToNode(match);
      setTreeSearchHelper(`Jumped to ${describeNode(match)}.`);
      setTreeSearchHasError(false);
    } else {
      setTreeSearchHelper(`No matches found for "${normalized}".`);
      setTreeSearchHasError(true);
    }
  }, [treeSearch, findNodeBySearch, jumpToNode]);

  const selectedTableId = selectedContext?.table?.dataDefinitionTableId ?? null;
  const shouldLoadTableContext = Boolean(
    selectedTableId && (selectedContext?.type === 'table' || selectedContext?.type === 'field')
  );

  const tableContextQuery = useQuery(
    ['data-quality', 'table-context', selectedTableId],
    () => fetchDataQualityTableContext(selectedTableId!),
    {
      enabled: shouldLoadTableContext,
      staleTime: 5 * 60 * 1000
    }
  );

  const shouldLoadColumnProfile =
    selectedContext?.type === 'field' &&
    Boolean(tableContextQuery.data?.tableGroupId && selectedContext.field?.name);

  const columnProfileQuery = useQuery(
    [
      'data-quality',
      'column-profile',
      tableContextQuery.data?.tableGroupId,
      selectedContext?.field?.name
    ],
    () =>
      fetchDataQualityColumnProfile(
        tableContextQuery.data!.tableGroupId,
        selectedContext!.field!.name,
        tableContextQuery.data!.tableName,
        tableContextQuery.data!.physicalName
      ),
    {
      enabled: Boolean(shouldLoadColumnProfile),
      staleTime: 60 * 1000
    }
  );

  const tableContextError = tableContextQuery.isError ? resolveErrorMessage(tableContextQuery.error) : null;
  const fieldProfileError = columnProfileQuery.isError ? resolveErrorMessage(columnProfileQuery.error) : null;

  const tableContextDetail = shouldLoadTableContext ? tableContextQuery.data ?? null : null;
  const tableContextMeta = shouldLoadTableContext
    ? {
        isLoading: tableContextQuery.isLoading,
        error: tableContextError
      }
    : undefined;

  const columnProfileState = shouldLoadColumnProfile
    ? {
        profile: columnProfileQuery.data ?? null,
        isLoading: columnProfileQuery.isLoading,
        error: fieldProfileError
      }
    : undefined;

  const handleTreeNodeSelect = useCallback(
    (event: MouseEvent<HTMLElement>, nodeId: string) => {
      event.preventDefault();
      event.stopPropagation();
      setSelectedNodeId(nodeId);
    },
    []
  );

  interface NodeLabelProps {
    nodeId: string;
    title: string;
    subtitle?: string | null;
    meta?: ReactNode;
  }

  const renderNodeLabel = ({ nodeId, title, subtitle, meta }: NodeLabelProps) => {
    const isSelected = selectedNodeId === nodeId;
    return (
      <Box
        onClick={(event) => handleTreeNodeSelect(event, nodeId)}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          px: 0.75,
          py: 0.75,
          borderRadius: 1,
          cursor: 'pointer',
          bgcolor: isSelected ? 'action.selected' : 'transparent'
        }}
      >
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography
            variant="body2"
            sx={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
          >
            {title}
          </Typography>
          {subtitle ? (
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
            >
              {subtitle}
            </Typography>
          ) : null}
        </Box>
        {meta}
      </Box>
    );
  };

  const renderFieldItem = (field: DataQualityDatasetField) => {
    const nodeId = buildFieldNodeId(field.dataDefinitionFieldId);
    const subtitle = field.description?.trim() || formatFieldSubtitle(field);
    const fieldChipLabel = field.fieldType
      ? field.fieldLength
        ? `${field.fieldType} (${field.fieldLength})`
        : field.fieldType
      : null;
    return (
      <TreeItem
        key={nodeId}
        itemId={nodeId}
        label={renderNodeLabel({
          nodeId,
          title: field.name,
          subtitle: subtitle || undefined,
          meta: fieldChipLabel ? <Chip size="small" label={fieldChipLabel} /> : undefined
        })}
      />
    );
  };

  const renderTableItem = (definition: DataQualityDatasetDefinition, table: DataQualityDatasetTable) => {
    const nodeId = buildTableNodeId(table.dataDefinitionTableId);
    const subtitle = formatTableSecondary(table) || formatDescription(definition.description);
    const { fieldCount } = summarizeTableFields(table);
    return (
      <TreeItem
        key={nodeId}
        itemId={nodeId}
        label={renderNodeLabel({
          nodeId,
          title: formatTablePrimary(table),
          subtitle,
          meta: renderCountChip(fieldCount, 'field')
        })}
      >
        {table.fields.length > 0 ? (
          table.fields.map((field) => renderFieldItem(field))
        ) : (
          <TreeItem
            key={`${nodeId}::no-fields`}
            itemId={`${nodeId}::no-fields`}
            label={<Typography variant="caption" color="text.secondary">No fields registered.</Typography>}
            disabled
          />
        )}
      </TreeItem>
    );
  };

  const renderDataObjectItem = (
    productTeam: DataQualityDatasetProductTeam,
    application: DataQualityDatasetApplication,
    dataObject: DataQualityDatasetObject
  ) => {
    const nodeId = buildDataObjectNodeId(
      productTeam.productTeamId,
      application.applicationId,
      dataObject.dataObjectId
    );
    const summary = summarizeDataObject(dataObject);
    const tables = getDataObjectTables(dataObject);
    return (
      <TreeItem
        key={nodeId}
        itemId={nodeId}
        label={renderNodeLabel({
          nodeId,
          title: dataObject.name,
          subtitle: formatDescription(dataObject.description),
          meta: renderCountChip(summary.tableCount, 'table')
        })}
      >
        {tables.length ? (
          tables.map(({ definition, table }) => renderTableItem(definition, table))
        ) : (
          <TreeItem
            key={`${nodeId}::no-tables`}
            itemId={`${nodeId}::no-tables`}
            label={<Typography variant="caption" color="text.secondary">No tables mapped.</Typography>}
            disabled
          />
        )}
      </TreeItem>
    );
  };

  const renderApplicationItem = (
    productTeam: DataQualityDatasetProductTeam,
    application: DataQualityDatasetApplication
  ) => {
    const nodeId = buildApplicationNodeId(productTeam.productTeamId, application.applicationId);
    const summary = summarizeApplication(application);
    const subtitleParts = [application.description?.trim(), `Physical: ${application.physicalName}`].filter(Boolean);
    return (
      <TreeItem
        key={nodeId}
        itemId={nodeId}
        label={renderNodeLabel({
          nodeId,
          title: application.name,
          subtitle: subtitleParts.join(' • '),
          meta: renderCountChip(summary.dataObjectCount, 'data object')
        })}
      >
        {application.dataObjects.length ? (
          application.dataObjects.map((dataObject) => renderDataObjectItem(productTeam, application, dataObject))
        ) : (
          <TreeItem
            key={`${nodeId}::no-data-objects`}
            itemId={`${nodeId}::no-data-objects`}
            label={<Typography variant="caption" color="text.secondary">No data objects configured.</Typography>}
            disabled
          />
        )}
      </TreeItem>
    );
  };

  const renderProductTeamItem = (productTeam: DataQualityDatasetProductTeam) => {
    const nodeId = buildProductTeamNodeId(productTeam.productTeamId);
    const summary = summarizeProductTeam(productTeam);
    return (
      <TreeItem
        key={nodeId}
        itemId={nodeId}
        label={renderNodeLabel({
          nodeId,
          title: productTeam.name,
          subtitle: formatDescription(productTeam.description),
          meta: renderCountChip(summary.applicationCount, 'application')
        })}
      >
        {productTeam.applications.length ? (
          productTeam.applications.map((application) => renderApplicationItem(productTeam, application))
        ) : (
          <TreeItem
            key={`${nodeId}::no-applications`}
            itemId={`${nodeId}::no-applications`}
            label={<Typography variant="caption" color="text.secondary">No applications configured.</Typography>}
            disabled
          />
        )}
      </TreeItem>
    );
  };

  const renderRootNode = () => (
    <TreeItem
      key={ROOT_NODE_ID}
      itemId={ROOT_NODE_ID}
      label={renderNodeLabel({
        nodeId: ROOT_NODE_ID,
        title: 'All Process Areas',
        subtitle: `${hierarchySummary.productTeamCount.toLocaleString()} process area${
          hierarchySummary.productTeamCount === 1 ? '' : 's'
        }`,
        meta: (
          <Stack direction="row" spacing={0.5}>
            {renderCountChip(hierarchySummary.tableCount, 'table')}
            {renderCountChip(hierarchySummary.fieldCount, 'field')}
          </Stack>
        )
      })}
    >
      {hierarchyData.map((productTeam) => renderProductTeamItem(productTeam))}
    </TreeItem>
  );

  const renderRootDetail = () => (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h6" fontWeight={600}>
          Data Quality Catalog
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Select a process area, application, data object, table, or field from the tree to see metadata and profiling
          options.
        </Typography>
      </Box>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap">
        <StatTile label="Process Areas" value={hierarchySummary.productTeamCount.toLocaleString()} />
        <StatTile label="Applications" value={hierarchySummary.applicationCount.toLocaleString()} />
        <StatTile label="Data Objects" value={hierarchySummary.dataObjectCount.toLocaleString()} />
        <StatTile label="Tables" value={hierarchySummary.tableCount.toLocaleString()} />
        <StatTile label="Fields" value={hierarchySummary.fieldCount.toLocaleString()} />
      </Stack>
    </Stack>
  );

  const renderProductTeamDetail = (productTeam: DataQualityDatasetProductTeam) => {
    const summary = summarizeProductTeam(productTeam);
    return (
      <Stack spacing={2}>
        <Box>
          <Typography variant="h6" fontWeight={600}>
            {productTeam.name}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {formatDescription(productTeam.description)}
          </Typography>
        </Box>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap">
          <StatTile label="Applications" value={summary.applicationCount.toLocaleString()} />
          <StatTile label="Data Objects" value={summary.dataObjectCount.toLocaleString()} />
          <StatTile label="Tables" value={summary.tableCount.toLocaleString()} />
          <StatTile label="Fields" value={summary.fieldCount.toLocaleString()} />
        </Stack>
      </Stack>
    );
  };

  const renderApplicationDetail = (application: DataQualityDatasetApplication) => {
    const summary = summarizeApplication(application);
    return (
      <Stack spacing={2}>
        <Box>
          <Typography variant="h6" fontWeight={600}>
            {application.name}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {application.description ? application.description : `Physical name: ${application.physicalName}`}
          </Typography>
        </Box>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap">
          <StatTile label="Data Objects" value={summary.dataObjectCount.toLocaleString()} />
          <StatTile label="Tables" value={summary.tableCount.toLocaleString()} />
          <StatTile label="Fields" value={summary.fieldCount.toLocaleString()} />
        </Stack>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
          <DetailRow label="Physical Name" value={application.physicalName} />
          <DetailRow label="Description" value={formatDescription(application.description)} />
        </Stack>
      </Stack>
    );
  };

  const renderDataObjectDetail = (dataObject: DataQualityDatasetObject) => {
    const summary = summarizeDataObject(dataObject);
    const isProfilingTarget = Boolean(
      profilingState.isLoading && profilingState.activeId === dataObject.dataObjectId
    );
    return (
      <Stack spacing={2}>
        <Box>
          <Typography variant="h6" fontWeight={600}>
            {dataObject.name}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {formatDescription(dataObject.description)}
          </Typography>
        </Box>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap">
          <StatTile label="Data Definitions" value={summary.definitionCount.toLocaleString()} />
          <StatTile label="Tables" value={summary.tableCount.toLocaleString()} />
          <StatTile label="Fields" value={summary.fieldCount.toLocaleString()} />
        </Stack>
        <Stack direction="row" spacing={1} flexWrap="wrap">
          <Button
            variant="contained"
            onClick={() => handleRunProfiling(dataObject.dataObjectId)}
            disabled={summary.tableCount === 0 || profilingState.isLoading}
            startIcon={isProfilingTarget ? <CircularProgress size={16} color="inherit" /> : undefined}
          >
            {isProfilingTarget ? 'Running…' : 'Run Profiling'}
          </Button>
          <Button component={RouterLink} to="/data-quality/profiling-runs" variant="outlined">
            View Profiling Runs
          </Button>
        </Stack>
      </Stack>
    );
  };

  const renderTableDetail = (
    table: DataQualityDatasetTable,
    definition?: DataQualityDatasetDefinition,
    context?: DataQualityDatasetTableContext | null,
    contextState?: { isLoading: boolean; error?: string | null }
  ) => (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h6" fontWeight={600}>
          {formatTablePrimary(table)}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {formatTableSecondary(table) || formatDescription(definition?.description)}
        </Typography>
      </Box>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        <DetailRow label="Schema" value={table.schemaName ?? '—'} />
        <DetailRow label="Alias" value={table.alias ?? '—'} />
        <DetailRow label="Load Order" value={table.loadOrder ?? '—'} />
      </Stack>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        <DetailRow label="Table Type" value={table.tableType ?? (table.isConstructed ? 'Constructed' : 'Source')} />
        <DetailRow label="Physical Name" value={table.physicalName} />
      </Stack>
      <Stack spacing={0.5}>
        <Typography variant="subtitle2">Field overview</Typography>
        {table.fields.length ? (
          table.fields.slice(0, 6).map((field) => (
            <Typography key={field.dataDefinitionFieldId} variant="body2">
              {field.name}
            </Typography>
          ))
        ) : (
          <Typography variant="body2" color="text.secondary">
            No fields registered yet.
          </Typography>
        )}
        {table.fields.length > 6 ? (
          <Typography variant="caption" color="text.secondary">
            {`+${table.fields.length - 6} more fields available in the tree.`}
          </Typography>
        ) : null}
      </Stack>
      {contextState ? (
        contextState.isLoading ? (
          <Stack direction="row" spacing={1} alignItems="center">
            <CircularProgress size={16} />
            <Typography variant="body2" color="text.secondary">
              Loading table context…
            </Typography>
          </Stack>
        ) : contextState.error ? (
          <Alert severity="warning">{contextState.error}</Alert>
        ) : context ? (
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <DetailRow label="Table Group ID" value={context.tableGroupId} />
            <DetailRow label="Table ID" value={context.tableId ?? '—'} />
          </Stack>
        ) : null
      ) : null}
    </Stack>
  );

  const renderFieldDetail = (
    field: DataQualityDatasetField,
    table?: DataQualityDatasetTable,
    profileState?: {
      profile?: DataQualityColumnProfile | null;
      isLoading?: boolean;
      error?: string | null;
    }
  ) => (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h6" fontWeight={600}>
          {field.name}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {formatDescription(field.description)}
        </Typography>
      </Box>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap">
        <StatTile label="Data Type" value={field.fieldType ? (field.fieldLength ? `${field.fieldType} (${field.fieldLength})` : field.fieldType) : 'Unknown'} />
        <StatTile label="Uniqueness" value={field.isUnique ? 'Enforced' : 'Not enforced'} />
        {table ? <StatTile label="Parent Table" value={formatTablePrimary(table)} /> : null}
      </Stack>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        <DetailRow label="Application Usage" value={field.applicationUsage ?? '—'} />
        <DetailRow label="Business Definition" value={field.businessDefinition ?? '—'} />
      </Stack>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        <DetailRow label="Reference Table" value={field.referenceTable ?? '—'} />
        <DetailRow label="Notes" value={field.notes ?? '—'} />
      </Stack>
      <Divider />
      <Stack spacing={1}>
        <Typography variant="subtitle2">Latest profiling results</Typography>
        <ColumnProfilePanel
          profile={profileState?.profile ?? null}
          isLoading={profileState?.isLoading}
          error={profileState?.error ?? null}
          variant="inline"
          emptyHint="No profiling runs have produced column statistics yet."
        />
      </Stack>
    </Stack>
  );

  const renderDetailContent = () => {
    if (!selectedContext) {
      return renderRootDetail();
    }
    switch (selectedContext.type) {
      case 'productTeam':
        return selectedContext.productTeam ? renderProductTeamDetail(selectedContext.productTeam) : renderRootDetail();
      case 'application':
        return selectedContext.application ? renderApplicationDetail(selectedContext.application) : renderRootDetail();
      case 'dataObject':
        return selectedContext.dataObject ? renderDataObjectDetail(selectedContext.dataObject) : renderRootDetail();
      case 'table':
        return selectedContext.table
          ? renderTableDetail(selectedContext.table, selectedContext.dataDefinition, tableContextDetail, tableContextMeta)
          : renderRootDetail();
      case 'field':
        return selectedContext.field
          ? renderFieldDetail(selectedContext.field, selectedContext.table, columnProfileState)
          : renderRootDetail();
      default:
        return renderRootDetail();
    }
  };

  const handleExpandAll = () => {
    if (autoExpandedItems.length) {
      setExpandedItems(autoExpandedItems);
    }
  };

  const handleCollapseAll = () => {
    setExpandedItems([ROOT_NODE_ID]);
  };

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Data Quality Datasets"
        subtitle="Explore process areas, applications, data objects, and their registered data definitions synchronized from Conversion Central."
        actions={
          <Button component={RouterLink} to="/data-quality/profiling-runs" variant="outlined">
            View Profiling Runs
          </Button>
        }
      />

      {monitoredRunStatuses.length > 0 ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack spacing={2}>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ md: 'center' }}>
              <Typography fontWeight={600}>Recent profiling requests</Typography>
              {hasPendingMonitoredRuns ? (
                <Stack direction="row" spacing={1} alignItems="center">
                  <CircularProgress size={16} />
                  <Typography variant="body2" color="text.secondary">
                    Background jobs are launching. We refresh every few seconds.
                  </Typography>
                </Stack>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  All requested runs have been dispatched.
                </Typography>
              )}
              <Box flexGrow={1} />
              <Button
                size="small"
                onClick={handleClearMonitoredRuns}
                disabled={monitorIsFetching && hasPendingMonitoredRuns}
              >
                Clear
              </Button>
            </Stack>
            {monitorErrorMessage ? <Alert severity="warning">{monitorErrorMessage}</Alert> : null}
            <Stack spacing={1.5}>
              {monitoredRunStatuses.map((run) => (
                <Paper key={run.profileRunId} variant="outlined" sx={{ p: 1.5 }}>
                  <Stack
                    direction={{ xs: 'column', sm: 'row' }}
                    spacing={1.5}
                    alignItems={{ sm: 'center' }}
                    justifyContent="space-between"
                  >
                    <Stack spacing={0.5}>
                      <Typography variant="body2" color="text.secondary">
                        Table Group
                      </Typography>
                      <Typography fontFamily="monospace" fontSize={14}>
                        {run.tableGroupId}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Run ID: {run.profileRunId}
                      </Typography>
                    </Stack>
                    <Stack direction="row" spacing={1} alignItems="center">
                      {run.isLoading ? <CircularProgress size={16} /> : null}
                      <Chip label={formatStatusLabel(run.status)} color={resolveStatusChipColor(run.status)} size="small" />
                    </Stack>
                    <Button
                      component={RouterLink}
                      to={`/data-quality/profiling-runs?tableGroupId=${encodeURIComponent(run.tableGroupId)}`}
                      variant="outlined"
                      size="small"
                    >
                      View runs
                    </Button>
                  </Stack>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                    {describeStatusTimestamps(run.startedAt, run.completedAt)}
                  </Typography>
                </Paper>
              ))}
            </Stack>
          </Stack>
        </Paper>
      ) : null}

      {hierarchyQuery.isLoading ? (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress />
        </Box>
      ) : hierarchyQuery.isError ? (
        <Alert severity="error">Unable to load the dataset hierarchy.</Alert>
      ) : hasHierarchyData ? (
        <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems="stretch">
          <Paper
            variant="outlined"
            sx={{
              p: 2,
              width: { xs: '100%', lg: 360 },
              flexShrink: 0,
              display: 'flex',
              flexDirection: 'column',
              maxHeight: { xs: '100%', lg: 640 }
            }}
          >
            <Stack spacing={1.5} sx={{ mb: 1 }}>
              <Stack direction="row" spacing={1} alignItems="center" justifyContent="space-between">
                <Typography fontWeight={600}>Dataset Hierarchy</Typography>
                <Stack direction="row" spacing={1}>
                  <Button size="small" onClick={handleExpandAll} disabled={!hasHierarchyData}>
                    Expand all
                  </Button>
                  <Button size="small" onClick={handleCollapseAll} disabled={!hasHierarchyData}>
                    Collapse
                  </Button>
                </Stack>
              </Stack>
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ sm: 'flex-end' }}>
                <TextField
                  label="Quick search"
                  size="small"
                  fullWidth
                  value={treeSearch}
                  onChange={(event) => {
                    setTreeSearch(event.target.value);
                    setTreeSearchHelper(TREE_SEARCH_DEFAULT_HELPER);
                    setTreeSearchHasError(false);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') {
                      event.preventDefault();
                      handleTreeSearchSubmit();
                    }
                  }}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon fontSize="small" />
                      </InputAdornment>
                    )
                  }}
                  helperText={treeSearchHelper}
                  error={treeSearchHasError}
                />
                <Button
                  variant="outlined"
                  size="small"
                  onClick={handleTreeSearchSubmit}
                  sx={{ alignSelf: { xs: 'stretch', sm: 'center' }, minWidth: { sm: 80 } }}
                >
                  Jump
                </Button>
              </Stack>
            </Stack>
            <Divider sx={{ mb: 1 }} />
            <SimpleTreeView
              aria-label="Dataset hierarchy"
              expandedItems={expandedItems}
              onExpandedItemsChange={(_, itemIds) => setExpandedItems(itemIds as string[])}
              slots={{ collapseIcon: ExpandMoreIcon, expandIcon: ChevronRightIcon }}
              sx={{ flex: 1, minHeight: 320, overflowY: 'auto', pr: 0.5 }}
            >
              {renderRootNode()}
            </SimpleTreeView>
          </Paper>
          <Paper elevation={1} sx={{ flex: 1, minWidth: 0, p: 3 }}>
            {renderDetailContent()}
          </Paper>
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
