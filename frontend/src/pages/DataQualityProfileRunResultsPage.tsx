import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  List,
  ListItemButton,
  ListItemText,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { useQuery } from 'react-query';

import PageHeader from '@components/common/PageHeader';
import { TextProfileSummary } from '@components/data-quality/ColumnProfilePanel';
import { fetchProfileRunResults } from '@services/dataQualityService';
import {
  DataQualityProfileColumnEntry,
  DataQualityProfileRunResultResponse,
  DataQualityProfileTableEntry,
  DataQualityProfileValueEntry
} from '@cc-types/data';

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

const normalizeNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
};

const formatNumber = (value?: number | null) => {
  if (typeof value !== 'number') {
    return '—';
  }
  return value.toLocaleString();
};

const getTableKey = (table: DataQualityProfileTableEntry): string => {
  if (table.tableId) {
    return table.tableId;
  }
  const schemaPrefix = table.schemaName ? `${table.schemaName}.` : '';
  if (table.tableName) {
    return `${schemaPrefix}${table.tableName}`;
  }
  if (table.tableGroupId) {
    return `${table.tableGroupId}-${table.columns.length}`;
  }
  return `table-${table.columns.length}`;
};

const getColumnKey = (column: DataQualityProfileColumnEntry): string => {
  if (column.columnId) {
    return column.columnId;
  }
  const tablePrefix = column.tableName ? `${column.tableName}.` : '';
  return `${tablePrefix}${column.columnName}`;
};

const describeTableLabel = (table: DataQualityProfileTableEntry) => {
  const schema = table.schemaName ? `${table.schemaName}.` : '';
  return `${schema}${table.tableName ?? 'Unnamed table'}`;
};

const renderValueLabel = (entry: DataQualityProfileValueEntry) => {
  if (entry.label) {
    return entry.label;
  }
  if (entry.value === null || entry.value === undefined) {
    return 'NULL';
  }
  if (typeof entry.value === 'string') {
    return entry.value.length ? entry.value : '""';
  }
  return String(entry.value);
};

const CHARACTERISTIC_METRIC_KEYS = new Set([
  'semanticType',
  'semantic_type',
  'semantic_data_type',
  'suggestedType',
  'suggested_type',
  'suggested_data_type'
]);

const pickMetricString = (
  metrics: Record<string, unknown> | undefined,
  keys: string[]
): string | null => {
  if (!metrics) {
    return null;
  }
  for (const key of keys) {
    const value = metrics[key];
    if (value === undefined || value === null) {
      continue;
    }
    const normalized = String(value).trim();
    if (normalized) {
      return normalized;
    }
  }
  return null;
};

const pickMetricNumber = (
  metrics: Record<string, unknown> | undefined,
  keys: string[]
): number | null => {
  if (!metrics) {
    return null;
  }
  for (const key of keys) {
    const normalized = normalizeNumber(metrics[key]);
    if (normalized !== null) {
      return normalized;
    }
  }
  return null;
};

const DataQualityProfileRunResultsPage = () => {
  const navigate = useNavigate();
  const { profileRunId } = useParams<{ profileRunId: string }>();
  const [searchParams] = useSearchParams();
  const tableGroupId = searchParams.get('tableGroupId');
  const tableGroupLabel = searchParams.get('tableGroupLabel');

  const [selectedTableKey, setSelectedTableKey] = useState<string | null>(null);
  const [selectedColumnKey, setSelectedColumnKey] = useState<string | null>(null);
  const [valuePreviewOpen, setValuePreviewOpen] = useState(false);

  const queryEnabled = Boolean(profileRunId && tableGroupId);

  const resultsQuery = useQuery<DataQualityProfileRunResultResponse>(
    ['data-quality', 'profiling-runs', profileRunId, tableGroupId, 'results'],
    () => fetchProfileRunResults(profileRunId as string, tableGroupId as string),
    {
      enabled: queryEnabled,
      staleTime: 30 * 1000
    }
  );

  const tables = useMemo(() => resultsQuery.data?.tables ?? [], [resultsQuery.data]);

  useEffect(() => {
    if (!tables.length) {
      setSelectedTableKey(null);
      return;
    }
    setSelectedTableKey((current) => {
      if (current && tables.some((table) => getTableKey(table) === current)) {
        return current;
      }
      const fallback = tables[0];
      return fallback ? getTableKey(fallback) : null;
    });
  }, [tables]);

  const selectedTable = useMemo(() => {
    if (!selectedTableKey) {
      return tables[0] ?? null;
    }
    return tables.find((table) => getTableKey(table) === selectedTableKey) ?? tables[0] ?? null;
  }, [selectedTableKey, tables]);

  const columns = useMemo(() => selectedTable?.columns ?? [], [selectedTable]);

  useEffect(() => {
    if (!columns.length) {
      setSelectedColumnKey(null);
      return;
    }
    setSelectedColumnKey((current) => {
      if (current && columns.some((column) => getColumnKey(column) === current)) {
        return current;
      }
      const fallback = columns[0];
      return fallback ? getColumnKey(fallback) : null;
    });
  }, [columns]);

  const selectedColumn = useMemo(() => {
    if (!columns.length) {
      return null;
    }
    if (!selectedColumnKey) {
      return columns[0];
    }
    return columns.find((column) => getColumnKey(column) === selectedColumnKey) ?? columns[0];
  }, [columns, selectedColumnKey]);

  const piiAnomalies = useMemo(
    () => (selectedColumn?.anomalies ?? []).filter((anomaly) => Boolean(anomaly.piiRisk)),
    [selectedColumn]
  );

  const hygieneAnomalies = useMemo(
    () => (selectedColumn?.anomalies ?? []).filter((anomaly) => !anomaly.piiRisk),
    [selectedColumn]
  );

  const valueDistributionTotal = useMemo(() => {
    const direct = normalizeNumber(selectedColumn?.rowCount ?? null);
    if (direct !== null && direct > 0) {
      return direct;
    }
    const columnMetricTotal = pickMetricNumber(selectedColumn?.metrics, [
      'rowCount',
      'row_count',
      'record_count',
      'rowCountEstimate',
      'non_null_count'
    ]);
    if (columnMetricTotal !== null && columnMetricTotal > 0) {
      return columnMetricTotal;
    }
    const tableMetricTotal = pickMetricNumber(selectedTable?.metrics, [
      'rowCount',
      'row_count',
      'record_count'
    ]);
    if (tableMetricTotal !== null && tableMetricTotal > 0) {
      return tableMetricTotal;
    }
    return null;
  }, [selectedColumn, selectedTable]);

  const topValues = useMemo(() => {
    const entries = selectedColumn?.topValues ?? [];
    if (!entries.length) {
      return [] as DataQualityProfileValueEntry[];
    }
    if (!valueDistributionTotal || valueDistributionTotal <= 0) {
      return entries;
    }
    let appliedFallback = false;
    const normalizedEntries = entries.map((entry) => {
      if (typeof entry.percentage === 'number') {
        return entry;
      }
      const count = normalizeNumber(entry.count ?? null);
      if (count === null || count < 0) {
        return entry;
      }
      appliedFallback = true;
      return {
        ...entry,
        percentage: count / valueDistributionTotal
      };
    });
    return appliedFallback ? normalizedEntries : entries;
  }, [selectedColumn, valueDistributionTotal]);
  const distributionStats = useMemo(
    () => [
      { label: 'Row count', value: formatNumber(selectedColumn?.rowCount) },
      { label: 'Null values', value: formatNumber(selectedColumn?.nullCount) },
      { label: 'Distinct values', value: formatNumber(selectedColumn?.distinctCount) },
      { label: 'Non-null values', value: formatNumber(selectedColumn?.nonNullCount) }
    ],
    [selectedColumn]
  );
  const columnMetrics = useMemo(() => {
    if (!selectedColumn?.metrics) {
      return [] as Array<[string, unknown]>;
    }
    return Object.entries(selectedColumn.metrics);
  }, [selectedColumn]);
  const semanticDataType = useMemo(
    () =>
      pickMetricString(selectedColumn?.metrics, [
        'semanticType',
        'semantic_type',
        'semantic_data_type'
      ]) ?? selectedColumn?.generalType ?? null,
    [selectedColumn]
  );
  const suggestedDataType = useMemo(
    () =>
      pickMetricString(selectedColumn?.metrics, [
        'suggestedType',
        'suggested_type',
        'suggested_data_type'
      ]),
    [selectedColumn]
  );
  const filteredColumnMetrics = useMemo(
    () => columnMetrics.filter(([key]) => !CHARACTERISTIC_METRIC_KEYS.has(key)),
    [columnMetrics]
  );
  const textProfile = useMemo(() => selectedColumn?.textProfile ?? null, [selectedColumn]);

  const summary = resultsQuery.data?.summary;
  const headerTitle = tableGroupLabel ?? summary?.tableGroupId ?? 'Profiling results';
  const headerSubtitleParts = [
    summary?.startedAt ? `Started ${formatDateTime(summary.startedAt)}` : null,
    summary?.completedAt ? `Completed ${formatDateTime(summary.completedAt)}` : null
  ].filter((value): value is string => Boolean(value));
  const headerSubtitle = headerSubtitleParts.length ? headerSubtitleParts.join(' • ') : undefined;

  const handleBack = () => {
    navigate('/data-quality/profiling-runs');
  };

  const handleTableSelect = (table: DataQualityProfileTableEntry) => {
    setSelectedTableKey(getTableKey(table));
  };

  const handleColumnSelect = (column: DataQualityProfileColumnEntry) => {
    setSelectedColumnKey(getColumnKey(column));
  };

  const handleOpenValuePreview = () => setValuePreviewOpen(true);
  const handleCloseValuePreview = () => setValuePreviewOpen(false);

  const renderStatusChip = () => {
    if (!summary?.status) {
      return null;
    }
    return <Chip label={summary.status.replace(/_/g, ' ')} color="primary" size="small" sx={{ textTransform: 'capitalize' }} />;
  };

  return (
    <Box>
      <PageHeader
        title={headerTitle}
        subtitle={headerSubtitle}
        actions={
          <Stack direction="row" spacing={1} alignItems="center">
            {renderStatusChip()}
            <Button variant="outlined" startIcon={<ArrowBackIcon />} onClick={handleBack}>
              Back to runs
            </Button>
          </Stack>
        }
      />

      {!queryEnabled ? (
        <Alert severity="warning">Missing profiling run identifiers. Please open this page from the profiling runs list.</Alert>
      ) : null}

      {resultsQuery.isLoading && queryEnabled ? (
        <Stack alignItems="center" py={8} spacing={2}>
          <CircularProgress />
          <Typography variant="body2" color="text.secondary">
            Loading profiling results…
          </Typography>
        </Stack>
      ) : null}

      {resultsQuery.isError && queryEnabled ? (
        <Alert severity="error" sx={{ mb: 3 }}>
          Unable to load profiling results. Please try again later.
        </Alert>
      ) : null}

      {resultsQuery.data ? (
        <Grid container spacing={3} alignItems="stretch">
          <Grid item xs={12}>
            <Paper sx={{ p: 3 }}>
              <Stack spacing={2.5}>
                <Stack direction={{ xs: 'column', md: 'row' }} spacing={3} justifyContent="space-between" alignItems={{ md: 'center' }}>
                  <Stack spacing={0.75} flex={1}>
                    <Typography variant="overline" color="text.secondary">
                      Table group
                    </Typography>
                    <Typography variant="h5" fontWeight={700}>
                      {headerTitle}
                    </Typography>
                    {summary?.databricksRunId ? (
                      <Typography variant="body2" color="text.secondary">
                        Databricks run {summary.databricksRunId}
                      </Typography>
                    ) : null}
                  </Stack>
                  <Stack direction="row" spacing={3} flexWrap="wrap" useFlexGap>
                    <Stack spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        Rows profiled
                      </Typography>
                      <Typography variant="h6">{formatNumber(summary?.rowCount)}</Typography>
                    </Stack>
                    <Stack spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        Anomalies
                      </Typography>
                      <Typography variant="h6">{formatNumber(summary?.anomalyCount)}</Typography>
                    </Stack>
                  </Stack>
                </Stack>
                <Divider />
                <Stack direction={{ xs: 'column', md: 'row' }} spacing={3} flexWrap="wrap" useFlexGap>
                  <Stack spacing={0.5}>
                    <Typography variant="caption" color="text.secondary">
                      Started at
                    </Typography>
                    <Typography variant="body2">{formatDateTime(summary?.startedAt)}</Typography>
                  </Stack>
                  <Stack spacing={0.5}>
                    <Typography variant="caption" color="text.secondary">
                      Completed at
                    </Typography>
                    <Typography variant="body2">{formatDateTime(summary?.completedAt)}</Typography>
                  </Stack>
                </Stack>
              </Stack>
            </Paper>
          </Grid>

          <Grid item xs={12} md={4} display="flex" flexDirection="column" gap={3}>
            <Paper sx={{ p: 2, flex: 1, display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" fontWeight={700} gutterBottom>
                Tables ({tables.length})
              </Typography>
              {tables.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  No tables were returned for this profiling run.
                </Typography>
              ) : (
                <List dense sx={{ flex: 1, overflow: 'auto' }}>
                  {tables.map((table) => {
                    const key = getTableKey(table);
                    return (
                      <ListItemButton
                        key={key}
                        selected={key === selectedTableKey}
                        onClick={() => handleTableSelect(table)}
                        sx={{ borderRadius: 1, mb: 0.5 }}
                      >
                        <ListItemText
                          primary={describeTableLabel(table)}
                          secondary={`${table.columns.length} column${table.columns.length === 1 ? '' : 's'}`}
                        />
                      </ListItemButton>
                    );
                  })}
                </List>
              )}
            </Paper>

            <Paper sx={{ p: 2, flex: 1, display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" fontWeight={700} gutterBottom>
                Columns ({columns.length})
              </Typography>
              {!columns.length ? (
                <Typography variant="body2" color="text.secondary">
                  Select a table to see its profiled columns.
                </Typography>
              ) : (
                <List dense sx={{ flex: 1, overflow: 'auto' }}>
                  {columns.map((column) => {
                    const columnKey = getColumnKey(column);
                    const issueCount = column.anomalies?.length ?? 0;
                    return (
                      <ListItemButton
                        key={columnKey}
                        selected={columnKey === selectedColumnKey}
                        onClick={() => handleColumnSelect(column)}
                        sx={{ alignItems: 'flex-start', borderRadius: 1, mb: 0.5 }}
                      >
                        <ListItemText
                          primary={column.columnName}
                          secondary={column.dataType ? column.dataType : undefined}
                        />
                        {issueCount ? <Chip label={issueCount} color="warning" size="small" /> : null}
                      </ListItemButton>
                    );
                  })}
                </List>
              )}
            </Paper>
          </Grid>

          <Grid item xs={12} md={8} display="flex" flexDirection="column" gap={3}>
            <Paper sx={{ p: 3 }}>
              {selectedColumn ? (
                <Stack spacing={2}>
                  <Box>
                    <Typography variant="h6" fontWeight={700} gutterBottom>
                      Column characteristics
                    </Typography>
                    <Typography variant="h5" fontWeight={700}>
                      {selectedColumn.columnName}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {selectedTable?.tableName ?? 'Unknown table'}
                    </Typography>
                  </Box>
                  <Divider />
                  <Stack direction={{ xs: 'column', sm: 'row' }} spacing={3} flexWrap="wrap" useFlexGap>
                    <Stack spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        Data type
                      </Typography>
                      <Typography variant="h6">
                        {selectedColumn.dataType ?? 'Data type pending'}
                      </Typography>
                    </Stack>
                    <Stack spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        Semantic data type
                      </Typography>
                      <Typography variant="h6">
                        {semanticDataType ?? 'Not detected'}
                      </Typography>
                    </Stack>
                    <Stack spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        Suggested data type
                      </Typography>
                      <Typography variant="h6">
                        {suggestedDataType ?? 'Not available'}
                      </Typography>
                    </Stack>
                  </Stack>
                </Stack>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Select a column to view its profiling details.
                </Typography>
              )}
            </Paper>

            <Paper sx={{ p: 3 }}>
              <Stack spacing={2}>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="h6" fontWeight={700}>
                    Value distribution
                  </Typography>
                  <Button
                    variant="outlined"
                    size="small"
                    onClick={handleOpenValuePreview}
                  >
                    View unique values
                  </Button>
                </Stack>
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={3} flexWrap="wrap" useFlexGap>
                  {distributionStats.map((stat) => (
                    <Stack key={stat.label} spacing={0.5}>
                      <Typography variant="caption" color="text.secondary">
                        {stat.label}
                      </Typography>
                      <Typography variant="h6">{stat.value}</Typography>
                    </Stack>
                  ))}
                </Stack>
                {filteredColumnMetrics.length ? (
                  <>
                    <Divider />
                    <Stack spacing={1}>
                      <Typography variant="subtitle2" color="text.secondary">
                        Additional metrics
                      </Typography>
                      <Stack spacing={0.75}>
                        {filteredColumnMetrics.map(([key, value]) => (
                          <Stack key={key} direction="row" justifyContent="space-between" spacing={2}>
                            <Typography variant="body2" color="text.secondary">
                              {key}
                            </Typography>
                            <Typography variant="body2" fontWeight={600}>
                              {typeof value === 'number' ? value.toLocaleString() : String(value)}
                            </Typography>
                          </Stack>
                        ))}
                      </Stack>
                    </Stack>
                  </>
                ) : null}
                <Divider />
                {!topValues.length ? (
                  <Typography variant="body2" color="text.secondary">
                    This column did not include value samples.
                  </Typography>
                ) : (
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Value</TableCell>
                        <TableCell align="right">Count</TableCell>
                        <TableCell align="right">Percentage</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {topValues.slice(0, 5).map((entry, index) => (
                        <TableRow key={`${renderValueLabel(entry)}-${index}`}>
                          <TableCell>{renderValueLabel(entry)}</TableCell>
                          <TableCell align="right">{formatNumber(entry.count)}</TableCell>
                          <TableCell align="right">
                            {typeof entry.percentage === 'number' ? `${(entry.percentage * 100).toFixed(1)}%` : '—'}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </Stack>
            </Paper>

            {textProfile ? <TextProfileSummary profile={textProfile} /> : null}

            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" fontWeight={700} gutterBottom>
                Potential PII
              </Typography>
              {!piiAnomalies.length ? (
                <Typography variant="body2" color="text.secondary">
                  No potential PII findings were reported for this column.
                </Typography>
              ) : (
                <Stack spacing={2}>
                  {piiAnomalies.map((anomaly, index) => (
                    <Box key={`${anomaly.anomalyTypeId ?? anomaly.detail ?? index}`}>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {anomaly.detail ?? 'Potential PII detected'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Risk level: {anomaly.piiRisk ?? 'Unspecified'}
                      </Typography>
                      {anomaly.detectedAt ? (
                        <Typography variant="caption" color="text.secondary">
                          Detected {formatDateTime(anomaly.detectedAt)}
                        </Typography>
                      ) : null}
                    </Box>
                  ))}
                </Stack>
              )}
            </Paper>

            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" fontWeight={700} gutterBottom>
                Hygiene issues
              </Typography>
              {!hygieneAnomalies.length ? (
                <Typography variant="body2" color="text.secondary">
                  No hygiene issues were reported for this column.
                </Typography>
              ) : (
                <Stack spacing={2}>
                  {hygieneAnomalies.map((anomaly, index) => (
                    <Box key={`${anomaly.anomalyTypeId ?? anomaly.detail ?? index}`}>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {anomaly.detail ?? 'Quality issue detected'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Severity: {anomaly.severity ?? 'Unspecified'} · Likelihood: {anomaly.likelihood ?? 'Unspecified'}
                      </Typography>
                      {anomaly.detectedAt ? (
                        <Typography variant="caption" color="text.secondary">
                          Detected {formatDateTime(anomaly.detectedAt)}
                        </Typography>
                      ) : null}
                    </Box>
                  ))}
                </Stack>
              )}
            </Paper>
          </Grid>
        </Grid>
      ) : null}

      <Dialog open={valuePreviewOpen} onClose={handleCloseValuePreview} maxWidth="sm" fullWidth>
        <DialogTitle>Value distribution preview</DialogTitle>
        <DialogContent dividers>
          {!topValues.length ? (
            <Typography variant="body2" color="text.secondary">
              No sample values were returned for this column.
            </Typography>
          ) : (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Value</TableCell>
                  <TableCell align="right">Count</TableCell>
                  <TableCell align="right">Percentage</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {topValues.map((entry, index) => (
                  <TableRow key={`${renderValueLabel(entry)}-${index}`}>
                    <TableCell>{renderValueLabel(entry)}</TableCell>
                    <TableCell align="right">{formatNumber(entry.count)}</TableCell>
                    <TableCell align="right">
                      {typeof entry.percentage === 'number' ? `${(entry.percentage * 100).toFixed(1)}%` : '—'}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
};

export default DataQualityProfileRunResultsPage;
