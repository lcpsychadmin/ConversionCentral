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
  LinearProgress,
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
import { fetchProfileRunResults } from '@services/dataQualityService';
import {
  DataQualityNumericDistributionBar,
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

const formatStatValue = (value?: unknown) => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(value);
};

const toPercentageValue = (value?: number | null) => {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 0;
  }
  return Math.max(0, Math.min(value * 100, 100));
};

const formatPercentageDisplay = (value?: number | null, fractionDigits = 1) => {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return '—';
  }
  return `${(value * 100).toFixed(fractionDigits)}%`;
};

const clampToRange = (value: number, min: number, max: number) => {
  return Math.min(Math.max(value, min), max);
};

const normalizeDistributionKey = (value?: string | null): 'nonZero' | 'zero' | 'null' => {
  const normalized = (value ?? '').toLowerCase();
  if (normalized.includes('zero')) {
    return 'zero';
  }
  if (normalized.includes('null')) {
    return 'null';
  }
  return 'nonZero';
};

const NUMERIC_DISTRIBUTION_COLORS: Record<'nonZero' | 'zero' | 'null', string> = {
  nonZero: 'primary.main',
  zero: 'info.light',
  null: 'warning.main'
};

const NUMERIC_DISTRIBUTION_LABELS: Record<'nonZero' | 'zero' | 'null', string> = {
  nonZero: 'Non-Zero Values',
  zero: 'Zero Values',
  null: 'Null Values'
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
  const topValueMaxCount = useMemo(() => {
    return topValues.reduce((max, entry) => {
      const count = normalizeNumber(entry.count ?? null) ?? 0;
      return count > max ? count : max;
    }, 0);
  }, [topValues]);
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
  const textProfileStats = textProfile?.stats ?? null;
  const numericProfile = useMemo(() => selectedColumn?.numericProfile ?? null, [selectedColumn]);
  const numericProfileStats = numericProfile?.stats ?? null;
  const textSummaryMetrics = useMemo(() => {
    if (!textProfileStats && !selectedColumn) {
      return [] as Array<{ label: string; value: string; annotation?: string }>;
    }
    const recordCount = textProfileStats?.recordCount ?? selectedColumn?.rowCount ?? null;
    const valueCount = textProfileStats?.valueCount ?? selectedColumn?.nonNullCount ?? null;
    const zeroLength = textProfileStats?.zeroLengthCount ?? 0;
    const dummyValues = textProfileStats?.dummyValueCount ?? 0;
    const derivedActual =
      valueCount !== null && valueCount !== undefined
        ? Math.max(valueCount - zeroLength - dummyValues, 0)
        : null;
    const actualValues = textProfileStats?.actualValueCount ?? derivedActual;
    const distinctValues = selectedColumn?.distinctCount ?? null;
    const duplicateCount = textProfileStats?.duplicateCount ?? null;
    const duplicateAnnotation =
      typeof textProfileStats?.duplicatePercentage === 'number'
        ? formatPercentageDisplay(textProfileStats.duplicatePercentage)
        : undefined;
    return [
      { label: 'Record count', value: formatStatValue(recordCount) },
      { label: 'Value count', value: formatStatValue(valueCount) },
      { label: 'Actual values', value: formatStatValue(actualValues) },
      { label: 'Distinct values', value: formatStatValue(distinctValues) },
      {
        label: 'Duplicate values',
        value: formatStatValue(duplicateCount),
        annotation: duplicateAnnotation
      }
    ];
  }, [selectedColumn, textProfileStats]);
  const breakdownSections = useMemo(
    () => {
      if (!textProfileStats) {
        return [] as Array<{
          key: string;
          title: string;
          total?: number | null;
          percentage?: number | null;
          entries: Array<{ label: string; count?: number | null; percentage?: number | null }>;
          color: 'success' | 'warning' | 'secondary';
        }>;
      }
      return [
        {
          key: 'missing',
          title: 'Missing values',
          total: textProfileStats.missingCount,
          percentage: textProfileStats.missingPercentage,
          entries: textProfile?.missingBreakdown ?? [],
          color: 'success' as const
        },
        {
          key: 'duplicates',
          title: 'Duplicate values',
          total: textProfileStats.duplicateCount,
          percentage: textProfileStats.duplicatePercentage,
          entries: textProfile?.duplicateBreakdown ?? [],
          color: 'warning' as const
        },
        {
          key: 'case',
          title: 'Case distribution',
          total: textProfileStats.valueCount,
          percentage: null,
          entries: textProfile?.caseBreakdown ?? [],
          color: 'secondary' as const
        }
      ].filter((section) =>
        Boolean(section.total || section.percentage || section.entries.length)
      );
    },
    [textProfile, textProfileStats]
  );
  const textValueDetailMetrics = useMemo(() => {
    if (!textProfileStats) {
      return [] as Array<{ label: string; value?: number | null }>;
    }
    return [
      { label: 'Includes digits', value: textProfileStats.includesDigitCount },
      { label: 'Quoted values', value: textProfileStats.quotedCount },
      { label: 'Numeric values', value: textProfileStats.numericOnlyCount },
      { label: 'Leading spaces', value: textProfileStats.leadingSpaceCount },
      { label: 'Zero values', value: textProfileStats.zeroCount },
      { label: 'Embedded spaces', value: textProfileStats.embeddedSpaceCount },
      { label: 'Date values', value: textProfileStats.dateValueCount },
      { label: 'Avg. embedded spaces', value: textProfileStats.averageEmbeddedSpaces }
    ].filter((metric) => metric.value !== null && metric.value !== undefined);
  }, [textProfileStats]);
  const textLengthMetrics = useMemo(() => {
    if (!textProfileStats) {
      return [] as Array<{ label: string; value?: number | string | null }>;
    }
    return [
      { label: 'Minimum length', value: textProfileStats.minLength },
      { label: 'Maximum length', value: textProfileStats.maxLength },
      { label: 'Average length', value: textProfileStats.avgLength },
      { label: 'Minimum text', value: textProfileStats.minText },
      { label: 'Maximum text', value: textProfileStats.maxText },
      { label: 'Standard pattern match', value: textProfileStats.standardPatternMatches },
      { label: 'Distinct patterns', value: textProfileStats.distinctPatterns }
    ].filter((metric) => metric.value !== null && metric.value !== undefined);
  }, [textProfileStats]);
  const textFrequentPatterns = useMemo(() => textProfile?.frequentPatterns ?? [], [textProfile]);
  const textMissingDetailMetrics = useMemo(() => {
    if (!textProfileStats && !selectedColumn) {
      return [] as Array<{ label: string; value?: number | null }>;
    }
    return [
      {
        label: 'Null',
        value: textProfileStats?.nullValueCount ?? selectedColumn?.nullCount ?? null
      },
      { label: 'Zero length', value: textProfileStats?.zeroLengthCount ?? null },
      { label: 'Dummy values', value: textProfileStats?.dummyValueCount ?? null }
    ].filter((metric) => metric.value !== null && metric.value !== undefined);
  }, [selectedColumn, textProfileStats]);
  const numericSummaryMetrics = useMemo(() => {
    const recordCount = numericProfileStats?.recordCount ?? selectedColumn?.rowCount ?? null;
    const valueCount = numericProfileStats?.valueCount ?? selectedColumn?.nonNullCount ?? null;
    const metricsGrid = [
      { label: 'Distinct Values', value: numericProfileStats?.distinctCount ?? selectedColumn?.distinctCount ?? null },
      { label: 'Average Value', value: numericProfileStats?.average ?? null },
      { label: 'Standard Deviation', value: numericProfileStats?.stddev ?? null },
      { label: 'Minimum Value', value: numericProfileStats?.minimum ?? null },
      { label: 'Minimum Value > 0', value: numericProfileStats?.minimumPositive ?? null },
      { label: 'Maximum Value', value: numericProfileStats?.maximum ?? null },
      { label: '25th Percentile', value: numericProfileStats?.percentile25 ?? null },
      { label: 'Median Value', value: numericProfileStats?.median ?? null },
      { label: '75th Percentile', value: numericProfileStats?.percentile75 ?? null }
    ].filter((metric) => metric.value !== null && metric.value !== undefined);
    const primary = [
      { label: 'Record Count', value: recordCount },
      { label: 'Value Count', value: valueCount }
    ].filter((metric) => metric.value !== null && metric.value !== undefined);
    return { primary, metricsGrid };
  }, [numericProfileStats, selectedColumn]);
  const numericPrimaryStats = numericSummaryMetrics.primary;
  const numericDetailStats = numericSummaryMetrics.metricsGrid;
  const numericDistribution = useMemo(() => {
    if (!numericProfile && !numericProfileStats && !selectedColumn) {
      return null;
    }
    const counts: Record<'nonZero' | 'zero' | 'null', number | null> = {
      nonZero: null,
      zero: null,
      null: null
    };
    const percentages: Record<'nonZero' | 'zero' | 'null', number | null> = {
      nonZero: null,
      zero: null,
      null: null
    };
    (numericProfile?.distributionBars ?? []).forEach((entry: DataQualityNumericDistributionBar) => {
      const key = normalizeDistributionKey(entry.key);
      counts[key] = normalizeNumber(entry.count ?? null);
      percentages[key] = typeof entry.percentage === 'number' ? clampToRange(entry.percentage, 0, 1) : null;
    });
    if (counts.nonZero === null && counts.zero === null && counts.null === null && numericProfileStats) {
      const zeroCount = normalizeNumber(numericProfileStats.zeroCount ?? null) ?? 0;
      const nullCount =
        normalizeNumber(numericProfileStats.nullCount ?? null) ??
        normalizeNumber(selectedColumn?.nullCount ?? null) ??
        0;
      const valueCount = normalizeNumber(
        numericProfileStats.valueCount ?? selectedColumn?.nonNullCount ?? null
      );
      counts.zero = zeroCount;
      counts.null = nullCount;
      counts.nonZero = valueCount !== null ? Math.max(valueCount - zeroCount, 0) : null;
    }
    const hasCounts = Object.values(counts).some(
      (value) => value !== null && value !== undefined
    );
    if (!hasCounts) {
      return null;
    }
    const totalCount =
      (counts.nonZero ?? 0) + (counts.zero ?? 0) + (counts.null ?? 0);
    const segments = (['nonZero', 'zero', 'null'] as const).map((key) => {
      const count = counts[key];
      const share =
        percentages[key] !== null && percentages[key] !== undefined
          ? (percentages[key] as number)
          : totalCount > 0 && count !== null && count !== undefined
          ? clampToRange(count / totalCount, 0, 1)
          : 0;
      return {
        key,
        label: NUMERIC_DISTRIBUTION_LABELS[key],
        count,
        percentage: share
      };
    });
    return {
      segments,
      total: totalCount
    };
  }, [numericProfile, numericProfileStats, selectedColumn]);
  const numericBoxPlot = numericProfile?.boxPlot ?? null;
  const numericBoxPlotGeometry = useMemo(() => {
    if (!numericBoxPlot || typeof numericBoxPlot.min !== 'number' || typeof numericBoxPlot.max !== 'number') {
      return null;
    }
    const min = numericBoxPlot.min;
    const max = numericBoxPlot.max;
    const span = Math.abs(max - min) || 1;
    const toPercent = (value?: number | null) => {
      if (typeof value !== 'number' || Number.isNaN(value)) {
        return null;
      }
      const normalized = (value - min) / span;
      return clampToRange(normalized, 0, 1) * 100;
    };
    const clampValue = (value?: number | null, fallback?: number | null) => {
      if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
      }
      return fallback ?? null;
    };
    const stdStart =
      typeof numericBoxPlot.mean === 'number' && typeof numericBoxPlot.stdDev === 'number'
        ? toPercent(numericBoxPlot.mean - numericBoxPlot.stdDev)
        : null;
    const stdEnd =
      typeof numericBoxPlot.mean === 'number' && typeof numericBoxPlot.stdDev === 'number'
        ? toPercent(numericBoxPlot.mean + numericBoxPlot.stdDev)
        : null;
    return {
      values: {
        min,
        max,
        p25: clampValue(numericBoxPlot.p25, min),
        median: clampValue(numericBoxPlot.median, numericBoxPlot.p25 ?? numericBoxPlot.p75 ?? min),
        p75: clampValue(numericBoxPlot.p75, max),
        mean: numericBoxPlot.mean ?? null,
        stdDev: numericBoxPlot.stdDev ?? null
      },
      percents: {
        min: 0,
        max: 100,
        p25: toPercent(numericBoxPlot.p25 ?? min),
        median: toPercent(numericBoxPlot.median ?? min),
        p75: toPercent(numericBoxPlot.p75 ?? max),
        mean: toPercent(numericBoxPlot.mean ?? null),
        stdStart,
        stdEnd
      }
    };
  }, [numericBoxPlot]);
  const valueSamplesTable = useMemo(() => {
    if (!topValues.length) {
      return (
        <Typography variant="body2" color="text.secondary">
          This column did not include value samples.
        </Typography>
      );
    }
    return (
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
    );
  }, [topValues]);
  const hasTextProfile = Boolean(textProfile && textProfileStats);
  const hasNumericProfile = Boolean(
    numericProfile && (numericProfileStats || numericDistribution || numericBoxPlot)
  );
  const showNumericProfile = hasNumericProfile;
  const showTextProfile = !showNumericProfile && hasTextProfile;

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
              <Stack spacing={2.5}>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="h6" fontWeight={700}>
                    Value distribution
                  </Typography>
                  <Button variant="outlined" size="small" onClick={handleOpenValuePreview}>
                    Data preview
                  </Button>
                </Stack>

                {showTextProfile ? (
                  <Stack spacing={2.5}>
                    {textSummaryMetrics.length ? (
                      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={3} flexWrap="wrap" useFlexGap>
                        {textSummaryMetrics.map((metric) => (
                          <Stack spacing={0.5} key={metric.label}>
                            <Typography variant="caption" color="text.secondary">
                              {metric.label}
                            </Typography>
                            <Typography variant="h6">
                              {metric.value}
                              {metric.annotation ? (
                                <Box component="span" sx={{ ml: 0.75 }}>
                                  <Typography component="span" variant="body2" color="text.secondary">
                                    {metric.annotation}
                                  </Typography>
                                </Box>
                              ) : null}
                            </Typography>
                          </Stack>
                        ))}
                      </Stack>
                    ) : null}

                    {textMissingDetailMetrics.length ? (
                      <Grid container spacing={2}>
                        {textMissingDetailMetrics.map((metric) => (
                          <Grid item xs={12} sm={4} key={metric.label}>
                            <Typography variant="caption" color="text.secondary">
                              {metric.label}
                            </Typography>
                            <Typography variant="subtitle2">{formatStatValue(metric.value)}</Typography>
                          </Grid>
                        ))}
                      </Grid>
                    ) : null}

                    {breakdownSections.length ? (
                      <Stack spacing={2}>
                        {breakdownSections.map((section) => (
                          <Box key={section.key}>
                            <Stack direction="row" justifyContent="space-between" alignItems="center">
                              <Typography variant="subtitle2" fontWeight={600}>
                                {section.title}
                              </Typography>
                              {section.key !== 'case' ? (
                                <Typography variant="body2" color="text.secondary">
                                  {formatStatValue(section.total)}{' '}
                                  {section.percentage !== null && section.percentage !== undefined
                                    ? `(${formatPercentageDisplay(section.percentage)})`
                                    : ''}
                                </Typography>
                              ) : null}
                            </Stack>
                            <LinearProgress
                              variant="determinate"
                              value={section.key === 'case' ? 100 : toPercentageValue(section.percentage)}
                              color={section.color}
                              sx={{ mt: 0.75, height: 8, borderRadius: 999, bgcolor: 'action.hover' }}
                            />
                            {section.entries.length ? (
                              <Stack direction="row" flexWrap="wrap" useFlexGap spacing={1} sx={{ mt: 1 }}>
                                {section.entries.map((entry) => (
                                  <Chip
                                    key={`${section.key}-${entry.label}`}
                                    size="small"
                                    label={`${entry.label}: ${formatStatValue(entry.count)}${
                                      entry.percentage !== null && entry.percentage !== undefined
                                        ? ` (${formatPercentageDisplay(entry.percentage)})`
                                        : ''
                                    }`}
                                  />
                                ))}
                              </Stack>
                            ) : null}
                          </Box>
                        ))}
                      </Stack>
                    ) : null}

                    {topValues.length ? (
                      <Stack spacing={1.25}>
                        <Divider />
                        <Typography variant="subtitle2" fontWeight={600}>
                          Frequent values
                        </Typography>
                        <Stack spacing={1.25}>
                          {topValues.slice(0, 8).map((entry, index) => {
                            const normalizedCount = normalizeNumber(entry.count ?? null) ?? 0;
                            const share = typeof entry.percentage === 'number'
                              ? entry.percentage
                              : valueDistributionTotal && valueDistributionTotal > 0
                              ? normalizedCount / valueDistributionTotal
                              : topValueMaxCount > 0
                              ? normalizedCount / topValueMaxCount
                              : 0;
                            const shareDisplay = Number.isFinite(share) ? share : null;
                            return (
                              <Box key={`${renderValueLabel(entry)}-${index}`}>
                                <Stack direction="row" justifyContent="space-between" spacing={2} alignItems="center">
                                  <Typography
                                    variant="body2"
                                    noWrap
                                    title={renderValueLabel(entry)}
                                    sx={{ flex: 1, pr: 2 }}
                                  >
                                    {renderValueLabel(entry)}
                                  </Typography>
                                  <Typography variant="body2" color="text.secondary">
                                    {formatNumber(entry.count)}{' '}
                                    {shareDisplay !== null ? `(${(shareDisplay * 100).toFixed(1)}%)` : ''}
                                  </Typography>
                                </Stack>
                                <LinearProgress
                                  variant="determinate"
                                  value={toPercentageValue(shareDisplay)}
                                  color="secondary"
                                  sx={{ mt: 0.5, height: 6, borderRadius: 999, bgcolor: 'action.hover' }}
                                />
                              </Box>
                            );
                          })}
                        </Stack>
                      </Stack>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        This column did not include value samples.
                      </Typography>
                    )}

                    {(textValueDetailMetrics.length || textLengthMetrics.length || textFrequentPatterns.length) ? (
                      <Stack spacing={2}>
                        {textValueDetailMetrics.length ? (
                          <>
                            <Divider />
                            <Box>
                              <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                                Value characteristics
                              </Typography>
                              <Grid container spacing={2}>
                                {textValueDetailMetrics.map((metric) => (
                                  <Grid item xs={6} sm={4} md={3} key={metric.label}>
                                    <Typography variant="caption" color="text.secondary">
                                      {metric.label}
                                    </Typography>
                                    <Typography variant="subtitle2">{formatStatValue(metric.value)}</Typography>
                                  </Grid>
                                ))}
                              </Grid>
                            </Box>
                          </>
                        ) : null}
                        {textLengthMetrics.length ? (
                          <>
                            <Divider />
                            <Box>
                              <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                                Length & pattern summary
                              </Typography>
                              <Grid container spacing={2}>
                                {textLengthMetrics.map((metric) => (
                                  <Grid item xs={6} sm={4} md={3} key={metric.label}>
                                    <Typography variant="caption" color="text.secondary">
                                      {metric.label}
                                    </Typography>
                                    <Typography
                                      variant="subtitle2"
                                      noWrap={typeof metric.value === 'string'}
                                      title={typeof metric.value === 'string' ? String(metric.value) : undefined}
                                    >
                                      {formatStatValue(metric.value)}
                                    </Typography>
                                  </Grid>
                                ))}
                              </Grid>
                            </Box>
                          </>
                        ) : null}
                        {textFrequentPatterns.length ? (
                          <>
                            <Divider />
                            <Box>
                              <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                                Frequent patterns
                              </Typography>
                              <Stack spacing={0.75}>
                                {textFrequentPatterns.slice(0, 6).map((pattern, idx) => (
                                  <Stack
                                    key={`${pattern.label ?? 'pattern'}-${idx}`}
                                    direction="row"
                                    justifyContent="space-between"
                                    spacing={2}
                                    alignItems="center"
                                  >
                                    <Typography
                                      variant="body2"
                                      fontFamily="monospace"
                                      sx={{ flex: 1, pr: 2 }}
                                      noWrap
                                      title={pattern.label ?? undefined}
                                    >
                                      {pattern.label ?? 'Pattern'}
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary">
                                      {formatStatValue(pattern.count)}
                                      {pattern.percentage !== null && pattern.percentage !== undefined
                                        ? ` (${formatPercentageDisplay(pattern.percentage)})`
                                        : ''}
                                    </Typography>
                                  </Stack>
                                ))}
                              </Stack>
                            </Box>
                          </>
                        ) : null}
                      </Stack>
                    ) : null}
                  </Stack>
                ) : showNumericProfile ? (
                  <Stack spacing={3}>
                    {numericPrimaryStats.length ? (
                      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={3} flexWrap="wrap" useFlexGap>
                        {numericPrimaryStats.map((metric) => (
                          <Stack key={metric.label} spacing={0.25} minWidth={140}>
                            <Typography variant="caption" color="text.secondary" sx={{ textTransform: 'uppercase', letterSpacing: 0.4 }}>
                              {metric.label}
                            </Typography>
                            <Typography variant="h6">{formatStatValue(metric.value)}</Typography>
                          </Stack>
                        ))}
                      </Stack>
                    ) : null}

                    {numericDistribution ? (
                      <Box>
                        <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                          Numeric Distribution
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                          {numericDistribution.segments
                            .map((segment) => `${segment.label}: ${formatStatValue(segment.count)}`)
                            .join(' | ')}
                        </Typography>
                        <Stack spacing={1.5}>
                          <Box
                            sx={{
                              display: 'flex',
                              height: 12,
                              borderRadius: 999,
                              overflow: 'hidden',
                              bgcolor: 'action.hover'
                            }}
                          >
                            {numericDistribution.segments.map((segment) => {
                              const widthPercent = numericDistribution.total > 0
                                ? toPercentageValue(segment.percentage)
                                : 100 / numericDistribution.segments.length;
                              return (
                                <Box
                                  key={segment.key}
                                  sx={{
                                    width: `${widthPercent}%`,
                                    bgcolor: NUMERIC_DISTRIBUTION_COLORS[segment.key]
                                  }}
                                />
                              );
                            })}
                          </Box>
                          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} flexWrap="wrap" useFlexGap>
                            {numericDistribution.segments.map((segment) => (
                              <Typography key={segment.key} variant="body2" color="text.secondary">
                                <Box component="span" fontWeight={600} color="text.primary">
                                  {segment.label}:
                                </Box>{' '}
                                {segment.percentage !== null && segment.percentage !== undefined
                                  ? formatPercentageDisplay(segment.percentage)
                                  : '—'}
                              </Typography>
                            ))}
                          </Stack>
                        </Stack>
                      </Box>
                    ) : null}

                    {numericDetailStats.length ? (
                      <>
                        <Divider />
                        <Grid container spacing={2}>
                          {numericDetailStats.map((metric) => (
                            <Grid item xs={6} sm={4} key={metric.label}>
                              <Typography variant="caption" color="text.secondary">
                                {metric.label}
                              </Typography>
                              <Typography variant="subtitle2">{formatStatValue(metric.value)}</Typography>
                            </Grid>
                          ))}
                        </Grid>
                      </>
                    ) : null}

                    {numericBoxPlotGeometry ? (
                      <>
                        <Divider />
                        <Box>
                          <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                            Distribution Summary
                          </Typography>
                          <Box sx={{ position: 'relative', height: 80, mt: 2 }}>
                            <Box
                              sx={{
                                position: 'absolute',
                                top: '50%',
                                left: 0,
                                right: 0,
                                height: 2,
                                bgcolor: 'divider',
                                transform: 'translateY(-50%)'
                              }}
                            />
                            <Box
                              sx={{
                                position: 'absolute',
                                top: '50%',
                                left: `${numericBoxPlotGeometry.percents.min}%`,
                                width: 2,
                                height: 28,
                                bgcolor: 'text.secondary',
                                transform: 'translate(-50%, -50%)'
                              }}
                            />
                            <Box
                              sx={{
                                position: 'absolute',
                                top: '50%',
                                left: `${numericBoxPlotGeometry.percents.max}%`,
                                width: 2,
                                height: 28,
                                bgcolor: 'text.secondary',
                                transform: 'translate(-50%, -50%)'
                              }}
                            />
                            {numericBoxPlotGeometry.percents.p25 !== null &&
                            numericBoxPlotGeometry.percents.p75 !== null ? (
                              <Box
                                sx={{
                                  position: 'absolute',
                                  top: '50%',
                                  left: `${numericBoxPlotGeometry.percents.p25}%`,
                                  width: `${Math.max(
                                    numericBoxPlotGeometry.percents.p75 - numericBoxPlotGeometry.percents.p25,
                                    1
                                  )}%`,
                                  height: 24,
                                  bgcolor: 'primary.light',
                                  border: '1px solid',
                                  borderColor: 'primary.main',
                                  transform: 'translateY(-50%)'
                                }}
                              />
                            ) : null}
                            {numericBoxPlotGeometry.percents.median !== null ? (
                              <Box
                                sx={{
                                  position: 'absolute',
                                  top: '50%',
                                  left: `${numericBoxPlotGeometry.percents.median}%`,
                                  width: 2,
                                  height: 28,
                                  bgcolor: 'primary.dark',
                                  transform: 'translate(-50%, -50%)'
                                }}
                              />
                            ) : null}
                            {numericBoxPlotGeometry.percents.stdStart !== null &&
                            numericBoxPlotGeometry.percents.stdEnd !== null ? (
                              <Box
                                sx={{
                                  position: 'absolute',
                                  top: '25%',
                                  left: `${numericBoxPlotGeometry.percents.stdStart}%`,
                                  width: `${Math.max(
                                    numericBoxPlotGeometry.percents.stdEnd - numericBoxPlotGeometry.percents.stdStart,
                                    1
                                  )}%`,
                                  height: 2,
                                  bgcolor: 'warning.main'
                                }}
                              />
                            ) : null}
                            {numericBoxPlotGeometry.percents.mean !== null ? (
                              <Box
                                sx={{
                                  position: 'absolute',
                                  top: '25%',
                                  left: `${numericBoxPlotGeometry.percents.mean}%`,
                                  width: 10,
                                  height: 10,
                                  borderRadius: '50%',
                                  bgcolor: 'warning.main',
                                  transform: 'translate(-50%, -50%)'
                                }}
                              />
                            ) : null}
                          </Box>
                          <Stack
                            direction={{ xs: 'column', sm: 'row' }}
                            spacing={2}
                            flexWrap="wrap"
                            useFlexGap
                            sx={{ mt: 1.5 }}
                          >
                            {[
                              { label: 'Min', value: numericBoxPlotGeometry.values.min },
                              { label: 'P25', value: numericBoxPlotGeometry.values.p25 },
                              { label: 'Median', value: numericBoxPlotGeometry.values.median },
                              { label: 'P75', value: numericBoxPlotGeometry.values.p75 },
                              { label: 'Max', value: numericBoxPlotGeometry.values.max }
                            ].map((item) => (
                              <Stack key={item.label} spacing={0.25} sx={{ minWidth: 80 }}>
                                <Typography variant="caption" color="text.secondary">
                                  {item.label}
                                </Typography>
                                <Typography variant="subtitle2">{formatStatValue(item.value)}</Typography>
                              </Stack>
                            ))}
                          </Stack>
                          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ mt: 2 }} flexWrap="wrap" useFlexGap>
                            <Stack direction="row" spacing={1} alignItems="center">
                              <Box sx={{ width: 32, height: 2, bgcolor: 'warning.main' }} />
                              <Typography variant="caption" color="text.secondary">
                                Average — Standard Deviation
                              </Typography>
                            </Stack>
                            <Stack direction="row" spacing={1} alignItems="center">
                              <Box sx={{ width: 32, height: 2, bgcolor: 'text.secondary', position: 'relative' }}>
                                <Box sx={{ position: 'absolute', left: 0, width: 2, height: 8, bgcolor: 'text.secondary', top: -3 }} />
                                <Box sx={{ position: 'absolute', right: 0, width: 2, height: 8, bgcolor: 'text.secondary', top: -3 }} />
                              </Box>
                              <Typography variant="caption" color="text.secondary">
                                Minimum — Maximum
                              </Typography>
                            </Stack>
                            <Stack direction="row" spacing={1} alignItems="center">
                              <Box sx={{ width: 32, height: 12, borderRadius: 0.5, bgcolor: 'primary.light', border: '1px solid', borderColor: 'primary.main', position: 'relative' }}>
                                <Box sx={{ position: 'absolute', left: '50%', top: 0, bottom: 0, width: 2, bgcolor: 'primary.dark', transform: 'translateX(-50%)' }} />
                              </Box>
                              <Typography variant="caption" color="text.secondary">
                                25th — Median — 75th Percentile
                              </Typography>
                            </Stack>
                          </Stack>
                        </Box>
                      </>
                    ) : null}

                    <Divider />
                    {valueSamplesTable}
                  </Stack>
                ) : (
                  <Stack spacing={2}>
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
                    ) : null}
                    <Divider />
                    {valueSamplesTable}
                  </Stack>
                )}
              </Stack>
            </Paper>

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
