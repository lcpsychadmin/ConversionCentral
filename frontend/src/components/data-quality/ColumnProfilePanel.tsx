import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Divider,
  Grid,
  LinearProgress,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import {
  DataQualityColumnMetric,
  DataQualityColumnProfile,
  DataQualityColumnValueFrequency,
  DataQualityColumnHistogramBin,
  DataQualityProfileAnomaly,
  DataQualityNumericColumnProfile,
  DataQualityTextColumnProfile,
  DataQualityColumnIssueSummary
} from '@cc-types/data';

interface ColumnProfilePanelProps {
  profile?: DataQualityColumnProfile | null;
  isLoading?: boolean;
  error?: string | null;
  variant?: 'inline' | 'full';
  emptyHint?: string;
}

const DEFAULT_EMPTY_MESSAGE =
  'No profiling runs have produced column statistics yet. Launch a profiling job to populate this view.';

const formatCount = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return Number.isInteger(value) ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
};

const formatPercentage = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(2)}%`;
};

const formatMetric = (metric: DataQualityColumnMetric): string => {
  if (metric.formatted && metric.formatted.trim().length > 0) {
    return metric.formatted;
  }
  if (metric.value === null || metric.value === undefined) {
    return '—';
  }
  if (typeof metric.value === 'number') {
    const formatted = Number.isInteger(metric.value)
      ? metric.value.toLocaleString()
      : metric.value.toLocaleString(undefined, { maximumFractionDigits: 4 });
    if (!metric.unit) {
      return formatted;
    }
    return metric.unit === '%' ? `${formatted}${metric.unit}` : `${formatted} ${metric.unit}`;
  }
  return String(metric.value);
};

const formatDate = (value?: string | null): string => {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '—';
  }
  return parsed.toLocaleString();
};

const formatFrequencyValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toLocaleString() : value.toString();
  }
  if (typeof value === 'boolean') {
    return value ? 'True' : 'False';
  }
  return String(value);
};

const IssueSummary = ({ title, summary }: { title: string; summary: DataQualityColumnIssueSummary | null }) => {
  if (!summary || (!summary.total && !summary.items?.length)) {
    return null;
  }

  return (
    <Paper variant="outlined" sx={{ p: 2 }}>
      <Stack spacing={1}>
        <Typography variant="subtitle2">{title}</Typography>
        <Stack direction="row" spacing={1} alignItems="center">
          <Typography variant="body2" fontWeight={600}>
            {formatCount(summary.total ?? summary.items?.length ?? 0)} issues
          </Typography>
          {summary.severityCounts
            ? Object.entries(summary.severityCounts).map(([severity, count]) => (
                <Chip key={severity} size="small" label={`${severity}: ${count}`} />
              ))
            : null}
        </Stack>
        {summary.items?.length ? (
          <Stack spacing={1}>
            {summary.items.slice(0, 3).map((item) => (
              <Alert key={`${item.anomalyType}-${item.detectedAt ?? item.description}`} severity="warning">
                <Stack spacing={0.25}>
                  <Typography variant="subtitle2">{item.anomalyType}</Typography>
                  <Typography variant="body2">{item.description}</Typography>
                  <Typography variant="caption" color="text.secondary">
                    {formatDate(item.detectedAt)}
                  </Typography>
                </Stack>
              </Alert>
            ))}
            {summary.items.length > 3 ? (
              <Typography variant="caption" color="text.secondary">
                +{summary.items.length - 3} more listed under anomalies
              </Typography>
            ) : null}
          </Stack>
        ) : null}
      </Stack>
    </Paper>
  );
};

const NumericProfile = ({ profile }: { profile: DataQualityNumericColumnProfile }) => {
  const stats = profile.stats;
  const distribution = profile.distributionBars ?? [];
  const histogram = profile.histogram ?? [];
  const topValues = profile.topValues ?? [];
  const histogramMax = histogram.reduce<number>((max, bin) => {
    const count = typeof bin.count === 'number' ? bin.count : 0;
    return count > max ? count : max;
  }, 0);

  return (
    <Stack spacing={2}>
      {stats ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Numeric Summary
          </Typography>
          <Grid container spacing={2}>
            {Object.entries(stats)
              .filter(([, value]) => value !== null && value !== undefined)
              .map(([key, value]) => (
                <Grid item xs={6} md={4} key={key}>
                  <Typography variant="body2" color="text.secondary">
                    {key.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase())}
                  </Typography>
                  <Typography variant="subtitle2">
                    {typeof value === 'number' ? formatCount(value) : String(value)}
                  </Typography>
                </Grid>
              ))}
          </Grid>
        </Paper>
      ) : null}

      {distribution.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Distribution
          </Typography>
          <Stack spacing={1.25}>
            {distribution.map((entry) => (
              <Box key={entry.label ?? entry.key}>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2">{entry.label ?? entry.key}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                  </Typography>
                </Stack>
                <LinearProgress
                  variant="determinate"
                  value={Math.min(Math.max(entry.percentage ?? 0, 0), 100)}
                  sx={{ mt: 0.5, height: 6, borderRadius: 999 }}
                />
              </Box>
            ))}
          </Stack>
        </Paper>
      ) : null}

      {histogram.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Histogram
          </Typography>
          <Stack spacing={1.25}>
            {histogram.map((bin, idx) => (
              <Box key={`${bin.label}-${idx}`}>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2">{bin.label || 'Range'}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {formatCount(bin.count)}
                  </Typography>
                </Stack>
                <LinearProgress
                  variant="determinate"
                  value={histogramMax > 0 ? (Math.max(bin.count ?? 0, 0) / histogramMax) * 100 : 0}
                  sx={{ mt: 0.5, height: 6, borderRadius: 999 }}
                />
              </Box>
            ))}
          </Stack>
        </Paper>
      ) : null}

      {topValues.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Top Values
          </Typography>
          <Stack spacing={0.5}>
            {topValues.slice(0, 5).map((entry, idx) => (
              <Stack key={`${String(entry.value)}-${idx}`} direction="row" justifyContent="space-between">
                <Typography variant="body2">{formatFrequencyValue(entry.value)}</Typography>
                <Typography variant="body2" color="text.secondary">
                  {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      ) : null}
    </Stack>
  );
};

const TextProfile = ({ profile }: { profile: DataQualityTextColumnProfile }) => {
  const stats = profile.stats;
  const missing = profile.missingBreakdown ?? [];
  const duplicate = profile.duplicateBreakdown ?? [];
  const caseBreakdown = profile.caseBreakdown ?? [];
  const patterns = profile.frequentPatterns ?? [];
  const topValues = profile.topValues ?? [];

  return (
    <Stack spacing={2}>
      {stats ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Text Summary
          </Typography>
          <Grid container spacing={2}>
            {Object.entries(stats)
              .filter(([, value]) => value !== null && value !== undefined)
              .slice(0, 9)
              .map(([key, value]) => (
                <Grid item xs={6} md={4} key={key}>
                  <Typography variant="body2" color="text.secondary">
                    {key.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase())}
                  </Typography>
                  <Typography variant="subtitle2">
                    {typeof value === 'number' ? formatCount(value) : String(value)}
                  </Typography>
                </Grid>
              ))}
          </Grid>
        </Paper>
      ) : null}

      {[missing, duplicate, caseBreakdown].some((group) => group.length) ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Value Quality
          </Typography>
          <Stack spacing={1.25}>
            {[{ title: 'Missing Values', data: missing }, { title: 'Duplicate Values', data: duplicate }, { title: 'Case Distribution', data: caseBreakdown }]
              .filter((section) => section.data.length)
              .map((section) => (
                <Box key={section.title}>
                  <Typography variant="body2" fontWeight={600}>
                    {section.title}
                  </Typography>
                  {section.data.map((entry) => (
                    <Stack key={`${section.title}-${entry.label}`} direction="row" justifyContent="space-between" alignItems="center">
                      <Typography variant="body2">{entry.label}</Typography>
                      <Typography variant="body2" color="text.secondary">
                        {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                      </Typography>
                    </Stack>
                  ))}
                </Box>
              ))}
          </Stack>
        </Paper>
      ) : null}

      {patterns.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Frequent Patterns
          </Typography>
          <Stack spacing={0.5}>
            {patterns.slice(0, 6).map((entry, idx) => (
              <Stack key={`${entry.label}-${idx}`} direction="row" justifyContent="space-between">
                <Typography variant="body2" fontFamily="monospace">
                  {entry.label || 'Pattern'}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      ) : null}

      {topValues.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Top Values
          </Typography>
          <Stack spacing={0.5}>
            {topValues.slice(0, 5).map((entry, idx) => (
              <Stack key={`${String(entry.value)}-${idx}`} direction="row" justifyContent="space-between">
                <Typography variant="body2">{formatFrequencyValue(entry.value)}</Typography>
                <Typography variant="body2" color="text.secondary">
                  {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      ) : null}
    </Stack>
  );
};

const LegacyMetrics = ({
  metrics,
  histogram,
  topValues
}: {
  metrics: DataQualityColumnMetric[];
  histogram: DataQualityColumnHistogramBin[];
  topValues: DataQualityColumnValueFrequency[];
}) => {
  const histogramMax = histogram.reduce<number>((max, bin) => {
    const count = typeof bin.count === 'number' ? bin.count : 0;
    return count > max ? count : max;
  }, 0);

  return (
    <Stack spacing={2}>
      {metrics.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Column Metrics
          </Typography>
          <Grid container spacing={2}>
            {metrics.map((metric) => (
              <Grid item xs={12} sm={6} md={4} key={metric.key}>
                <Typography variant="body2" color="text.secondary">
                  {metric.label}
                </Typography>
                <Typography variant="subtitle2">{formatMetric(metric)}</Typography>
              </Grid>
            ))}
          </Grid>
        </Paper>
      ) : null}

      {topValues.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Top Values
          </Typography>
          <Stack spacing={0.5}>
            {topValues.slice(0, 5).map((entry, idx) => (
              <Stack key={`${String(entry.value)}-${idx}`} direction="row" justifyContent="space-between">
                <Typography variant="body2">{formatFrequencyValue(entry.value)}</Typography>
                <Typography variant="body2" color="text.secondary">
                  {formatCount(entry.count)} ({formatPercentage(entry.percentage)})
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      ) : null}

      {histogram.length ? (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Distribution
          </Typography>
          <Stack spacing={1.25}>
            {histogram.map((bin, idx) => (
              <Box key={`${bin.label}-${idx}`}>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2">{bin.label || 'Range'}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {formatCount(bin.count)}
                  </Typography>
                </Stack>
                <LinearProgress
                  variant="determinate"
                  value={histogramMax > 0 ? (Math.max(bin.count ?? 0, 0) / histogramMax) * 100 : 0}
                  sx={{ mt: 0.5, height: 6, borderRadius: 999 }}
                />
              </Box>
            ))}
          </Stack>
        </Paper>
      ) : null}
    </Stack>
  );
};

const ColumnProfilePanel = ({
  profile,
  isLoading,
  error,
  variant = 'full',
  emptyHint = DEFAULT_EMPTY_MESSAGE
}: ColumnProfilePanelProps) => {
  if (isLoading) {
    return (
      <Box display="flex" justifyContent="center" py={variant === 'inline' ? 2 : 4}>
        <CircularProgress size={24} />
      </Box>
    );
  }

  if (error) {
    return <Alert severity="error">{error}</Alert>;
  }

  if (!profile) {
    return (
      <Typography variant="body2" color="text.secondary">
        {emptyHint}
      </Typography>
    );
  }

  const showCompact = variant === 'inline';
  const summaryItems = [
    { label: 'Status', value: profile.status ?? 'Pending' },
    { label: 'Profile Run', value: profile.profileRunId ?? '—' },
    { label: 'Rows Profiled', value: formatCount(profile.rowCount) },
    { label: 'Completed', value: formatDate(profile.completedAt) },
    { label: 'Started', value: formatDate(profile.startedAt) },
    { label: 'Anomalies', value: formatCount(profile.anomalies.length) }
  ];

  const layout = profile.profileLayout;

  return (
    <Stack spacing={showCompact ? 2 : 3}>
      <Stack spacing={0.25}>
        <Typography variant={showCompact ? 'subtitle1' : 'h6'}>{profile.columnName}</Typography>
        <Typography variant="body2" color="text.secondary">
          {profile.tableName ?? 'Unmapped table'}
        </Typography>
        {profile.dataType ? (
          <Typography variant="body2" color="text.secondary">
            Data type: {profile.dataType}
          </Typography>
        ) : null}
      </Stack>

      <Paper variant="outlined" sx={{ p: 2, backgroundColor: showCompact ? 'transparent' : 'grey.50' }}>
        <Grid container spacing={2}>
          {summaryItems.slice(0, showCompact ? 4 : summaryItems.length).map((item) => (
            <Grid item xs={12} sm={6} md={4} key={item.label}>
              <Typography variant="body2" color="text.secondary">
                {item.label}
              </Typography>
              <Typography variant="subtitle2">{item.value}</Typography>
            </Grid>
          ))}
        </Grid>
      </Paper>

      {layout ? (
        <Stack spacing={2}>
          <Stack direction="row" spacing={1} flexWrap="wrap">
            {layout.badges?.map((badge) => (
              <Chip key={badge} label={badge} size="small" />
            ))}
          </Stack>

          {layout.characteristics ? (
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle2" gutterBottom>
                Column Characteristics
              </Typography>
              <Grid container spacing={2}>
                {Object.entries(layout.characteristics)
                  .filter(([, value]) => value)
                  .map(([key, value]) => (
                    <Grid item xs={12} sm={6} md={4} key={key}>
                      <Typography variant="body2" color="text.secondary">
                        {key.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase())}
                      </Typography>
                      <Typography variant="subtitle2">{String(value)}</Typography>
                    </Grid>
                  ))}
              </Grid>
            </Paper>
          ) : null}

          {layout.tags ? (
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle2" gutterBottom>
                Catalog Tags
              </Typography>
              <Grid container spacing={2}>
                {Object.entries(layout.tags)
                  .filter(([, value]) => value !== null && value !== undefined)
                  .map(([key, value]) => (
                    <Grid item xs={12} sm={6} md={4} key={key}>
                      <Typography variant="body2" color="text.secondary">
                        {key.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase())}
                      </Typography>
                      <Typography variant="subtitle2">{String(value)}</Typography>
                    </Grid>
                  ))}
              </Grid>
            </Paper>
          ) : null}

          <IssueSummary title="Potential PII" summary={layout.piiSignals ?? null} />
          <IssueSummary title="Hygiene Issues" summary={layout.hygieneIssues ?? null} />
          <IssueSummary title="Test Issues" summary={layout.testIssues ?? null} />

          {layout.relatedTestSuites?.length ? (
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle2" gutterBottom>
                Related Test Suites
              </Typography>
              <Stack spacing={0.5}>
                {layout.relatedTestSuites.map((suite) => (
                  <Stack key={suite.testSuiteKey} direction="row" spacing={1} alignItems="center">
                    <Typography variant="body2" fontWeight={600}>
                      {suite.name}
                    </Typography>
                    {suite.severity ? (
                      <Chip label={suite.severity} size="small" />
                    ) : null}
                  </Stack>
                ))}
              </Stack>
            </Paper>
          ) : null}

          {layout.numericProfile ? <NumericProfile profile={layout.numericProfile} /> : null}
          {layout.textProfile ? <TextProfile profile={layout.textProfile} /> : null}
        </Stack>
      ) : null}

      {!layout ? (
        <LegacyMetrics metrics={profile.metrics} histogram={profile.histogram} topValues={profile.topValues} />
      ) : null}

      {profile.anomalies.length ? (
        <Stack spacing={1}>
          <Divider />
          <Typography variant="subtitle2">Recent Anomalies</Typography>
          <Stack spacing={1}>
            {profile.anomalies.map((anomaly: DataQualityProfileAnomaly, index) => (
              <Alert key={`${anomaly.anomalyType}-${anomaly.detectedAt ?? index}`} severity="warning">
                <Stack spacing={0.25}>
                  <Typography variant="subtitle2">{anomaly.anomalyType}</Typography>
                  <Typography variant="body2">{anomaly.description}</Typography>
                  <Typography variant="caption" color="text.secondary">
                    {formatDate(anomaly.detectedAt)}
                  </Typography>
                </Stack>
              </Alert>
            ))}
          </Stack>
        </Stack>
      ) : null}
    </Stack>
  );
};

export default ColumnProfilePanel;
