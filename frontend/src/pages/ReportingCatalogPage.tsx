import { useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Typography
} from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';
import RefreshIcon from '@mui/icons-material/Refresh';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import { isAxiosError } from 'axios';
import { useQuery } from 'react-query';
import { SimpleTreeView } from '@mui/x-tree-view/SimpleTreeView';
import { TreeItem } from '@mui/x-tree-view/TreeItem';
import { useNavigate } from 'react-router-dom';

import PageHeader from '@components/common/PageHeader';
import ConfirmDialog from '@components/common/ConfirmDialog';
import ReportCatalogGrid from '@components/reporting/ReportCatalogGrid';
import {
  exportReportDataset,
  fetchReportDataset,
  deleteReport,
  listReports,
  reportQueryKeys
} from '@services/reportingService';
import type { ReportSummary } from '@cc-types/reporting';

const DATASET_LIMIT = 500;

const UNASSIGNED_PROCESS_AREA_LABEL = 'Unassigned Process Area';
const UNASSIGNED_DATA_OBJECT_LABEL = 'Unassigned Data Object';

type CatalogBranchNode = {
  id: string;
  label: string;
  type: 'productTeam' | 'dataObject';
  reportCount: number;
  children: CatalogTreeNode[];
};

type CatalogReportNode = {
  id: string;
  label: string;
  type: 'report';
  report: ReportSummary;
};

type CatalogTreeNode = CatalogBranchNode | CatalogReportNode;

const formatDateTime = (value: string | null | undefined): string => {
  if (!value) {
    return '—';
  }
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
};

const ReportingCatalogPage = () => {
  const navigate = useNavigate();
  const [selectedReportId, setSelectedReportId] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [deletingReportId, setDeletingReportId] = useState<string | null>(null);
  const [pendingDeleteReport, setPendingDeleteReport] = useState<ReportSummary | null>(null);
  const [expandedItems, setExpandedItems] = useState<string[]>([]);

  const {
    data: publishedReports = [],
    isLoading: reportsLoading,
    isError: reportsError,
    error: reportsErrorRaw,
    refetch: refetchReports
  } = useQuery(reportQueryKeys.list('published'), () => listReports('published'), {
    staleTime: 60_000,
    onSuccess: (reports) => {
      if (reports.length > 0 && !selectedReportId) {
        setSelectedReportId(reports[0].id);
      }
    }
  });

  useEffect(() => {
    if (publishedReports.length === 0) {
      setSelectedReportId(null);
    } else if (selectedReportId && !publishedReports.some((report) => report.id === selectedReportId)) {
      setSelectedReportId(publishedReports[0]?.id ?? null);
    }
  }, [publishedReports, selectedReportId]);

  const catalogTree = useMemo<CatalogTreeNode[]>(() => {
    if (publishedReports.length === 0) {
      return [];
    }

    const productTeamNodes = new Map<string, CatalogBranchNode>();
    const dataObjectNodes = new Map<string, CatalogBranchNode>();

    publishedReports.forEach((report) => {
      const productTeamKey = report.productTeamId ?? 'unassigned';
      const productTeamLabel = report.productTeamName?.trim() || UNASSIGNED_PROCESS_AREA_LABEL;

      let productTeamNode = productTeamNodes.get(productTeamKey);
      if (!productTeamNode) {
        productTeamNode = {
          id: `pt-${productTeamKey}`,
          label: productTeamLabel,
          type: 'productTeam',
          reportCount: 0,
          children: []
        } satisfies CatalogBranchNode;
        productTeamNodes.set(productTeamKey, productTeamNode);
      }
      productTeamNode.reportCount += 1;

      const dataObjectKey = `${productTeamKey}::${report.dataObjectId ?? 'unassigned'}`;
      const dataObjectLabel = report.dataObjectName?.trim() || UNASSIGNED_DATA_OBJECT_LABEL;

      let dataObjectNode = dataObjectNodes.get(dataObjectKey);
      if (!dataObjectNode) {
        dataObjectNode = {
          id: `do-${dataObjectKey}`,
          label: dataObjectLabel,
          type: 'dataObject',
          reportCount: 0,
          children: []
        } satisfies CatalogBranchNode;
        dataObjectNodes.set(dataObjectKey, dataObjectNode);
        productTeamNode.children.push(dataObjectNode);
      }
      dataObjectNode.reportCount += 1;

      dataObjectNode.children.push({
        id: `report-${report.id}`,
        label: report.name,
        type: 'report',
        report
      } satisfies CatalogReportNode);
    });

    const sortNodes = (nodes: CatalogTreeNode[]) => {
      nodes.sort((a, b) => a.label.localeCompare(b.label));
      nodes.forEach((node) => {
        if (node.type !== 'report') {
          sortNodes(node.children);
        }
      });
    };

    const roots = Array.from(productTeamNodes.values());
    sortNodes(roots);
    return roots;
  }, [publishedReports]);

  const autoExpandedItems = useMemo(() => {
    const ids: string[] = [];
    const traverse = (node: CatalogTreeNode) => {
      if (node.type !== 'report') {
        ids.push(node.id);
        node.children.forEach(traverse);
      }
    };
    catalogTree.forEach(traverse);
    return ids;
  }, [catalogTree]);

  const allGroupsExpanded = useMemo(() => {
    if (autoExpandedItems.length === 0) {
      return false;
    }
    const expandedSet = new Set(expandedItems);
    return autoExpandedItems.every((id) => expandedSet.has(id));
  }, [autoExpandedItems, expandedItems]);

  // Keep tree collapsed by default. Allow user to expand manually or with the
  // controls in the UI.

  const {
    data: dataset,
    isFetching: datasetLoading,
    isError: datasetError,
    error: datasetErrorRaw,
    refetch: refetchDataset
  } = useQuery(
    selectedReportId ? reportQueryKeys.dataset(selectedReportId, DATASET_LIMIT) : ['reporting', 'dataset', 'none'],
    () => fetchReportDataset(selectedReportId!, DATASET_LIMIT),
    {
      enabled: Boolean(selectedReportId),
      staleTime: 30_000
    }
  );

  const datasetColumns = dataset?.columns ?? [];
  const datasetRows = dataset?.rows ?? [];

  const selectedReport = useMemo(() => {
    if (!selectedReportId) {
      return null;
    }
    return publishedReports.find((report) => report.id === selectedReportId) ?? null;
  }, [publishedReports, selectedReportId]);

  const datasetSubtitle = useMemo(() => {
    if (!dataset) {
      return 'Select a published report to preview results.';
    }
    return `Showing up to ${dataset.limit.toLocaleString()} rows. Generated ${formatDateTime(dataset.generatedAt)}.`;
  }, [dataset]);

  const handleReportSelect = (report: ReportSummary) => {
    if (report.id === selectedReportId) {
      return;
    }
    setSelectedReportId(report.id);
    setStatusMessage(null);
  };

  const handleModifyReport = (report: ReportSummary) => {
    navigate('/reporting/designer', { state: { reportId: report.id } });
  };

  const handleDeleteReport = async (report: ReportSummary) => {
    if (deletingReportId) {
      return;
    }

    setDeletingReportId(report.id);
    setStatusMessage(null);

    try {
      await deleteReport(report.id);
      if (selectedReportId === report.id) {
        setSelectedReportId(null);
      }
      await refetchReports();
      setStatusMessage(`Report "${report.name}" deleted.`);
    } catch (error) {
      const message = isAxiosError(error)
        ? error.response?.data?.detail ?? error.message
        : error instanceof Error
          ? error.message
          : 'Unable to delete the report. Please try again.';
      setStatusMessage(message);
    } finally {
      setDeletingReportId(null);
    }
  };

  const requestDeleteReport = (report: ReportSummary) => {
    if (deletingReportId) {
      return;
    }
    setPendingDeleteReport(report);
  };

  const handleConfirmDelete = async () => {
    if (!pendingDeleteReport) {
      return;
    }
    const report = pendingDeleteReport;
    setPendingDeleteReport(null);
    await handleDeleteReport(report);
  };

  const handleCancelDelete = () => {
    setPendingDeleteReport(null);
  };

  const handleExport = async () => {
    if (!selectedReportId) {
      return;
    }
    setExporting(true);
    setStatusMessage(null);
    try {
      const filenameBase =
        selectedReport?.name?.trim().replace(/\s+/g, '_').toLowerCase() || `report-${selectedReportId}`;
      await exportReportDataset(selectedReportId, {
        limit: DATASET_LIMIT,
        filename: `${filenameBase}.csv`
      });
      setStatusMessage('Export generated successfully.');
    } catch (error) {
      const message = isAxiosError(error)
        ? error.response?.data?.detail ?? error.message
        : error instanceof Error
          ? error.message
          : 'Unable to export the report dataset right now.';
      setStatusMessage(message);
    } finally {
      setExporting(false);
    }
  };

  const reportsErrorMessage = useMemo(() => {
    if (!reportsError) {
      return null;
    }
    if (isAxiosError(reportsErrorRaw)) {
      return reportsErrorRaw.response?.data?.detail ?? reportsErrorRaw.message;
    }
    if (reportsErrorRaw instanceof Error) {
      return reportsErrorRaw.message;
    }
    return 'Unable to load published reports.';
  }, [reportsError, reportsErrorRaw]);

  const datasetErrorMessage = useMemo(() => {
    if (!datasetError) {
      return null;
    }
    if (isAxiosError(datasetErrorRaw)) {
      return datasetErrorRaw.response?.data?.detail ?? datasetErrorRaw.message;
    }
    if (datasetErrorRaw instanceof Error) {
      return datasetErrorRaw.message;
    }
    return 'Unable to load the published dataset.';
  }, [datasetError, datasetErrorRaw]);

  const renderTreeItem = (node: CatalogTreeNode): ReactNode => {
    const isReportNode = node.type === 'report';
    const isSelectedReport = isReportNode && node.report.id === selectedReportId;

    const handleItemClick = () => {
      if (isReportNode) {
        handleReportSelect(node.report);
      }
    };

    const reportMetadata = isReportNode
      ? `Published ${formatDateTime(node.report.publishedAt ?? node.report.updatedAt)}`
      : `${node.reportCount} report${node.reportCount === 1 ? '' : 's'}`;

    return (
      <TreeItem
        key={node.id}
        itemId={node.id}
        label={
          <Box
            onClick={handleItemClick}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              px: 0.5,
              py: 0.5,
              borderRadius: 1,
              bgcolor: isSelectedReport ? 'action.selected' : 'transparent',
              cursor: isReportNode ? 'pointer' : 'default'
            }}
          >
            <Box sx={{ flex: 1, minWidth: 0 }}>
              <Typography
                variant="body2"
                sx={{ fontWeight: isReportNode ? 600 : 500, overflow: 'hidden', textOverflow: 'ellipsis' }}
              >
                {node.label}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {reportMetadata}
              </Typography>
            </Box>
            {isReportNode ? (
              <Chip
                size="small"
                color={deletingReportId === node.report.id ? 'warning' : 'success'}
                label={deletingReportId === node.report.id ? 'Deleting…' : 'Published'}
              />
            ) : (
              <Chip
                size="small"
                variant="outlined"
                label={`${node.reportCount} report${node.reportCount === 1 ? '' : 's'}`}
              />
            )}
          </Box>
        }
        onClick={handleItemClick}
      >
        {node.type !== 'report' && node.children.map((child) => renderTreeItem(child))}
      </TreeItem>
    );
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, minHeight: '100vh' }}>
      <PageHeader
        title="Report Catalog"
        subtitle="Browse published reports grouped by process area and data object, preview result sets, and export to CSV."
      />

      <Stack direction={{ xs: 'column', lg: 'row' }} spacing={2} alignItems={{ xs: 'stretch', lg: 'flex-start' }}>
        <Paper
          elevation={1}
          sx={{
            width: { xs: '100%', lg: 320 },
            flexShrink: 0,
            p: 2,
            display: 'flex',
            flexDirection: 'column',
            gap: 2,
            minHeight: { xs: 200, lg: 520 }
          }}
        >
          <Stack spacing={1}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Published Reports
            </Typography>
            <Stack
              direction={{ xs: 'column', sm: 'row' }}
              spacing={1}
              alignItems={{ xs: 'stretch', sm: 'center' }}
              justifyContent="flex-start"
            >
              <Button
                size="small"
                startIcon={<RefreshIcon fontSize="small" />}
                onClick={() => refetchReports()}
                disabled={reportsLoading}
              >
                Refresh
              </Button>
              <Button
                size="small"
                startIcon={
                  allGroupsExpanded ? (
                    <ExpandLessIcon fontSize="small" />
                  ) : (
                    <ExpandMoreIcon fontSize="small" />
                  )
                }
                onClick={() => setExpandedItems(allGroupsExpanded ? [] : autoExpandedItems)}
                disabled={reportsLoading || autoExpandedItems.length === 0}
              >
                {allGroupsExpanded ? 'Collapse all' : 'Expand all'}
              </Button>
            </Stack>
          </Stack>

          {reportsLoading && (
            <Stack alignItems="center" spacing={1} sx={{ py: 4 }}>
              <CircularProgress size={24} />
              <Typography variant="body2" color="text.secondary">
                Loading published reports…
              </Typography>
            </Stack>
          )}

          {!reportsLoading && reportsErrorMessage && (
            <Alert severity="error">{reportsErrorMessage}</Alert>
          )}

          {!reportsLoading && !reportsError && publishedReports.length === 0 && (
            <Typography variant="body2" color="text.secondary">
              No reports have been published yet. Publish a report from the designer to see it here.
            </Typography>
          )}

          {!reportsLoading && !reportsError && publishedReports.length > 0 && (
            <SimpleTreeView
              aria-label="Published reports"
              expandedItems={expandedItems}
              onExpandedItemsChange={(_, itemIds) => setExpandedItems(itemIds)}
              slots={{ collapseIcon: ExpandMoreIcon, expandIcon: ChevronRightIcon }}
              sx={{ flex: 1, overflowY: 'auto', pr: 1 }}
            >
              {catalogTree.map((node) => renderTreeItem(node))}
            </SimpleTreeView>
          )}
        </Paper>

        <Box sx={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Paper elevation={1} sx={{ p: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} justifyContent="space-between" alignItems={{ xs: 'flex-start', md: 'center' }}>
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {dataset?.name ?? 'Select a report'}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {datasetSubtitle}
                </Typography>
              </Box>
              <Stack
                direction="row"
                spacing={1}
                alignItems="center"
                flexWrap="wrap"
                justifyContent={{ xs: 'flex-start', md: 'flex-end' }}
              >
                <Button
                  variant="outlined"
                  startIcon={<EditOutlinedIcon />}
                  disabled={!selectedReport}
                  onClick={() => {
                    if (selectedReport) {
                      handleModifyReport(selectedReport);
                    }
                  }}
                >
                  Modify
                </Button>
                <Button
                  variant="outlined"
                  color="error"
                  startIcon={
                    deletingReportId === selectedReport?.id ? <CircularProgress size={16} /> : <DeleteOutlineIcon />
                  }
                  disabled={!selectedReport || deletingReportId === selectedReport?.id}
                  onClick={() => {
                    if (selectedReport) {
                      requestDeleteReport(selectedReport);
                    }
                  }}
                >
                  Delete
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<RefreshIcon />}
                  disabled={!selectedReportId || datasetLoading}
                  onClick={() => {
                    setStatusMessage(null);
                    if (selectedReportId) {
                      refetchDataset();
                    }
                  }}
                >
                  Refresh data
                </Button>
                <Button
                  variant="contained"
                  startIcon={exporting ? <CircularProgress size={16} /> : <DownloadIcon />}
                  disabled={!dataset || datasetRows.length === 0 || exporting || datasetLoading}
                  onClick={handleExport}
                >
                  Export CSV
                </Button>
              </Stack>
            </Stack>

            {datasetErrorMessage && (
              <Alert severity="error">{datasetErrorMessage}</Alert>
            )}

            <ReportCatalogGrid
              columns={datasetColumns}
              rows={datasetRows}
              loading={datasetLoading}
              emptyMessage={selectedReportId ? 'No rows returned for this report. Try refreshing or adjust the definition.' : 'Select a published report to preview its output.'}
            />

            {statusMessage && (
              <Typography variant="caption" color="text.secondary">
                {statusMessage}
              </Typography>
            )}
          </Paper>
        </Box>
      </Stack>
      <ConfirmDialog
        open={Boolean(pendingDeleteReport)}
        title="Delete Report"
        description={`Delete published report "${pendingDeleteReport?.name ?? ''}"? This action cannot be undone.`}
        confirmLabel="Delete"
        onClose={handleCancelDelete}
        onConfirm={handleConfirmDelete}
        loading={Boolean(deletingReportId)}
        confirmDisabled={Boolean(deletingReportId)}
      />
    </Box>
  );
};

export default ReportingCatalogPage;
