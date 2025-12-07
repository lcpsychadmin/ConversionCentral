import client from './api/client';
import type {
  ReportDatasetResponse,
  ReportDesignerDefinition,
  ReportDetail,
  ReportStatus,
  ReportSummary
} from '../types/reporting';

export interface ReportPreviewResponse {
  sql: string;
  limit: number;
  rowCount: number;
  durationMs: number;
  columns: string[];
  rows: Record<string, unknown>[];
}

export const fetchReportPreview = async (
  definition: ReportDesignerDefinition,
  limit = 100
): Promise<ReportPreviewResponse> => {
  const response = await client.post<ReportPreviewResponse>('/reporting/preview', {
    definition,
    limit
  });
  return response.data;
};

export const REPORTS_QUERY_KEY = ['reporting', 'reports'] as const;

export const reportQueryKeys = {
  list: (status?: ReportStatus, workspaceId?: string | null) =>
    [...REPORTS_QUERY_KEY, status ?? 'all', workspaceId ?? 'all'] as const,
  detail: (reportId: string) => [...REPORTS_QUERY_KEY, 'detail', reportId] as const,
  dataset: (reportId: string, limit: number) => [...REPORTS_QUERY_KEY, 'dataset', reportId, limit] as const
};

export interface ReportSavePayload {
  name: string;
  description?: string | null;
  definition: ReportDesignerDefinition;
  status?: ReportStatus;
  productTeamId?: string | null;
  dataObjectId?: string | null;
  workspaceId?: string | null;
}

export interface ReportUpdatePayload {
  name?: string;
  description?: string | null;
  definition?: ReportDesignerDefinition;
  status?: ReportStatus;
  productTeamId?: string | null;
  dataObjectId?: string | null;
  workspaceId?: string | null;
}

export interface ReportPublishPayload {
  name?: string;
  description?: string | null;
  definition?: ReportDesignerDefinition;
  productTeamId: string;
  dataObjectId: string;
  workspaceId?: string | null;
}

export const listReports = async (
  status?: ReportStatus,
  workspaceId?: string | null
): Promise<ReportSummary[]> => {
  const params: Record<string, string> = {};
  if (status) {
    params.status = status;
  }
  if (workspaceId) {
    params.workspaceId = workspaceId;
  }
  const response = await client.get<ReportSummary[]>('/reporting/reports', {
    params: Object.keys(params).length ? params : undefined
  });
  return response.data;
};

export const fetchReport = async (reportId: string): Promise<ReportDetail> => {
  const response = await client.get<ReportDetail>(`/reporting/reports/${reportId}`);
  return response.data;
};

export const createReport = async (payload: ReportSavePayload): Promise<ReportDetail> => {
  const response = await client.post<ReportDetail>('/reporting/reports', payload);
  return response.data;
};

export const updateReport = async (
  reportId: string,
  payload: ReportUpdatePayload
): Promise<ReportDetail> => {
  const response = await client.put<ReportDetail>(`/reporting/reports/${reportId}`, payload);
  return response.data;
};

export const publishReport = async (
  reportId: string,
  payload: ReportPublishPayload
): Promise<ReportDetail> => {
  const response = await client.post<ReportDetail>(`/reporting/reports/${reportId}/publish`, payload);
  return response.data;
};

export const deleteReport = async (reportId: string): Promise<void> => {
  await client.delete(`/reporting/reports/${reportId}`);
};

export const fetchReportDataset = async (
  reportId: string,
  limit = 500
): Promise<ReportDatasetResponse> => {
  const response = await client.get<ReportDatasetResponse>(`/reporting/reports/${reportId}/dataset`, {
    params: { limit }
  });
  return response.data;
};

export const exportReportDataset = async (
  reportId: string,
  options: { limit?: number; filename?: string } = {}
): Promise<void> => {
  const response = await client.get<Blob>(`/reporting/reports/${reportId}/export`, {
    params: options.limit ? { limit: options.limit } : undefined,
    responseType: 'blob'
  });

  const rawData = response.data;
  if (!rawData) {
    return;
  }

  const blob = rawData instanceof Blob
    ? rawData
    : new Blob([rawData as BlobPart], { type: 'text/csv;charset=utf-8' });
  if (blob.size === 0) {
    return;
  }

  const blobUrl = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = blobUrl;
  link.download = options.filename ?? `report-${reportId}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(blobUrl);
};
