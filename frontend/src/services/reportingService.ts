import client from './api/client';
import type { ReportDesignerDefinition } from '../types/reporting';

export interface ReportPreviewResponse {
  sql: string;
  limit: number;
  rowCount: number;
  durationMs: number;
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface ReportPreviewPayload {
  definition: ReportDesignerDefinition;
  limit?: number;
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
