import client from './api/client';
import {
  IngestionRun,
  IngestionSchedule,
  IngestionScheduleInput,
  IngestionScheduleUpdateInput,
  IngestionLoadStrategy
} from '../types/data';

interface IngestionScheduleResponse {
  id: string;
  connection_table_selection_id: string;
  schedule_expression: string;
  timezone?: string | null;
  load_strategy: IngestionLoadStrategy;
  watermark_column?: string | null;
  primary_key_column?: string | null;
  target_schema?: string | null;
  target_table_name?: string | null;
  batch_size: number;
  is_active: boolean;
  last_watermark_timestamp?: string | null;
  last_watermark_id?: number | null;
  last_run_started_at?: string | null;
  last_run_completed_at?: string | null;
  last_run_status?: string | null;
  last_run_error?: string | null;
  total_runs: number;
  total_rows_loaded: number;
  created_at?: string;
  updated_at?: string;
}

interface IngestionRunResponse {
  id: string;
  ingestion_schedule_id: string;
  status: 'scheduled' | 'running' | 'completed' | 'failed';
  started_at?: string | null;
  completed_at?: string | null;
  rows_loaded?: number | null;
  watermark_timestamp_before?: string | null;
  watermark_timestamp_after?: string | null;
  watermark_id_before?: number | null;
  watermark_id_after?: number | null;
  query_text?: string | null;
  error_message?: string | null;
  created_at?: string;
  updated_at?: string;
}

const mapIngestionSchedule = (payload: IngestionScheduleResponse): IngestionSchedule => ({
  id: payload.id,
  connectionTableSelectionId: payload.connection_table_selection_id,
  scheduleExpression: payload.schedule_expression,
  timezone: payload.timezone ?? null,
  loadStrategy: payload.load_strategy,
  watermarkColumn: payload.watermark_column ?? null,
  primaryKeyColumn: payload.primary_key_column ?? null,
  targetSchema: payload.target_schema ?? null,
  targetTableName: payload.target_table_name ?? null,
  batchSize: payload.batch_size,
  isActive: payload.is_active,
  lastWatermarkTimestamp: payload.last_watermark_timestamp ?? null,
  lastWatermarkId: payload.last_watermark_id ?? null,
  lastRunStartedAt: payload.last_run_started_at ?? null,
  lastRunCompletedAt: payload.last_run_completed_at ?? null,
  lastRunStatus: payload.last_run_status ?? null,
  lastRunError: payload.last_run_error ?? null,
  totalRuns: payload.total_runs,
  totalRowsLoaded: payload.total_rows_loaded,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

const mapIngestionRun = (payload: IngestionRunResponse): IngestionRun => ({
  id: payload.id,
  ingestionScheduleId: payload.ingestion_schedule_id,
  status: payload.status,
  startedAt: payload.started_at ?? null,
  completedAt: payload.completed_at ?? null,
  rowsLoaded: payload.rows_loaded ?? null,
  watermarkTimestampBefore: payload.watermark_timestamp_before ?? null,
  watermarkTimestampAfter: payload.watermark_timestamp_after ?? null,
  watermarkIdBefore: payload.watermark_id_before ?? null,
  watermarkIdAfter: payload.watermark_id_after ?? null,
  queryText: payload.query_text ?? null,
  errorMessage: payload.error_message ?? null,
  createdAt: payload.created_at,
  updatedAt: payload.updated_at
});

export const fetchIngestionSchedules = async (): Promise<IngestionSchedule[]> => {
  const response = await client.get<IngestionScheduleResponse[]>('/ingestion-schedules');
  return response.data.map(mapIngestionSchedule);
};

export const createIngestionSchedule = async (
  input: IngestionScheduleInput
): Promise<IngestionSchedule> => {
  const response = await client.post<IngestionScheduleResponse>('/ingestion-schedules', {
    connection_table_selection_id: input.connectionTableSelectionId,
    schedule_expression: input.scheduleExpression,
    timezone: input.timezone ?? null,
    load_strategy: input.loadStrategy,
    watermark_column: input.watermarkColumn ?? null,
    primary_key_column: input.primaryKeyColumn ?? null,
    target_schema: input.targetSchema ?? null,
    target_table_name: input.targetTableName ?? null,
    batch_size: input.batchSize,
    is_active: input.isActive
  });
  return mapIngestionSchedule(response.data);
};

export const updateIngestionSchedule = async (
  id: string,
  input: IngestionScheduleUpdateInput
): Promise<IngestionSchedule> => {
  const response = await client.put<IngestionScheduleResponse>(`/ingestion-schedules/${id}`, {
    ...(input.scheduleExpression !== undefined
      ? { schedule_expression: input.scheduleExpression }
      : {}),
    ...(input.timezone !== undefined ? { timezone: input.timezone } : {}),
    ...(input.loadStrategy !== undefined ? { load_strategy: input.loadStrategy } : {}),
    ...(input.watermarkColumn !== undefined ? { watermark_column: input.watermarkColumn } : {}),
    ...(input.primaryKeyColumn !== undefined
      ? { primary_key_column: input.primaryKeyColumn }
      : {}),
    ...(input.targetSchema !== undefined ? { target_schema: input.targetSchema } : {}),
    ...(input.targetTableName !== undefined
      ? { target_table_name: input.targetTableName }
      : {}),
    ...(input.batchSize !== undefined ? { batch_size: input.batchSize } : {}),
    ...(input.isActive !== undefined ? { is_active: input.isActive } : {})
  });
  return mapIngestionSchedule(response.data);
};

export const triggerIngestionSchedule = async (id: string): Promise<void> => {
  await client.post(`/ingestion-schedules/${id}/run`);
};

export const fetchIngestionRuns = async (scheduleId: string): Promise<IngestionRun[]> => {
  const response = await client.get<IngestionRunResponse[]>(`/ingestion-schedules/${scheduleId}/runs`);
  return response.data.map(mapIngestionRun);
};
