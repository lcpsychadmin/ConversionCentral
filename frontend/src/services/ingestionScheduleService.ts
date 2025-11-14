import client from './api/client';
import {
  DataWarehouseTarget,
  IngestionRun,
  IngestionSchedule,
  IngestionScheduleInput,
  IngestionScheduleUpdateInput,
  IngestionLoadStrategy
} from '../types/data';

interface IngestionScheduleResponse {
  id: string;
  connectionTableSelectionId: string;
  scheduleExpression: string;
  timezone?: string | null;
  loadStrategy: IngestionLoadStrategy;
  watermarkColumn?: string | null;
  primaryKeyColumn?: string | null;
  targetSchema?: string | null;
  targetTableName?: string | null;
  targetWarehouse: DataWarehouseTarget;
  sapHanaSettingId?: string | null;
  batchSize: number;
  isActive: boolean;
  lastWatermarkTimestamp?: string | null;
  lastWatermarkId?: number | null;
  lastRunStartedAt?: string | null;
  lastRunCompletedAt?: string | null;
  lastRunStatus?: string | null;
  lastRunError?: string | null;
  totalRuns: number;
  totalRowsLoaded: number;
  createdAt?: string;
  updatedAt?: string;
}

interface IngestionRunResponse {
  id: string;
  ingestionScheduleId: string;
  status: 'scheduled' | 'running' | 'completed' | 'failed';
  startedAt?: string | null;
  completedAt?: string | null;
  rowsLoaded?: number | null;
  rowsExpected?: number | null;
  watermarkTimestampBefore?: string | null;
  watermarkTimestampAfter?: string | null;
  watermarkIdBefore?: number | null;
  watermarkIdAfter?: number | null;
  queryText?: string | null;
  errorMessage?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

const mapIngestionSchedule = (payload: IngestionScheduleResponse): IngestionSchedule => ({
  id: payload.id,
  connectionTableSelectionId: payload.connectionTableSelectionId,
  scheduleExpression: payload.scheduleExpression,
  timezone: payload.timezone ?? null,
  loadStrategy: payload.loadStrategy,
  watermarkColumn: payload.watermarkColumn ?? null,
  primaryKeyColumn: payload.primaryKeyColumn ?? null,
  targetSchema: payload.targetSchema ?? null,
  targetTableName: payload.targetTableName ?? null,
  targetWarehouse: payload.targetWarehouse,
  sapHanaSettingId: payload.sapHanaSettingId ?? null,
  batchSize: payload.batchSize,
  isActive: payload.isActive,
  lastWatermarkTimestamp: payload.lastWatermarkTimestamp ?? null,
  lastWatermarkId: payload.lastWatermarkId ?? null,
  lastRunStartedAt: payload.lastRunStartedAt ?? null,
  lastRunCompletedAt: payload.lastRunCompletedAt ?? null,
  lastRunStatus: payload.lastRunStatus ?? null,
  lastRunError: payload.lastRunError ?? null,
  totalRuns: payload.totalRuns,
  totalRowsLoaded: payload.totalRowsLoaded,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
});

const mapIngestionRun = (payload: IngestionRunResponse): IngestionRun => ({
  id: payload.id,
  ingestionScheduleId: payload.ingestionScheduleId,
  status: payload.status,
  startedAt: payload.startedAt ?? null,
  completedAt: payload.completedAt ?? null,
  rowsLoaded: payload.rowsLoaded ?? null,
  rowsExpected: payload.rowsExpected ?? null,
  watermarkTimestampBefore: payload.watermarkTimestampBefore ?? null,
  watermarkTimestampAfter: payload.watermarkTimestampAfter ?? null,
  watermarkIdBefore: payload.watermarkIdBefore ?? null,
  watermarkIdAfter: payload.watermarkIdAfter ?? null,
  queryText: payload.queryText ?? null,
  errorMessage: payload.errorMessage ?? null,
  createdAt: payload.createdAt,
  updatedAt: payload.updatedAt
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
    target_warehouse: input.targetWarehouse,
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
  ...(input.targetWarehouse !== undefined ? { target_warehouse: input.targetWarehouse } : {}),
    ...(input.batchSize !== undefined ? { batch_size: input.batchSize } : {}),
    ...(input.isActive !== undefined ? { is_active: input.isActive } : {})
  });
  return mapIngestionSchedule(response.data);
};

export const triggerIngestionSchedule = async (id: string): Promise<void> => {
  await client.post(`/ingestion-schedules/${id}/run`);
};

export const deleteIngestionSchedule = async (id: string): Promise<void> => {
  await client.delete(`/ingestion-schedules/${id}`);
};

export const fetchIngestionRuns = async (scheduleId: string): Promise<IngestionRun[]> => {
  const response = await client.get<IngestionRunResponse[]>(`/ingestion-schedules/${scheduleId}/runs`);
  return response.data.map(mapIngestionRun);
};

export const abortIngestionRun = async (scheduleId: string, runId: string): Promise<void> => {
  await client.post(`/ingestion-schedules/${scheduleId}/runs/${runId}/abort`);
};

export interface CleanupStuckRunsResponse {
  detail: string;
  expired_runs: number;
}

export const cleanupStuckIngestionRuns = async (): Promise<CleanupStuckRunsResponse> => {
  const response = await client.post<CleanupStuckRunsResponse>('/ingestion-schedules/cleanup-stuck-runs');
  return response.data;
};
