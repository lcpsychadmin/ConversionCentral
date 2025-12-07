import client from './api/client';
import type {
  TableObservabilityMetric,
  TableObservabilitySchedule,
  TableObservabilityScheduleUpdateInput
} from '@cc-types/data';

export interface TableObservabilityMetricFilters {
  runId?: string;
  selectionId?: string;
  systemConnectionId?: string;
  schemaName?: string;
  tableName?: string;
  categoryKey?: string;
  metricName?: string;
  limit?: number;
}

export const fetchTableObservabilitySchedules = async (): Promise<TableObservabilitySchedule[]> => {
  const response = await client.get<TableObservabilitySchedule[]>('/table-observability/schedules');
  return response.data;
};

export const updateTableObservabilitySchedule = async (
  scheduleId: string,
  input: TableObservabilityScheduleUpdateInput
): Promise<TableObservabilitySchedule> => {
  const payload: Record<string, unknown> = {};
  if (input.cronExpression !== undefined) {
    payload.cron_expression = input.cronExpression;
  }
  if (input.timezone !== undefined) {
    payload.timezone = input.timezone;
  }
  if (input.isActive !== undefined) {
    payload.is_active = input.isActive;
  }

  const response = await client.put<TableObservabilitySchedule>(
    `/table-observability/schedules/${scheduleId}`,
    payload
  );
  return response.data;
};

export const triggerTableObservabilitySchedule = async (scheduleId: string): Promise<void> => {
  await client.post(`/table-observability/schedules/${scheduleId}/run`, {});
};

export const fetchTableObservabilityMetrics = async (
  filters: TableObservabilityMetricFilters
): Promise<TableObservabilityMetric[]> => {
  const response = await client.get<TableObservabilityMetric[]>('/table-observability/metrics', {
    params: {
      runId: filters.runId,
      selectionId: filters.selectionId,
      systemConnectionId: filters.systemConnectionId,
      schemaName: filters.schemaName,
      tableName: filters.tableName,
      categoryKey: filters.categoryKey,
      metricName: filters.metricName,
      limit: filters.limit
    }
  });
  return response.data;
};
