import client from './api/client';
const mapIngestionSchedule = (payload) => ({
    id: payload.id,
    connectionTableSelectionId: payload.connectionTableSelectionId,
    scheduleExpression: payload.scheduleExpression,
    timezone: payload.timezone ?? null,
    loadStrategy: payload.loadStrategy,
    watermarkColumn: payload.watermarkColumn ?? null,
    primaryKeyColumn: payload.primaryKeyColumn ?? null,
    targetSchema: payload.targetSchema ?? null,
    targetTableName: payload.targetTableName ?? null,
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
const mapIngestionRun = (payload) => ({
    id: payload.id,
    ingestionScheduleId: payload.ingestionScheduleId,
    status: payload.status,
    startedAt: payload.startedAt ?? null,
    completedAt: payload.completedAt ?? null,
    rowsLoaded: payload.rowsLoaded ?? null,
    watermarkTimestampBefore: payload.watermarkTimestampBefore ?? null,
    watermarkTimestampAfter: payload.watermarkTimestampAfter ?? null,
    watermarkIdBefore: payload.watermarkIdBefore ?? null,
    watermarkIdAfter: payload.watermarkIdAfter ?? null,
    queryText: payload.queryText ?? null,
    errorMessage: payload.errorMessage ?? null,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const fetchIngestionSchedules = async () => {
    const response = await client.get('/ingestion-schedules');
    return response.data.map(mapIngestionSchedule);
};
export const createIngestionSchedule = async (input) => {
    const response = await client.post('/ingestion-schedules', {
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
export const updateIngestionSchedule = async (id, input) => {
    const response = await client.put(`/ingestion-schedules/${id}`, {
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
export const triggerIngestionSchedule = async (id) => {
    await client.post(`/ingestion-schedules/${id}/run`);
};
export const fetchIngestionRuns = async (scheduleId) => {
    const response = await client.get(`/ingestion-schedules/${scheduleId}/runs`);
    return response.data.map(mapIngestionRun);
};
