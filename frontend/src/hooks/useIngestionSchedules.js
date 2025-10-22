import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';
import { createIngestionSchedule, fetchIngestionSchedules, triggerIngestionSchedule, updateIngestionSchedule } from '../services/ingestionScheduleService';
import { useToast } from './useToast';
const INGESTION_SCHEDULES_KEY = ['ingestion-schedules'];
const getErrorMessage = (error) => {
    if (error instanceof AxiosError) {
        const detail = error.response?.data?.detail ??
            error.response?.data?.message;
        return detail ?? error.message;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return 'An unexpected error occurred.';
};
export const useIngestionSchedules = () => {
    const toast = useToast();
    const queryClient = useQueryClient();
    const invalidate = () => queryClient.invalidateQueries(INGESTION_SCHEDULES_KEY);
    const schedulesQuery = useQuery(INGESTION_SCHEDULES_KEY, fetchIngestionSchedules);
    const createMutation = useMutation(createIngestionSchedule, {
        onSuccess: () => {
            toast.showSuccess('Schedule created.');
            invalidate();
        },
        onError: (error) => toast.showError(getErrorMessage(error))
    });
    const updateMutation = useMutation(({ id, input }) => updateIngestionSchedule(id, input), {
        onSuccess: () => {
            toast.showSuccess('Schedule updated.');
            invalidate();
        },
        onError: (error) => toast.showError(getErrorMessage(error))
    });
    const triggerMutation = useMutation(triggerIngestionSchedule, {
        onSuccess: () => {
            toast.showSuccess('Ingestion run triggered.');
            invalidate();
        },
        onError: (error) => toast.showError(getErrorMessage(error))
    });
    return {
        schedulesQuery,
        createSchedule: createMutation.mutateAsync,
        updateSchedule: updateMutation.mutateAsync,
        triggerSchedule: triggerMutation.mutateAsync,
        creating: createMutation.isLoading,
        updating: updateMutation.isLoading,
        triggering: triggerMutation.isLoading
    };
};
