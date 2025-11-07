import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  abortIngestionRun,
  cleanupStuckIngestionRuns,
  createIngestionSchedule,
  deleteIngestionSchedule,
  fetchIngestionSchedules,
  triggerIngestionSchedule,
  updateIngestionSchedule
} from '../services/ingestionScheduleService';
import { IngestionSchedule, IngestionScheduleUpdateInput } from '../types/data';
import { useToast } from './useToast';

const INGESTION_SCHEDULES_KEY = ['ingestion-schedules'];

type UpdateArgs = {
  id: string;
  input: IngestionScheduleUpdateInput;
};

type AbortArgs = {
  scheduleId: string;
  runId: string;
};

type CleanupResult = {
  detail: string;
  expired_runs: number;
};

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    const detail =
      (error.response?.data as { detail?: string; message?: string } | undefined)?.detail ??
      (error.response?.data as { message?: string } | undefined)?.message;
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

  const schedulesQuery = useQuery<IngestionSchedule[]>(INGESTION_SCHEDULES_KEY, fetchIngestionSchedules);

  const createMutation = useMutation(createIngestionSchedule, {
    onSuccess: () => {
      toast.showSuccess('Schedule created.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const updateMutation = useMutation(({ id, input }: UpdateArgs) => updateIngestionSchedule(id, input), {
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

  const deleteMutation = useMutation(deleteIngestionSchedule, {
    onSuccess: () => {
      toast.showSuccess('Schedule deleted.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const abortMutation = useMutation(({ scheduleId, runId }: AbortArgs) => abortIngestionRun(scheduleId, runId), {
    onSuccess: () => {
      toast.showSuccess('Ingestion run aborted.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const cleanupMutation = useMutation(cleanupStuckIngestionRuns, {
    onSuccess: (result: CleanupResult) => {
      if (result.expired_runs > 0) {
        toast.showSuccess(result.detail);
      } else {
        toast.showInfo(result.detail);
      }
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  return {
    schedulesQuery,
    createSchedule: createMutation.mutateAsync,
    updateSchedule: updateMutation.mutateAsync,
    triggerSchedule: triggerMutation.mutateAsync,
    deleteSchedule: deleteMutation.mutateAsync,
    abortRun: abortMutation.mutateAsync,
    cleanupStuckRuns: cleanupMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    triggering: triggerMutation.isLoading,
    deleting: deleteMutation.isLoading,
    aborting: abortMutation.isLoading,
    cleaning: cleanupMutation.isLoading
  };
};
