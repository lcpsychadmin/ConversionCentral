import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createSapHanaSettings,
  fetchSapHanaSettings,
  testSapHanaSettings,
  updateSapHanaSettings
} from '../services/sapHanaSettingsService';
import {
  SapHanaSettings,
  SapHanaSettingsTestResult,
  SapHanaSettingsUpdate
} from '../types/data';
import { useToast } from './useToast';

const SETTINGS_KEY = ['sap-hana-settings'];

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

interface UpdateArgs {
  id: string;
  input: SapHanaSettingsUpdate;
}

export const useSapHanaSettings = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const invalidate = () => queryClient.invalidateQueries(SETTINGS_KEY);

  const settingsQuery = useQuery<SapHanaSettings | null>(SETTINGS_KEY, fetchSapHanaSettings);

  const createMutation = useMutation(createSapHanaSettings, {
    onSuccess: () => {
      toast.showSuccess('SAP HANA connection saved.');
      invalidate();
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error))
  });

  const updateMutation = useMutation(({ id, input }: UpdateArgs) => updateSapHanaSettings(id, input), {
    onSuccess: () => {
      toast.showSuccess('SAP HANA connection updated.');
      invalidate();
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error))
  });

  const testMutation = useMutation(testSapHanaSettings, {
    onSuccess: (result: SapHanaSettingsTestResult) => {
      const duration = result.durationMs !== null && result.durationMs !== undefined
        ? ` (${result.durationMs.toFixed(0)} ms)`
        : '';
      toast.showSuccess(`${result.message}${duration}`);
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error))
  });

  return {
    settingsQuery,
    createSettings: createMutation.mutateAsync,
    updateSettings: updateMutation.mutateAsync,
    testSettings: testMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    testing: testMutation.isLoading
  };
};
