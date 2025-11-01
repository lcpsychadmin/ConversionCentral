import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createDatabricksSettings,
  fetchDatabricksSettings,
  testDatabricksSettings,
  updateDatabricksSettings
} from '../services/databricksSettingsService';
import {
  DatabricksSqlSettings,
  DatabricksSqlSettingsTestResult,
  DatabricksSqlSettingsUpdate
} from '../types/data';
import { useToast } from './useToast';

const SETTINGS_KEY = ['databricks-settings'];

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
  input: DatabricksSqlSettingsUpdate;
}

export const useDatabricksSettings = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const invalidate = () => queryClient.invalidateQueries(SETTINGS_KEY);

  const settingsQuery = useQuery<DatabricksSqlSettings | null>(SETTINGS_KEY, fetchDatabricksSettings);

  const createMutation = useMutation(createDatabricksSettings, {
    onSuccess: () => {
      toast.showSuccess('Databricks connection saved.');
      invalidate();
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error))
  });

  const updateMutation = useMutation(({ id, input }: UpdateArgs) => updateDatabricksSettings(id, input), {
    onSuccess: () => {
      toast.showSuccess('Databricks connection updated.');
      invalidate();
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error))
  });

  const testMutation = useMutation(testDatabricksSettings, {
    onSuccess: (result: DatabricksSqlSettingsTestResult) => {
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
