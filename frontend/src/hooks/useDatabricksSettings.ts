import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createDatabricksSettings,
  fetchDatabricksSettings,
  fetchDatabricksClusterPolicies,
  syncDatabricksClusterPolicies,
  testDatabricksSettings,
  updateDatabricksSettings,
} from '../services/databricksSettingsService';
import {
  DatabricksClusterPolicy,
  DatabricksSqlSettings,
  DatabricksSqlSettingsTestResult,
  DatabricksSqlSettingsUpdate,
} from '../types/data';
import { useToast } from './useToast';

const SETTINGS_KEY = ['databricks-settings'];
const POLICIES_KEY = [...SETTINGS_KEY, 'policies'];

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

  const invalidate = () => {
    queryClient.invalidateQueries(SETTINGS_KEY);
    queryClient.invalidateQueries(POLICIES_KEY);
  };

  const settingsQuery = useQuery<DatabricksSqlSettings | null>(SETTINGS_KEY, fetchDatabricksSettings);
  const policiesQuery = useQuery<DatabricksClusterPolicy[]>(POLICIES_KEY, fetchDatabricksClusterPolicies);

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

  const syncPoliciesMutation = useMutation(syncDatabricksClusterPolicies, {
    onSuccess: () => {
      toast.showSuccess('Cluster policies refreshed.');
      queryClient.invalidateQueries(POLICIES_KEY);
    },
    onError: (error: unknown) => toast.showError(getErrorMessage(error)),
  });

  return {
    settingsQuery,
    policiesQuery,
    createSettings: createMutation.mutateAsync,
    updateSettings: updateMutation.mutateAsync,
    testSettings: testMutation.mutateAsync,
    syncPolicies: syncPoliciesMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    testing: testMutation.isLoading,
    syncingPolicies: syncPoliciesMutation.isLoading,
  };
};
