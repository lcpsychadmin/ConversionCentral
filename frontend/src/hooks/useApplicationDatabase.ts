import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  APPLICATION_DATABASE_STATUS_QUERY_KEY,
  applyApplicationDatabaseSetting,
  fetchApplicationDatabaseStatus,
  testApplicationDatabase
} from '../services/applicationDatabaseService';
import {
  ADMIN_EMAIL_QUERY_KEY,
  updateAdminEmailSetting
} from '../services/applicationSettingsService';
import {
  ApplicationDatabaseApplyInput,
  ApplicationDatabaseStatus,
  ApplicationDatabaseTestResult
} from '../types/data';
import { useToast } from './useToast';

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    const data = error.response?.data as { detail?: string; message?: string } | undefined;
    return data?.detail ?? data?.message ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useApplicationDatabase = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const statusQuery = useQuery<ApplicationDatabaseStatus>(
    APPLICATION_DATABASE_STATUS_QUERY_KEY,
    fetchApplicationDatabaseStatus
  );

  const testMutation = useMutation(testApplicationDatabase, {
    onSuccess: (result: ApplicationDatabaseTestResult) => {
      if (result.success) {
        const latency =
          result.latencyMs !== undefined && result.latencyMs !== null
            ? ` (${result.latencyMs.toFixed(0)} ms)`
            : '';
        toast.showSuccess(`${result.message}${latency}`);
      } else {
        toast.showError(result.message);
      }
    },
    onError: (error: unknown) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const applyMutation = useMutation(applyApplicationDatabaseSetting, {
    onSuccess: () => {
      toast.showSuccess('Application database configuration saved.');
      queryClient.invalidateQueries(APPLICATION_DATABASE_STATUS_QUERY_KEY);
    },
    onError: (error: unknown) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const adminEmailMutation = useMutation(updateAdminEmailSetting, {
    onSuccess: () => {
      toast.showSuccess('Admin contact saved.');
      queryClient.invalidateQueries(APPLICATION_DATABASE_STATUS_QUERY_KEY);
      queryClient.invalidateQueries(ADMIN_EMAIL_QUERY_KEY);
    },
    onError: (error: unknown) => {
      toast.showError(getErrorMessage(error));
    }
  });

  return {
    statusQuery,
    testConnection: (input: ApplicationDatabaseApplyInput) => testMutation.mutateAsync(input),
    applySetting: (input: ApplicationDatabaseApplyInput) => applyMutation.mutateAsync(input),
    setAdminEmail: (email: string) => adminEmailMutation.mutateAsync(email),
    testing: testMutation.isLoading,
    applying: applyMutation.isLoading,
    settingAdminEmail: adminEmailMutation.isLoading,
    lastTestResult: testMutation.data ?? null
  };
};
