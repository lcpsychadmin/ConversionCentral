import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  COMPANY_SETTINGS_QUERY_KEY,
  fetchCompanySettings,
  updateCompanySettings
} from '../services/applicationSettingsService';
import { CompanySettings, CompanySettingsUpdateInput } from '../types/data';
import { useToast } from './useToast';

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

export const useCompanySettings = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const settingsQuery = useQuery<CompanySettings>(
    COMPANY_SETTINGS_QUERY_KEY,
    fetchCompanySettings
  );

  const updateMutation = useMutation(
    (input: CompanySettingsUpdateInput) => updateCompanySettings(input),
    {
      onSuccess: () => {
        toast.showSuccess('Company branding updated.');
        queryClient.invalidateQueries(COMPANY_SETTINGS_QUERY_KEY);
      },
      onError: (error: unknown) => toast.showError(getErrorMessage(error))
    }
  );

  return {
    settingsQuery,
    updateSettings: updateMutation.mutateAsync,
    updating: updateMutation.isLoading
  };
};
