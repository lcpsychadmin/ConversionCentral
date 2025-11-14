import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createSecurityClassification,
  deleteSecurityClassification,
  fetchSecurityClassifications,
  SecurityClassificationUpdateInput,
  updateSecurityClassification
} from '../services/securityClassificationService';
import { SecurityClassification } from '../types/data';
import { useToast } from './useToast';

const SECURITY_CLASSIFICATIONS_KEY = ['security-classifications'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useSecurityClassifications = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const classificationsQuery = useQuery<SecurityClassification[]>(
    SECURITY_CLASSIFICATIONS_KEY,
    fetchSecurityClassifications
  );

  const invalidate = () => queryClient.invalidateQueries(SECURITY_CLASSIFICATIONS_KEY);

  const createMutation = useMutation(createSecurityClassification, {
    onSuccess: () => {
      toast.showSuccess('Security classification created.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: SecurityClassificationUpdateInput }) =>
      updateSecurityClassification(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('Security classification updated.');
        invalidate();
      },
      onError: (error) => {
        toast.showError(getErrorMessage(error));
      }
    }
  );

  const deleteMutation = useMutation(deleteSecurityClassification, {
    onSuccess: () => {
      toast.showSuccess('Security classification deleted.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  return {
    securityClassificationsQuery: classificationsQuery,
    createSecurityClassification: createMutation.mutateAsync,
    updateSecurityClassification: updateMutation.mutateAsync,
    deleteSecurityClassification: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
