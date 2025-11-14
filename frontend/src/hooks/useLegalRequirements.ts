import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createLegalRequirement,
  deleteLegalRequirement,
  fetchLegalRequirements,
  LegalRequirementUpdateInput,
  updateLegalRequirement
} from '../services/legalRequirementService';
import { LegalRequirement } from '../types/data';
import { useToast } from './useToast';

const LEGAL_REQUIREMENTS_KEY = ['legal-requirements'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useLegalRequirements = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const legalRequirementsQuery = useQuery<LegalRequirement[]>(
    LEGAL_REQUIREMENTS_KEY,
    fetchLegalRequirements
  );

  const invalidate = () => queryClient.invalidateQueries(LEGAL_REQUIREMENTS_KEY);

  const createMutation = useMutation(createLegalRequirement, {
    onSuccess: () => {
      toast.showSuccess('Requirement created.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: LegalRequirementUpdateInput }) =>
      updateLegalRequirement(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('Requirement updated.');
        invalidate();
      },
      onError: (error) => {
        toast.showError(getErrorMessage(error));
      }
    }
  );

  const deleteMutation = useMutation(deleteLegalRequirement, {
    onSuccess: () => {
      toast.showSuccess('Requirement deleted.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  return {
    legalRequirementsQuery,
    createLegalRequirement: createMutation.mutateAsync,
    updateLegalRequirement: updateMutation.mutateAsync,
    deleteLegalRequirement: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
