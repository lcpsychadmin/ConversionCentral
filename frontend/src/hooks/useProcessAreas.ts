import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createProcessArea,
  deleteProcessArea,
  fetchProcessAreas,
  ProcessAreaInput,
  updateProcessArea
} from '../services/processAreaService';
import { ProcessArea } from '../types/data';
import { useToast } from './useToast';

const PROCESS_AREAS_KEY = ['process-areas'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useProcessAreas = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const processAreasQuery = useQuery<ProcessArea[]>(PROCESS_AREAS_KEY, fetchProcessAreas);

  const invalidate = () => queryClient.invalidateQueries(PROCESS_AREAS_KEY);

  const createMutation = useMutation(createProcessArea, {
    onSuccess: () => {
      toast.showSuccess('Process area created.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: Partial<ProcessAreaInput> }) => updateProcessArea(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('Process area updated.');
        invalidate();
      },
      onError: (error) => {
        toast.showError(getErrorMessage(error));
      }
    }
  );

  const deleteMutation = useMutation(deleteProcessArea, {
    onSuccess: () => {
      toast.showSuccess('Process area deleted.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  return {
    processAreasQuery,
    createProcessArea: createMutation.mutateAsync,
    updateProcessArea: updateMutation.mutateAsync,
    deleteProcessArea: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
