import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createSystem,
  deleteSystem,
  fetchSystems,
  SystemInput,
  updateSystem
} from '../services/systemService';
import { System } from '../types/data';
import { useToast } from './useToast';

const SYSTEMS_KEY = ['systems'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useSystems = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const systemsQuery = useQuery<System[]>(SYSTEMS_KEY, fetchSystems);

  const invalidate = () => queryClient.invalidateQueries(SYSTEMS_KEY);

  const createMutation = useMutation(createSystem, {
    onSuccess: () => {
      toast.showSuccess('System created.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: SystemInput }) => updateSystem(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('System updated.');
        invalidate();
      },
      onError: (error) => toast.showError(getErrorMessage(error))
    }
  );

  const deleteMutation = useMutation(deleteSystem, {
    onSuccess: () => {
      toast.showSuccess('System deleted.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  return {
    systemsQuery,
    createSystem: createMutation.mutateAsync,
    updateSystem: updateMutation.mutateAsync,
    deleteSystem: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
