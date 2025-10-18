import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createRelease,
  deleteRelease,
  fetchReleases,
  updateRelease
} from '../services/releaseService';
import { Release, ReleaseInput } from '../types/data';
import { useToast } from './useToast';

const RELEASES_KEY = ['releases'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useReleases = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const releasesQuery = useQuery<Release[]>(RELEASES_KEY, fetchReleases);

  const invalidate = () => queryClient.invalidateQueries(RELEASES_KEY);

  const createMutation = useMutation(createRelease, {
    onSuccess: () => {
      toast.showSuccess('Release created.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: Partial<ReleaseInput> }) => updateRelease(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('Release updated.');
        invalidate();
      },
      onError: (error) => {
        toast.showError(getErrorMessage(error));
      }
    }
  );

  const deleteMutation = useMutation(deleteRelease, {
    onSuccess: () => {
      toast.showSuccess('Release deleted.');
      invalidate();
    },
    onError: (error) => {
      toast.showError(getErrorMessage(error));
    }
  });

  return {
    releasesQuery,
    createRelease: createMutation.mutateAsync,
    updateRelease: updateMutation.mutateAsync,
    deleteRelease: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
