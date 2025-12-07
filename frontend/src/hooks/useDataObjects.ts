import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createDataObject,
  DataObjectInput,
  deleteDataObject,
  fetchDataObjects,
  updateDataObject
} from '../services/dataObjectService';
import { DataObject } from '../types/data';
import { useToast } from './useToast';
import { useWorkspaceScope } from './useWorkspaceScope';

const DATA_OBJECTS_KEY = ['data-objects'];

const getErrorMessage = (error: unknown) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred.';
};

export const useDataObjects = () => {
  const toast = useToast();
  const queryClient = useQueryClient();
  const { workspaceId } = useWorkspaceScope();

  const queryKey = workspaceId ? [...DATA_OBJECTS_KEY, workspaceId] : DATA_OBJECTS_KEY;

  const dataObjectsQuery = useQuery<DataObject[]>(
    queryKey,
    () => fetchDataObjects({ workspaceId }),
    {
      enabled: Boolean(workspaceId),
      keepPreviousData: true
    }
  );

  const invalidate = () => queryClient.invalidateQueries(DATA_OBJECTS_KEY);

  const ensureWorkspace = () => {
    if (!workspaceId) {
      throw new Error('Select an active workspace before managing data objects.');
    }
    return workspaceId;
  };

  const createMutation = useMutation(
    (input: DataObjectInput) =>
      createDataObject({ ...input, workspaceId: input.workspaceId ?? ensureWorkspace() }),
    {
      onSuccess: () => {
        toast.showSuccess('Data object created.');
        invalidate();
      },
      onError: (error) => toast.showError(getErrorMessage(error))
    }
  );

  const updateMutation = useMutation(
    ({ id, input }: { id: string; input: DataObjectInput }) =>
      updateDataObject(id, {
        ...input,
        workspaceId: input.workspaceId ?? workspaceId ?? undefined
      }),
    {
      onSuccess: () => {
        toast.showSuccess('Data object updated.');
        invalidate();
      },
      onError: (error) => toast.showError(getErrorMessage(error))
    }
  );

  const deleteMutation = useMutation(deleteDataObject, {
    onSuccess: () => {
      toast.showSuccess('Data object deleted.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  return {
    dataObjectsQuery,
    createDataObject: createMutation.mutateAsync,
    updateDataObject: updateMutation.mutateAsync,
    deleteDataObject: deleteMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading
  };
};
