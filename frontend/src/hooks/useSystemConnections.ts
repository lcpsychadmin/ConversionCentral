import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import {
  createSystemConnection,
  deleteSystemConnection,
  fetchSystemConnections,
  SystemConnectionTestPayload,
  SystemConnectionTestResult,
  testSystemConnection,
  updateSystemConnection
} from '../services/systemConnectionService';
import {
  SystemConnection,
  SystemConnectionInput,
  SystemConnectionUpdateInput
} from '../types/data';
import { useToast } from './useToast';

const SYSTEM_CONNECTIONS_KEY = ['system-connections'];

type UpdateArgs = {
  id: string;
  input: SystemConnectionUpdateInput;
};

type ErrorWithSummary = Error & { connectionSummary?: string };

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

export const useSystemConnections = () => {
  const toast = useToast();
  const queryClient = useQueryClient();

  const invalidate = () => queryClient.invalidateQueries(SYSTEM_CONNECTIONS_KEY);

  const connectionsQuery = useQuery<SystemConnection[]>(
    SYSTEM_CONNECTIONS_KEY,
    fetchSystemConnections
  );

  const createMutation = useMutation(createSystemConnection, {
    onSuccess: () => {
      toast.showSuccess('Connection created.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const updateMutation = useMutation(
    ({ id, input }: UpdateArgs) => updateSystemConnection(id, input),
    {
      onSuccess: () => {
        toast.showSuccess('Connection updated.');
        invalidate();
      },
      onError: (error) => toast.showError(getErrorMessage(error))
    }
  );

  const deleteMutation = useMutation(deleteSystemConnection, {
    onSuccess: () => {
      toast.showSuccess('Connection deleted.');
      invalidate();
    },
    onError: (error) => toast.showError(getErrorMessage(error))
  });

  const testMutation = useMutation(
    (payload: SystemConnectionTestPayload) => testSystemConnection(payload),
    {
      onSuccess: (result: SystemConnectionTestResult) => {
        const durationMessage =
          result.durationMs !== undefined ? ` in ${result.durationMs.toFixed(0)} ms` : '';
        const context = result.connectionSummary ?? 'Connection';
        toast.showSuccess(`${context} succeeded${durationMessage}.`);
      },
      onError: (error: unknown) => {
        const message = getErrorMessage(error);
        const summary = (error as ErrorWithSummary).connectionSummary;
        toast.showError(summary ? `${summary}: ${message}` : message);
      }
    }
  );

  return {
    connectionsQuery,
    createConnection: createMutation.mutateAsync,
    updateConnection: updateMutation.mutateAsync,
    deleteConnection: deleteMutation.mutateAsync,
    testConnection: testMutation.mutateAsync,
    creating: createMutation.isLoading,
    updating: updateMutation.isLoading,
    deleting: deleteMutation.isLoading,
    testing: testMutation.isLoading
  };
};
