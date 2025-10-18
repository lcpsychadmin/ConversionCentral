import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';
import { createDataObject, deleteDataObject, fetchDataObjects, updateDataObject } from '../services/dataObjectService';
import { useToast } from './useToast';
const DATA_OBJECTS_KEY = ['data-objects'];
const getErrorMessage = (error) => {
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
    const dataObjectsQuery = useQuery(DATA_OBJECTS_KEY, fetchDataObjects);
    const invalidate = () => queryClient.invalidateQueries(DATA_OBJECTS_KEY);
    const createMutation = useMutation(createDataObject, {
        onSuccess: () => {
            toast.showSuccess('Data object created.');
            invalidate();
        },
        onError: (error) => toast.showError(getErrorMessage(error))
    });
    const updateMutation = useMutation(({ id, input }) => updateDataObject(id, input), {
        onSuccess: () => {
            toast.showSuccess('Data object updated.');
            invalidate();
        },
        onError: (error) => toast.showError(getErrorMessage(error))
    });
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
