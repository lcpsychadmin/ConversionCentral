import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';
import { createDataDefinition, deleteDataDefinition, fetchDataDefinition, updateDataDefinition } from '../services/dataDefinitionService';
import { createDataDefinitionRelationship, deleteDataDefinitionRelationship, updateDataDefinitionRelationship } from '../services/dataDefinitionRelationshipService';
import { useToast } from './useToast';
const getErrorMessage = (error) => {
    if (error instanceof AxiosError) {
        return error.response?.data?.detail ?? error.message;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return 'An unexpected error occurred.';
};
const definitionKey = (dataObjectId, systemId) => [
    'data-definition',
    dataObjectId,
    systemId
];
export const useDataDefinition = (dataObjectId, systemId) => {
    const toast = useToast();
    const queryClient = useQueryClient();
    const enabled = Boolean(dataObjectId && systemId);
    const key = dataObjectId && systemId ? definitionKey(dataObjectId, systemId) : ['data-definition'];
    const definitionQuery = useQuery(key, () => fetchDataDefinition(dataObjectId, systemId), {
        enabled,
        keepPreviousData: true
    });
    const invalidate = () => {
        if (dataObjectId && systemId) {
            queryClient.invalidateQueries(definitionKey(dataObjectId, systemId));
        }
    };
    const createMutation = useMutation(createDataDefinition, {
        onSuccess: () => {
            toast.showSuccess('Data definition created.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const updateMutation = useMutation(({ id, input }) => updateDataDefinition(id, input), {
        onSuccess: () => {
            toast.showSuccess('Data definition updated.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const deleteMutation = useMutation(deleteDataDefinition, {
        onSuccess: () => {
            toast.showSuccess('Data definition deleted.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const createRelationshipMutation = useMutation(({ definitionId, input }) => createDataDefinitionRelationship(definitionId, input), {
        onSuccess: () => {
            toast.showSuccess('Relationship created.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const updateRelationshipMutation = useMutation(({ definitionId, relationshipId, input }) => updateDataDefinitionRelationship(definitionId, relationshipId, input), {
        onSuccess: () => {
            toast.showSuccess('Relationship updated.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const deleteRelationshipMutation = useMutation(({ definitionId, relationshipId }) => deleteDataDefinitionRelationship(definitionId, relationshipId), {
        onSuccess: () => {
            toast.showSuccess('Relationship deleted.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    return {
        definitionQuery,
        createDataDefinition: createMutation.mutateAsync,
        updateDataDefinition: updateMutation.mutateAsync,
        deleteDataDefinition: deleteMutation.mutateAsync,
        creating: createMutation.isLoading,
        updating: updateMutation.isLoading,
        deleting: deleteMutation.isLoading,
        createRelationship: createRelationshipMutation.mutateAsync,
        updateRelationship: updateRelationshipMutation.mutateAsync,
        deleteRelationship: deleteRelationshipMutation.mutateAsync,
        relationshipCreating: createRelationshipMutation.isLoading,
        relationshipUpdating: updateRelationshipMutation.isLoading,
        relationshipDeleting: deleteRelationshipMutation.isLoading
    };
};
