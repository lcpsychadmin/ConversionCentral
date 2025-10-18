import { useMutation, useQuery, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';
import { createProject, deleteProject, fetchProjects, updateProject } from '../services/projectService';
import { useToast } from './useToast';
const PROJECTS_KEY = ['projects'];
export const useProjects = () => {
    const toast = useToast();
    const queryClient = useQueryClient();
    const projectsQuery = useQuery(PROJECTS_KEY, fetchProjects, {
        staleTime: 5 * 60 * 1000
    });
    const invalidate = () => queryClient.invalidateQueries(PROJECTS_KEY);
    const getErrorMessage = (error) => {
        if (error instanceof AxiosError) {
            return error.response?.data?.detail ?? error.message;
        }
        if (error instanceof Error) {
            return error.message;
        }
        return 'An unexpected error occurred.';
    };
    const createMutation = useMutation(createProject, {
        onSuccess: () => {
            toast.showSuccess('Project created.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const updateMutation = useMutation(({ id, input }) => updateProject(id, input), {
        onSuccess: () => {
            toast.showSuccess('Project updated.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    const deleteMutation = useMutation(deleteProject, {
        onSuccess: () => {
            toast.showSuccess('Project deleted.');
            invalidate();
        },
        onError: (error) => {
            toast.showError(getErrorMessage(error));
        }
    });
    return {
        projectsQuery,
        createProject: createMutation.mutateAsync,
        updateProject: updateMutation.mutateAsync,
        deleteProject: deleteMutation.mutateAsync,
        creating: createMutation.isLoading,
        updating: updateMutation.isLoading,
        deleting: deleteMutation.isLoading
    };
};
