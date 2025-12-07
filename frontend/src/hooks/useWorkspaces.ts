import { useQuery } from 'react-query';

import { fetchWorkspaces } from '@services/workspaceService';
import type { Workspace } from '@cc-types/data';

export const WORKSPACES_QUERY_KEY = ['workspaces'] as const;

export const useWorkspaces = () =>
  useQuery<Workspace[]>(WORKSPACES_QUERY_KEY, fetchWorkspaces, {
    staleTime: 5 * 60 * 1000
  });
