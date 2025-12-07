import { useMemo } from 'react';

import { useWorkspaces } from './useWorkspaces';
import type { Workspace } from '@cc-types/data';

interface WorkspaceScope {
  workspaceId: string | null;
  workspace: Workspace | null;
  workspaces: Workspace[];
}

export const useWorkspaceScope = (): ReturnType<typeof useWorkspaces> & WorkspaceScope => {
  const workspacesQuery = useWorkspaces();

  const workspace = useMemo(() => {
    const workspaces = workspacesQuery.data ?? [];
    if (!workspaces.length) {
      return null;
    }
    const preferred = workspaces.find((entry) => entry.isDefault && entry.isActive);
    if (preferred) {
      return preferred;
    }
    const active = workspaces.find((entry) => entry.isActive);
    return active ?? workspaces[0];
  }, [workspacesQuery.data]);

  return {
    ...workspacesQuery,
    workspaceId: workspace?.id ?? null,
    workspace,
    workspaces: workspacesQuery.data ?? [],
  };
};

