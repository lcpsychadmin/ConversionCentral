import { Box, Button, CircularProgress, Typography } from '@mui/material';
import { ReactNode } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useQuery } from 'react-query';

import {
  APPLICATION_DATABASE_STATUS_QUERY_KEY,
  fetchApplicationDatabaseStatus
} from '../../services/applicationDatabaseService';

interface ApplicationDatabaseGuardProps {
  children: ReactNode;
}

export const ApplicationDatabaseGuard = ({ children }: ApplicationDatabaseGuardProps) => {
  const location = useLocation();
  const setupPath = '/application-settings/application-database';

  const statusQuery = useQuery(APPLICATION_DATABASE_STATUS_QUERY_KEY, fetchApplicationDatabaseStatus, {
    staleTime: 30_000
  });

  if (statusQuery.isLoading) {
    return (
      <Box display="flex" alignItems="center" justifyContent="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  if (statusQuery.isError) {
    const message = statusQuery.error instanceof Error
      ? statusQuery.error.message
      : 'Unable to load application database status.';

    return (
      <Box display="flex" flexDirection="column" alignItems="center" justifyContent="center" height="60vh" gap={2}>
        <Typography variant="h6" color="error">
          {message}
        </Typography>
        <Button variant="contained" onClick={() => statusQuery.refetch()}>
          Retry
        </Button>
      </Box>
    );
  }

  const status = statusQuery.data;
  const needsDatabase = !status?.configured;
  const needsAdminEmail = !status?.adminEmail;

  if ((needsDatabase || needsAdminEmail) && location.pathname !== setupPath) {
    return <Navigate to={setupPath} replace />;
  }

  return <>{children}</>;
};
