import { useQuery } from 'react-query';
import { Alert, Grid, Paper, Typography } from '@mui/material';

import { fetchDashboardSummary } from '../services/dashboardService';
import { ProjectSummary } from '../types/data';

const OverviewPage = () => {
  const { data, isLoading, isError } = useQuery<ProjectSummary>(['dashboard-summary'], fetchDashboardSummary);

  if (isError) {
    return (
      <Alert severity="error">
        Unable to reach the dashboard API. Make sure the backend server is running on
        {' '}
        {import.meta.env.VITE_API_URL ?? 'http://localhost:8000'}.
      </Alert>
    );
  }

  return (
    <Grid container spacing={3}>
      <Grid item xs={12} md={6} lg={3}>
        <Paper elevation={2} sx={{ p: 3 }}>
          <Typography variant="h6">Projects</Typography>
          <Typography variant="h4">{isLoading ? '...' : data?.projects ?? 0}</Typography>
        </Paper>
      </Grid>
      <Grid item xs={12} md={6} lg={3}>
        <Paper elevation={2} sx={{ p: 3 }}>
          <Typography variant="h6">Releases</Typography>
          <Typography variant="h4">{isLoading ? '...' : data?.releases ?? 0}</Typography>
        </Paper>
      </Grid>
      <Grid item xs={12} md={6} lg={3}>
        <Paper elevation={2} sx={{ p: 3 }}>
          <Typography variant="h6">Validation Issues</Typography>
          <Typography variant="h4">{isLoading ? '...' : data?.validationIssues ?? 0}</Typography>
        </Paper>
      </Grid>
      <Grid item xs={12} md={6} lg={3}>
        <Paper elevation={2} sx={{ p: 3 }}>
          <Typography variant="h6">Pending Approvals</Typography>
          <Typography variant="h4">{isLoading ? '...' : data?.pendingApprovals ?? 0}</Typography>
        </Paper>
      </Grid>
    </Grid>
  );
};

export default OverviewPage;
