import { Box, Paper, Stack, Typography } from '@mui/material';
import PageHeader from '../components/common/PageHeader';

const ReportingCatalogPage = () => {
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
      <PageHeader
        title="Reports & Outputs"
        subtitle="Browse published reporting assets, inspect run history, and access generated outputs. This directory will evolve alongside the Reporting Designer to provide governance and operational controls."
      />

      <Paper variant="outlined" sx={{ p: 3, minHeight: 280, display: 'flex', alignItems: 'center', justifyContent: 'center', textAlign: 'center' }}>
        <Stack spacing={1} alignItems="center">
          <Typography variant="h6" color="text.secondary">
            Report library coming soon
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Once publishing is available, saved reports and their latest outputs will appear here.
          </Typography>
        </Stack>
      </Paper>
    </Box>
  );
};

export default ReportingCatalogPage;
