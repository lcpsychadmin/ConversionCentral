import { Box, Paper, Stack, Typography } from '@mui/material';

const ReportingCatalogPage = () => {
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
      <Box>
        <Typography variant="h4" sx={{ fontWeight: 600, mb: 1 }}>
          Reports & Outputs
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Browse published reporting assets, inspect run history, and access generated outputs. This directory will evolve alongside the Reporting Designer to provide governance and operational controls.
        </Typography>
      </Box>

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
