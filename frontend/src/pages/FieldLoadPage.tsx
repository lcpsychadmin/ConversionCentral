import { useQuery } from 'react-query';
import { Box, Paper, Typography } from '@mui/material';

import { fetchFieldLoads } from '../services/fieldLoadService';
import FieldLoadTable from '../components/field-load/FieldLoadTable';
import { FieldLoad } from '../types/data';

const FieldLoadPage = () => {
  const { data, isLoading } = useQuery<FieldLoad[]>(['field-loads'], fetchFieldLoads);

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Field Load Management
      </Typography>
      <Paper elevation={1} sx={{ p: 3 }}>
        <FieldLoadTable data={data ?? []} loading={isLoading} />
      </Paper>
    </Box>
  );
};

export default FieldLoadPage;
