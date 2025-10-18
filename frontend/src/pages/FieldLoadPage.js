import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useQuery } from 'react-query';
import { Box, Paper, Typography } from '@mui/material';
import { fetchFieldLoads } from '../services/fieldLoadService';
import FieldLoadTable from '../components/field-load/FieldLoadTable';
const FieldLoadPage = () => {
    const { data, isLoading } = useQuery(['field-loads'], fetchFieldLoads);
    return (_jsxs(Box, { children: [_jsx(Typography, { variant: "h4", gutterBottom: true, children: "Field Load Management" }), _jsx(Paper, { elevation: 1, sx: { p: 3 }, children: _jsx(FieldLoadTable, { data: data ?? [], loading: isLoading }) })] }));
};
export default FieldLoadPage;
