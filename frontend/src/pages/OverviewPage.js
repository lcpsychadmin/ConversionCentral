import { jsxs as _jsxs, jsx as _jsx } from "react/jsx-runtime";
import { useQuery } from 'react-query';
import { Alert, Grid, Paper, Typography } from '@mui/material';
import { fetchDashboardSummary } from '../services/dashboardService';
const OverviewPage = () => {
    const { data, isLoading, isError } = useQuery(['dashboard-summary'], fetchDashboardSummary);
    if (isError) {
        return (_jsxs(Alert, { severity: "error", children: ["Unable to reach the dashboard API. Make sure the backend server is running on", ' ', import.meta.env.VITE_API_URL ?? 'http://localhost:8000', "."] }));
    }
    return (_jsxs(Grid, { container: true, spacing: 3, children: [_jsx(Grid, { item: true, xs: 12, md: 6, lg: 3, children: _jsxs(Paper, { elevation: 2, sx: { p: 3 }, children: [_jsx(Typography, { variant: "h6", children: "Projects" }), _jsx(Typography, { variant: "h4", children: isLoading ? '...' : data?.projects ?? 0 })] }) }), _jsx(Grid, { item: true, xs: 12, md: 6, lg: 3, children: _jsxs(Paper, { elevation: 2, sx: { p: 3 }, children: [_jsx(Typography, { variant: "h6", children: "Releases" }), _jsx(Typography, { variant: "h4", children: isLoading ? '...' : data?.releases ?? 0 })] }) }), _jsx(Grid, { item: true, xs: 12, md: 6, lg: 3, children: _jsxs(Paper, { elevation: 2, sx: { p: 3 }, children: [_jsx(Typography, { variant: "h6", children: "Validation Issues" }), _jsx(Typography, { variant: "h4", children: isLoading ? '...' : data?.validationIssues ?? 0 })] }) }), _jsx(Grid, { item: true, xs: 12, md: 6, lg: 3, children: _jsxs(Paper, { elevation: 2, sx: { p: 3 }, children: [_jsx(Typography, { variant: "h6", children: "Pending Approvals" }), _jsx(Typography, { variant: "h4", children: isLoading ? '...' : data?.pendingApprovals ?? 0 })] }) })] }));
};
export default OverviewPage;
