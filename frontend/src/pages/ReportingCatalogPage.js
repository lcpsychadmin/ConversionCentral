import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Box, Paper, Stack, Typography } from '@mui/material';
const ReportingCatalogPage = () => {
    return (_jsxs(Box, { sx: { display: 'flex', flexDirection: 'column', gap: 3 }, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "h4", sx: { fontWeight: 600, mb: 1 }, children: "Reports & Outputs" }), _jsx(Typography, { variant: "body1", color: "text.secondary", children: "Browse published reporting assets, inspect run history, and access generated outputs. This directory will evolve alongside the Reporting Designer to provide governance and operational controls." })] }), _jsx(Paper, { variant: "outlined", sx: { p: 3, minHeight: 280, display: 'flex', alignItems: 'center', justifyContent: 'center', textAlign: 'center' }, children: _jsxs(Stack, { spacing: 1, alignItems: "center", children: [_jsx(Typography, { variant: "h6", color: "text.secondary", children: "Report library coming soon" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Once publishing is available, saved reports and their latest outputs will appear here." })] }) })] }));
};
export default ReportingCatalogPage;
