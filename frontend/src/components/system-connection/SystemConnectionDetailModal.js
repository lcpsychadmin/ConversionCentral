import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, Box, Grid, Stack, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import { formatConnectionSummary, parseJdbcConnectionString } from '../../utils/connectionString';
const DetailLine = ({ label, value }) => (_jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { color: 'text.secondary', fontWeight: 600, mb: 0.5 }, children: label }), _jsx(Typography, { variant: "body2", children: value })] }));
const SystemConnectionDetailModal = ({ open, connection, system, onClose }) => {
    if (!connection) {
        return null;
    }
    const theme = useTheme();
    const parsed = parseJdbcConnectionString(connection.connectionString);
    return (_jsxs(Dialog, { open: open, onClose: onClose, fullWidth: true, maxWidth: "sm", children: [_jsx(DialogTitle, { sx: { fontWeight: 700, color: theme.palette.primary.dark }, children: "Connection Details" }), _jsx(DialogContent, { dividers: true, children: _jsx(Stack, { spacing: 3, sx: { mt: 1 }, children: _jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 12, sm: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "System", value: system?.name ?? '—' }), _jsx(DetailLine, { label: "Endpoint", value: formatConnectionSummary(connection.connectionString) }), _jsx(DetailLine, { label: "Database", value: parsed ? parsed.database : '—' }), _jsx(DetailLine, { label: "Host", value: parsed ? `${parsed.host}${parsed.port ? `:${parsed.port}` : ''}` : '—' })] }) }), _jsx(Grid, { item: true, xs: 12, sm: 6, children: _jsxs(Stack, { spacing: 2, children: [_jsx(DetailLine, { label: "Username", value: parsed?.username ? parsed.username : '—' }), _jsx(DetailLine, { label: "Status", value: connection.active ? 'Active' : 'Disabled' }), _jsx(DetailLine, { label: "Ingestion", value: connection.ingestionEnabled ? 'Enabled' : 'Disabled' }), _jsx(DetailLine, { label: "Notes", value: connection.notes ?? '—' }), _jsx(DetailLine, { label: "Last Updated", value: connection.updatedAt ? new Date(connection.updatedAt).toLocaleString() : '—' })] }) })] }) }) }), _jsx(DialogActions, { children: _jsx(Button, { onClick: onClose, variant: "contained", children: "Close" }) })] }));
};
export default SystemConnectionDetailModal;
