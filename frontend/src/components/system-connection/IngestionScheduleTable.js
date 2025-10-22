import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo } from 'react';
import { DataGrid, GridActionsCellItem } from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import { Box, Chip, Stack, Switch, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import { getDataGridStyles } from '../../utils/tableStyles';
const formatDateTime = (iso) => {
    if (!iso)
        return '—';
    const date = new Date(iso);
    if (Number.isNaN(date.getTime()))
        return '—';
    return date.toLocaleString();
};
const formatStrategy = (strategy) => {
    switch (strategy) {
        case 'timestamp':
            return 'Timestamp';
        case 'numeric_key':
            return 'Numeric key';
        case 'full':
            return 'Full';
        default:
            return strategy;
    }
};
const IngestionScheduleTable = ({ schedules, selectionLookup, loading = false, busyIds, onEdit, onRun, onToggleActive }) => {
    const theme = useTheme();
    const rows = useMemo(() => schedules.map((schedule) => {
        const selection = selectionLookup.get(schedule.connectionTableSelectionId);
        return {
            id: schedule.id,
            schedule,
            selection,
            tableLabel: selection?.targetPreview ?? schedule.targetTableName ?? '—',
            cron: schedule.scheduleExpression,
            timezone: schedule.timezone ?? 'UTC',
            loadStrategy: formatStrategy(schedule.loadStrategy),
            targetTable: schedule.targetTableName ?? selection?.targetPreview ?? '—',
            isActive: schedule.isActive,
            lastStatus: schedule.lastRunStatus ?? '—',
            lastCompleted: schedule.lastRunCompletedAt ?? schedule.lastRunStartedAt ?? null,
            totalRuns: schedule.totalRuns,
            totalRows: schedule.totalRowsLoaded,
            lastError: schedule.lastRunError ?? null,
            rawSchedule: schedule
        };
    }), [schedules, selectionLookup]);
    const columns = useMemo(() => [
        {
            field: 'tableLabel',
            headerName: 'Source Table',
            flex: 1.2,
            renderCell: ({ row }) => (_jsxs(Stack, { children: [_jsx(Typography, { variant: "body2", children: row.tableLabel }), row.selection && (_jsx(Typography, { variant: "caption", color: "text.secondary", children: `${row.selection.schemaName}.${row.selection.tableName}` }))] }))
        },
        {
            field: 'cron',
            headerName: 'Cron',
            flex: 0.8
        },
        {
            field: 'timezone',
            headerName: 'Timezone',
            flex: 0.6
        },
        {
            field: 'loadStrategy',
            headerName: 'Strategy',
            flex: 0.7
        },
        {
            field: 'targetTable',
            headerName: 'Target Table',
            flex: 1
        },
        {
            field: 'isActive',
            headerName: 'Active',
            width: 120,
            renderCell: ({ row }) => (_jsx(Switch, { size: "small", checked: row.isActive, disabled: busyIds?.has(row.id), onChange: (_event, checked) => onToggleActive?.(row.rawSchedule, checked) }))
        },
        {
            field: 'lastStatus',
            headerName: 'Last Run',
            flex: 0.8,
            renderCell: ({ row }) => (_jsxs(Stack, { spacing: 0.5, children: [_jsx(Chip, { label: row.lastStatus ?? '—', size: "small", color: row.lastStatus === 'completed'
                            ? 'success'
                            : row.lastStatus === 'failed'
                                ? 'error'
                                : 'default' }), _jsx(Typography, { variant: "caption", color: "text.secondary", children: formatDateTime(row.lastCompleted) })] }))
        },
        {
            field: 'totalRuns',
            headerName: 'Runs',
            width: 100
        },
        {
            field: 'totalRows',
            headerName: 'Rows Loaded',
            width: 140,
            valueFormatter: ({ value }) => (value ? value.toLocaleString() : '—')
        },
        {
            field: 'actions',
            type: 'actions',
            headerName: 'Actions',
            getActions: ({ row }) => {
                const disabled = busyIds?.has(row.id);
                return [
                    _jsx(GridActionsCellItem, { icon: _jsx(EditIcon, { fontSize: "small" }), label: "Edit", disabled: disabled, onClick: () => onEdit?.(row.rawSchedule), showInMenu: true }, "edit"),
                    _jsx(GridActionsCellItem, { icon: _jsx(PlayCircleIcon, { fontSize: "small" }), label: "Run now", disabled: disabled || !row.isActive, onClick: () => onRun?.(row.rawSchedule), showInMenu: true }, "run")
                ];
            }
        }
    ], [onEdit, onRun, onToggleActive, busyIds]);
    return (_jsx(Box, { sx: { height: 420, width: '100%' }, children: _jsx(DataGrid, { rows: rows, columns: columns, loading: loading, disableRowSelectionOnClick: true, sx: {
                ...getDataGridStyles(theme),
                '& .MuiDataGrid-cell': { alignItems: 'center' },
                '& .MuiDataGrid-row': { cursor: 'default' }
            }, getRowId: (row) => row.id }) }));
};
export default IngestionScheduleTable;
