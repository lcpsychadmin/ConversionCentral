import { jsx as _jsx } from "react/jsx-runtime";
import { useMemo } from 'react';
import { DataGrid, GridActionsCellItem } from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import FlashOnIcon from '@mui/icons-material/FlashOn';
import { Chip, Tooltip } from '@mui/material';
import { formatConnectionSummary, parseJdbcConnectionString } from '../../utils/connectionString';
const DATABASE_LABELS = {
    postgresql: 'PostgreSQL'
};
const buildColumns = (systemLookup, canManage, onEdit, onDelete, onTest) => {
    const baseColumns = [
        {
            field: 'systemId',
            headerName: 'System',
            flex: 1,
            valueGetter: ({ row }) => systemLookup.get(row.systemId) ?? '—'
        },
        {
            field: 'databaseType',
            headerName: 'Database',
            flex: 0.8,
            valueGetter: ({ row }) => {
                const parsed = parseJdbcConnectionString(row.connectionString);
                if (!parsed)
                    return '—';
                return DATABASE_LABELS[parsed.databaseType] ?? parsed.databaseType;
            }
        },
        {
            field: 'endpoint',
            headerName: 'Endpoint',
            flex: 1.4,
            renderCell: ({ row }) => {
                const summary = formatConnectionSummary(row.connectionString);
                return (_jsx(Tooltip, { title: row.notes ?? summary, placement: "top", enterDelay: 600, children: _jsx("span", { style: { overflow: 'hidden', textOverflow: 'ellipsis' }, children: summary }) }));
            }
        },
        {
            field: 'active',
            headerName: 'Status',
            flex: 0.6,
            renderCell: ({ value }) => (_jsx(Chip, { label: value ? 'Active' : 'Disabled', color: value ? 'success' : 'default', size: "small" }))
        }
    ];
    if (!canManage) {
        return baseColumns;
    }
    return [
        ...baseColumns,
        {
            field: 'actions',
            type: 'actions',
            headerName: 'Actions',
            getActions: (params) => [
                _jsx(GridActionsCellItem, { icon: _jsx(FlashOnIcon, { fontSize: "small" }), label: "Test", onClick: () => onTest?.(params.row), showInMenu: true }, "test"),
                _jsx(GridActionsCellItem, { icon: _jsx(EditIcon, { fontSize: "small" }), label: "Edit", onClick: () => onEdit?.(params.row), showInMenu: true }, "edit"),
                _jsx(GridActionsCellItem, { icon: _jsx(DeleteIcon, { fontSize: "small" }), label: "Delete", onClick: () => onDelete?.(params.row), showInMenu: true }, "delete")
            ]
        }
    ];
};
const SystemConnectionTable = ({ data, systems, loading, selectedId, canManage = false, onSelect, onEdit, onDelete, onTest }) => {
    const systemLookup = useMemo(() => new Map(systems.map((system) => [system.id, system.name])), [systems]);
    const columns = useMemo(() => buildColumns(systemLookup, canManage, onEdit, onDelete, onTest), [systemLookup, canManage, onEdit, onDelete, onTest]);
    const handleSelectionChange = (selection) => {
        const id = selection[0] ?? null;
        const selected = data.find((item) => item.id === id) ?? null;
        onSelect?.(selected);
    };
    return (_jsx("div", { style: { height: 520, width: '100%' }, children: _jsx(DataGrid, { rows: data, columns: columns, loading: loading, rowSelectionModel: selectedId ? [selectedId] : [], onRowSelectionModelChange: handleSelectionChange, onRowClick: (params) => onSelect?.(params.row), getRowId: (row) => row.id, disableRowSelectionOnClick: false }) }));
};
export default SystemConnectionTable;
