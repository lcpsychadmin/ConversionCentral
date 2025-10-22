import { jsx as _jsx } from "react/jsx-runtime";
import { DataGrid, GridActionsCellItem } from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import { useMemo } from 'react';
import { useTheme } from '@mui/material/styles';
import { getDataGridStyles } from '../../utils/tableStyles';
const buildColumns = (canManage, onEdit, onDelete) => {
    const baseColumns = [
        { field: 'name', headerName: 'Name', flex: 1 },
        { field: 'physicalName', headerName: 'Physical Name', flex: 1 },
        {
            field: 'systemType',
            headerName: 'Type',
            flex: 0.7,
            valueGetter: ({ row }) => row.systemType ?? '—'
        },
        { field: 'status', headerName: 'Status', flex: 0.6 }
    ];
    if (!canManage)
        return baseColumns;
    return [
        ...baseColumns,
        {
            field: 'actions',
            type: 'actions',
            headerName: 'Actions',
            getActions: (params) => [
                _jsx(GridActionsCellItem, { icon: _jsx(EditIcon, { fontSize: "small" }), label: "Edit", onClick: () => onEdit?.(params.row), showInMenu: true }, "edit"),
                _jsx(GridActionsCellItem, { icon: _jsx(DeleteIcon, { fontSize: "small" }), label: "Delete", onClick: () => onDelete?.(params.row), showInMenu: true }, "delete")
            ]
        }
    ];
};
const SystemTable = ({ data, loading, selectedId, canManage, onSelect, onEdit, onDelete }) => {
    const theme = useTheme();
    const columns = useMemo(() => buildColumns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);
    const handleSelectionChange = (selection) => {
        const id = selection[0] ?? null;
        const selected = data.find((item) => item.id === id) ?? null;
        onSelect?.(selected);
    };
    return (_jsx("div", { style: { height: 520, width: '100%' }, children: _jsx(DataGrid, { rows: data, columns: columns, loading: loading, rowSelectionModel: selectedId ? [selectedId] : [], onRowSelectionModelChange: handleSelectionChange, onRowClick: (params) => onSelect?.(params.row), getRowId: (row) => row.id, disableRowSelectionOnClick: false, sx: getDataGridStyles(theme) }) }));
};
export default SystemTable;
