import { useMemo } from 'react';
import {
  DataGrid,
  GridActionsCellItem,
  GridColDef,
  GridRenderCellParams
} from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import DeleteForeverIcon from '@mui/icons-material/DeleteForever';
import StopCircleIcon from '@mui/icons-material/StopCircle';
import ListAltIcon from '@mui/icons-material/ListAlt';
import { Box, Chip, Stack, Switch, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';

import { DataWarehouseTarget, IngestionSchedule } from '../../types/data';
import { getDataGridStyles } from '../../utils/tableStyles';

const formatDateTime = (iso: string | null | undefined) => {
  if (!iso) return '—';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleString();
};

const formatStrategy = (strategy: IngestionSchedule['loadStrategy']) => {
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

export interface SelectionMetadata {
  schemaName: string;
  tableName: string;
  targetPreview: string;
}

interface IngestionScheduleTableProps {
  schedules: IngestionSchedule[];
  selectionLookup: Map<string, SelectionMetadata>;
  loading?: boolean;
  busyIds?: Set<string>;
  warehouseLabels: Record<DataWarehouseTarget, string>;
  onEdit?: (schedule: IngestionSchedule) => void;
  onRun?: (schedule: IngestionSchedule) => void;
  onToggleActive?: (schedule: IngestionSchedule, isActive: boolean) => void;
  onDelete?: (schedule: IngestionSchedule) => void;
  onAbort?: (schedule: IngestionSchedule) => void;
  onViewRuns?: (schedule: IngestionSchedule) => void;
}

const IngestionScheduleTable = ({
  schedules,
  selectionLookup,
  loading = false,
  busyIds,
  warehouseLabels,
  onEdit,
  onRun,
  onToggleActive,
  onDelete,
  onAbort,
  onViewRuns
}: IngestionScheduleTableProps) => {
  const theme = useTheme();

  const rows = useMemo(
    () =>
      schedules.map((schedule) => {
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
          warehouse: warehouseLabels[schedule.targetWarehouse] ?? schedule.targetWarehouse,
          isActive: schedule.isActive,
          lastStatus: schedule.lastRunStatus ?? '—',
          lastCompleted: schedule.lastRunCompletedAt ?? schedule.lastRunStartedAt ?? null,
          totalRuns: schedule.totalRuns,
          totalRows: schedule.totalRowsLoaded,
          lastError: schedule.lastRunError ?? null,
          rawSchedule: schedule
        };
      }),
    [schedules, selectionLookup, warehouseLabels]
  );

  const columns = useMemo<GridColDef[]>(
    () => [
      {
        field: 'tableLabel',
        headerName: 'Source Table',
        flex: 1.2,
        renderCell: ({ row }) => (
          <Stack>
            <Typography variant="body2">{row.tableLabel}</Typography>
            {row.selection && (
              <Typography variant="caption" color="text.secondary">
                {`${row.selection.schemaName}.${row.selection.tableName}`}
              </Typography>
            )}
          </Stack>
        )
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
        field: 'warehouse',
        headerName: 'Warehouse',
        flex: 0.9
      },
      {
        field: 'isActive',
        headerName: 'Active',
        width: 120,
        renderCell: ({ row }: GridRenderCellParams) => (
          <Switch
            size="small"
            checked={row.isActive}
            disabled={busyIds?.has(row.id)}
            onChange={(_event, checked) => onToggleActive?.(row.rawSchedule, checked)}
          />
        )
      },
      {
        field: 'lastStatus',
        headerName: 'Last Run',
        flex: 0.8,
        renderCell: ({ row }) => (
          <Stack spacing={0.5}>
            <Chip
              label={row.lastStatus ?? '—'}
              size="small"
              color={
                row.lastStatus === 'completed'
                  ? 'success'
                  : row.lastStatus === 'failed'
                    ? 'error'
                    : 'default'
              }
            />
            <Typography variant="caption" color="text.secondary">
              {formatDateTime(row.lastCompleted)}
            </Typography>
          </Stack>
        )
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
            <GridActionsCellItem
              key="edit"
              icon={<EditIcon fontSize="small" />}
              label="Edit"
              disabled={disabled}
              onClick={() => onEdit?.(row.rawSchedule)}
              showInMenu
            />,
            <GridActionsCellItem
              key="run"
              icon={<PlayCircleIcon fontSize="small" />}
              label="Run now"
              disabled={disabled || !row.isActive}
              onClick={() => onRun?.(row.rawSchedule)}
              showInMenu
            />,
            <GridActionsCellItem
              key="runs"
              icon={<ListAltIcon fontSize="small" />}
              label="View runs"
              disabled={disabled}
              onClick={() => onViewRuns?.(row.rawSchedule)}
              showInMenu
            />,
            <GridActionsCellItem
              key="abort"
              icon={<StopCircleIcon fontSize="small" />}
              label="Abort run"
              disabled={disabled || row.lastStatus !== 'running'}
              onClick={() => onAbort?.(row.rawSchedule)}
              showInMenu
            />,
            <GridActionsCellItem
              key="delete"
              icon={<DeleteForeverIcon fontSize="small" />}
              label="Delete"
              disabled={disabled}
              onClick={() => onDelete?.(row.rawSchedule)}
              showInMenu
            />
          ];
        }
      }
    ],
    [onEdit, onRun, onToggleActive, onAbort, onDelete, onViewRuns, busyIds]
  );

  return (
    <Box sx={{ height: 420, width: '100%' }}>
      <DataGrid
        rows={rows}
        columns={columns}
        loading={loading}
        disableRowSelectionOnClick
        sx={{
          ...getDataGridStyles(theme),
          '& .MuiDataGrid-cell': { alignItems: 'center' },
          '& .MuiDataGrid-row': { cursor: 'default' }
        }}
        getRowId={(row) => row.id}
      />
    </Box>
  );
};

export default IngestionScheduleTable;
