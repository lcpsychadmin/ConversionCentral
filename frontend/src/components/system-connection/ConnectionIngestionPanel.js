import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { Alert, Box, Button, Stack, Typography } from '@mui/material';
import { useIngestionSchedules } from '../../hooks/useIngestionSchedules';
import IngestionScheduleForm from './IngestionScheduleForm';
import IngestionScheduleTable from './IngestionScheduleTable';
import { buildIngestionTargetName } from '../../utils/ingestion';
const trimOrUndefined = (value) => {
    if (value === undefined || value === null) {
        return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
};
const ConnectionIngestionPanel = ({ connection, system, catalogRows }) => {
    const { schedulesQuery, createSchedule, updateSchedule, triggerSchedule, creating, updating, triggering } = useIngestionSchedules();
    const [formOpen, setFormOpen] = useState(false);
    const [editing, setEditing] = useState(null);
    const [busyScheduleId, setBusyScheduleId] = useState(null);
    const selectionLookup = useMemo(() => {
        const map = new Map();
        catalogRows.forEach((row) => {
            if (!row.selectionId) {
                return;
            }
            const targetPreview = buildIngestionTargetName(system ?? undefined, connection, row.schemaName, row.tableName);
            map.set(row.selectionId, {
                schemaName: row.schemaName,
                tableName: row.tableName,
                targetPreview
            });
        });
        return map;
    }, [catalogRows, connection, system]);
    const selectionOptions = useMemo(() => {
        return catalogRows
            .filter((row) => row.selectionId)
            .map((row) => ({
            id: row.selectionId,
            label: `${row.schemaName}.${row.tableName}`,
            schemaName: row.schemaName,
            tableName: row.tableName,
            targetPreview: buildIngestionTargetName(system ?? undefined, connection, row.schemaName, row.tableName),
            disabled: !row.selected
        }));
    }, [catalogRows, connection, system]);
    const selectionIds = useMemo(() => new Set(selectionLookup.keys()), [selectionLookup]);
    const { data: schedules = [], isLoading: schedulesLoading } = schedulesQuery;
    const relevantSchedules = useMemo(() => schedules.filter((schedule) => selectionIds.has(schedule.connectionTableSelectionId)), [schedules, selectionIds]);
    const busyIds = useMemo(() => {
        if (!busyScheduleId)
            return undefined;
        return new Set([busyScheduleId]);
    }, [busyScheduleId]);
    const handleCreateClick = () => {
        setEditing(null);
        setFormOpen(true);
    };
    const handleEdit = (schedule) => {
        setEditing(schedule);
        setFormOpen(true);
    };
    const handleRun = async (schedule) => {
        setBusyScheduleId(schedule.id);
        try {
            await triggerSchedule(schedule.id);
        }
        finally {
            setBusyScheduleId(null);
        }
    };
    const handleToggleActive = async (schedule, isActive) => {
        setBusyScheduleId(schedule.id);
        try {
            const payload = {
                isActive
            };
            await updateSchedule({ id: schedule.id, input: payload });
        }
        finally {
            setBusyScheduleId(null);
        }
    };
    const handleFormSubmit = async (values) => {
        if (editing) {
            setBusyScheduleId(editing.id);
            try {
                const payload = {
                    scheduleExpression: values.scheduleExpression,
                    timezone: trimOrUndefined(values.timezone),
                    loadStrategy: values.loadStrategy,
                    watermarkColumn: trimOrUndefined(values.watermarkColumn),
                    primaryKeyColumn: trimOrUndefined(values.primaryKeyColumn),
                    targetSchema: trimOrUndefined(values.targetSchema),
                    batchSize: values.batchSize,
                    isActive: values.isActive
                };
                await updateSchedule({ id: editing.id, input: payload });
                setFormOpen(false);
                setEditing(null);
            }
            finally {
                setBusyScheduleId(null);
            }
            return;
        }
        try {
            const payload = {
                connectionTableSelectionId: values.connectionTableSelectionId,
                scheduleExpression: values.scheduleExpression,
                timezone: trimOrUndefined(values.timezone) ?? null,
                loadStrategy: values.loadStrategy,
                watermarkColumn: trimOrUndefined(values.watermarkColumn) ?? null,
                primaryKeyColumn: trimOrUndefined(values.primaryKeyColumn) ?? null,
                targetSchema: trimOrUndefined(values.targetSchema) ?? null,
                targetTableName: null,
                batchSize: values.batchSize,
                isActive: values.isActive
            };
            await createSchedule(payload);
            setFormOpen(false);
        }
        finally {
            setEditing(null);
            setBusyScheduleId(null);
        }
    };
    const canCreate = catalogRows.some((row) => row.selected && row.selectionId);
    const nothingSelected = catalogRows.filter((row) => row.selected).length === 0;
    return (_jsxs(Box, { children: [_jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, justifyContent: "space-between", alignItems: { xs: 'flex-start', sm: 'center' }, gap: 2, mb: 2.5, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "h5", sx: { fontWeight: 600 }, children: "Ingestion Schedules" }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Use cron expressions to ingest selected tables when this connection is enabled." })] }), _jsx(Button, { variant: "contained", onClick: handleCreateClick, disabled: !canCreate || creating, children: "New Schedule" })] }), !canCreate && (_jsx(Alert, { severity: "info", sx: { mb: 2 }, children: nothingSelected
                    ? 'Select one or more tables in the source catalog to enable scheduling.'
                    : 'Save the catalog selection to continue. If you just added a table, refresh the catalog.' })), relevantSchedules.length === 0 && schedulesLoading === false && canCreate && (_jsx(Alert, { severity: "info", sx: { mb: 2 }, children: "No schedules yet for this connection. Create one to begin automated ingestion." })), _jsx(IngestionScheduleTable, { schedules: relevantSchedules, selectionLookup: selectionLookup, loading: schedulesLoading || updating || triggering, busyIds: busyIds, onEdit: handleEdit, onRun: handleRun, onToggleActive: handleToggleActive }), _jsx(IngestionScheduleForm, { open: formOpen, title: editing ? 'Edit Schedule' : 'Create Schedule', options: selectionOptions, initialValues: editing, disableSelectionChange: Boolean(editing), loading: creating || updating, onClose: () => {
                    setFormOpen(false);
                    setEditing(null);
                }, onSubmit: handleFormSubmit })] }));
};
export default ConnectionIngestionPanel;
