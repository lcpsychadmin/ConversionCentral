import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useMemo, useState, useCallback, useRef, useEffect } from 'react';
import { Alert, Box, Button, Checkbox, Chip, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, Divider, FormControl, FormControlLabel, FormLabel, IconButton, InputAdornment, List, ListItem, ListItemText, MenuItem, Paper, Radio, RadioGroup, Stack, Table as MuiTable, TableBody as MuiTableBody, TableCell as MuiTableCell, TableContainer, TableHead as MuiTableHead, TableRow as MuiTableRow, TextField, Tooltip, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import SearchIcon from '@mui/icons-material/Search';
import SaveIcon from '@mui/icons-material/Save';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PublishIcon from '@mui/icons-material/Publish';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { isAxiosError } from 'axios';
import { useQuery } from 'react-query';
import ReactFlow, { ReactFlowProvider, Background, Controls, MiniMap, MarkerType, useEdgesState, useNodesState } from 'reactflow';
import 'reactflow/dist/style.css';
import { DndContext, KeyboardSensor, PointerSensor, closestCenter, useSensor, useSensors } from '@dnd-kit/core';
import { SortableContext, arrayMove, sortableKeyboardCoordinates, horizontalListSortingStrategy, useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { fetchTables, fetchFields } from '@services/tableService';
import { fetchReportPreview } from '@services/reportingService';
import ReportTableNode, { REPORTING_FIELD_DRAG_TYPE } from '@components/reporting/ReportTableNode';
const nodeTypes = { reportTable: ReportTableNode };
const tableDragMimeType = 'application/reporting-table';
const SortableOutputColumn = ({ column, isFirst, isLast, groupingEnabled, aggregateOptions, rowDescriptors, criteriaRowCount, onRemove, onSortChange, onAggregateChange, onShowToggle, onCriteriaChange }) => {
    const theme = useTheme();
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
        id: column.fieldId
    });
    const style = {
        transform: CSS.Transform.toString(transform),
        transition
    };
    const rows = rowDescriptors.reduce((acc, descriptor) => {
        const { key } = descriptor;
        let content = null;
        if (key === 'field') {
            content = (_jsxs(Box, { sx: { display: 'flex', alignItems: 'center', gap: 0.5 }, children: [_jsx(Tooltip, { title: "Drag to reorder", children: _jsx(IconButton, { size: "small", ...attributes, ...listeners, sx: {
                                cursor: 'grab',
                                color: alpha(theme.palette.text.secondary, 0.8),
                                '&:hover': {
                                    color: theme.palette.primary.main
                                }
                            }, children: _jsx(DragIndicatorIcon, { fontSize: "small" }) }) }), _jsx(TextField, { size: "small", value: column.field.name, fullWidth: true, InputProps: { readOnly: true }, sx: { flex: 1, '& .MuiInputBase-input': { fontSize: 13 } } }), _jsx(Tooltip, { title: "Remove column", children: _jsx("span", { children: _jsx(IconButton, { size: "small", onClick: () => onRemove(column.fieldId), children: _jsx(DeleteOutlineIcon, { fontSize: "small" }) }) }) })] }));
        }
        else if (key === 'table') {
            content = (_jsx(TextField, { size: "small", value: column.table?.name ?? '—', fullWidth: true, InputProps: { readOnly: true }, sx: { '& .MuiInputBase-input': { fontSize: 13 } } }));
        }
        else if (key === 'group' && groupingEnabled) {
            const aggregateValue = column.settings.aggregate ?? 'groupBy';
            content = (_jsx(TextField, { select: true, size: "small", fullWidth: true, value: aggregateValue, onChange: (event) => onAggregateChange(column.fieldId, event.target.value), sx: { '& .MuiInputBase-input': { fontSize: 13 } }, children: aggregateOptions.map((option) => (_jsx(MenuItem, { value: option.value, children: option.label }, option.value))) }));
        }
        else if (key === 'sort') {
            content = (_jsxs(TextField, { select: true, size: "small", fullWidth: true, value: column.settings.sort, onChange: (event) => onSortChange(column.fieldId, event.target.value), sx: { '& .MuiInputBase-input': { fontSize: 13 } }, children: [_jsx(MenuItem, { value: "none", children: "None" }), _jsx(MenuItem, { value: "asc", children: "Ascending" }), _jsx(MenuItem, { value: "desc", children: "Descending" })] }));
        }
        else if (key === 'show') {
            content = (_jsx(Checkbox, { size: "small", checked: column.settings.show, onChange: (event) => onShowToggle(column.fieldId, event.target.checked) }));
        }
        else if (key.startsWith('criteria-')) {
            const index = Number(key.split('-')[1]);
            if (!Number.isNaN(index) && index < criteriaRowCount) {
                content = (_jsx(TextField, { size: "small", fullWidth: true, value: column.settings.criteria[index] ?? '', onChange: (event) => onCriteriaChange(column.fieldId, index, event.target.value), placeholder: index === 0 ? 'e.g. >= 100' : '', sx: { '& .MuiInputBase-input': { fontSize: 13 } } }));
            }
        }
        if (!content) {
            return acc;
        }
        acc.push({
            key,
            content,
            paddingY: descriptor.paddingY,
            justifyContent: descriptor.justifyContent ?? 'flex-start'
        });
        return acc;
    }, []);
    return (_jsx(Box, { ref: setNodeRef, style: style, sx: {
            minWidth: 220,
            flex: '0 0 auto',
            display: 'flex',
            flexDirection: 'column',
            borderLeft: isFirst ? 'none' : `1px solid ${theme.palette.divider}`,
            borderRight: isLast ? 'none' : `1px solid ${theme.palette.divider}`,
            backgroundColor: alpha(theme.palette.background.paper, 0.85),
            opacity: isDragging ? 0.9 : 1,
            boxShadow: isDragging ? `0 12px 28px ${alpha(theme.palette.primary.main, 0.18)}` : 'none',
            transition: 'box-shadow 120ms ease, opacity 120ms ease'
        }, children: rows.map((row, index) => (_jsx(Box, { sx: {
                px: 1.25,
                py: row.paddingY,
                display: 'flex',
                alignItems: row.justifyContent === 'center' ? 'center' : 'stretch',
                justifyContent: row.justifyContent,
                borderBottom: index === rows.length - 1 ? 'none' : `1px solid ${theme.palette.divider}`
            }, children: row.content }, row.key))) }));
};
const designerStorageKey = 'cc_reporting_designer_draft';
const defaultNodeWidth = 260;
const defaultNodeHeight = 320;
const minNodeWidth = 220;
const minNodeHeight = 200;
const ReportingDesignerContent = () => {
    const { data: tables = [], isLoading: tablesLoading, isError: tablesError } = useQuery(['reporting-tables'], fetchTables);
    const { data: fields = [] } = useQuery(['reporting-fields'], fetchFields);
    const [searchTerm, setSearchTerm] = useState('');
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const [selectedFieldOrder, setSelectedFieldOrder] = useState([]);
    const [fieldSettings, setFieldSettings] = useState({});
    const [criteriaRowCount, setCriteriaRowCount] = useState(2);
    const [groupingEnabled, setGroupingEnabled] = useState(false);
    const [outputDropActive, setOutputDropActive] = useState(false);
    const [statusBanner, setStatusBanner] = useState(null);
    const [previewOpen, setPreviewOpen] = useState(false);
    const [previewData, setPreviewData] = useState(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewError, setPreviewError] = useState(null);
    const [joinDialogState, setJoinDialogState] = useState(null);
    const reactFlowInstanceRef = useRef(null);
    const reactFlowWrapper = useRef(null);
    const hasHydratedDraftRef = useRef(false);
    const draggedFieldRef = useRef(null);
    const theme = useTheme();
    const joinEdgePresentation = useMemo(() => {
        const stroke = theme.palette.primary.main;
        return {
            type: 'smoothstep',
            animated: false,
            style: {
                stroke,
                strokeWidth: 1.8
            },
            markerEnd: {
                type: MarkerType.ArrowClosed,
                color: stroke,
                width: 18,
                height: 18
            }
        };
    }, [theme]);
    const fieldsByTable = useMemo(() => {
        const map = new Map();
        fields.forEach((field) => {
            if (!map.has(field.tableId)) {
                map.set(field.tableId, []);
            }
            map.get(field.tableId).push(field);
        });
        return map;
    }, [fields]);
    const tablesById = useMemo(() => {
        const map = new Map();
        tables.forEach((table) => map.set(table.id, table));
        return map;
    }, [tables]);
    const fieldsById = useMemo(() => {
        const map = new Map();
        fields.forEach((field) => map.set(field.id, field));
        return map;
    }, [fields]);
    const fieldIdSet = useMemo(() => new Set(fields.map((field) => field.id)), [fields]);
    const extractFieldIdFromHandle = useCallback((handleId) => {
        if (!handleId) {
            return null;
        }
        const separatorIndex = handleId.indexOf(':');
        if (separatorIndex === -1) {
            return null;
        }
        return handleId.slice(separatorIndex + 1) || null;
    }, []);
    useEffect(() => {
        setSelectedFieldOrder((current) => {
            const filtered = current.filter((fieldId) => fieldIdSet.has(fieldId));
            return filtered.length === current.length ? current : filtered;
        });
    }, [fieldIdSet]);
    useEffect(() => {
        setFieldSettings((current) => {
            let didChange = false;
            const next = {};
            selectedFieldOrder.forEach((fieldId) => {
                if (!fieldIdSet.has(fieldId)) {
                    return;
                }
                const existing = current[fieldId];
                const nextCriteria = Array.from({ length: criteriaRowCount }, (_, index) => existing?.criteria[index] ?? '');
                const nextAggregate = groupingEnabled ? existing?.aggregate ?? 'groupBy' : null;
                if (existing &&
                    existing.criteria.length === criteriaRowCount &&
                    existing.criteria.every((value, index) => value === nextCriteria[index]) &&
                    existing.aggregate === nextAggregate) {
                    next[fieldId] = existing;
                    return;
                }
                didChange = true;
                next[fieldId] = {
                    show: existing?.show ?? true,
                    sort: existing?.sort ?? 'none',
                    aggregate: nextAggregate,
                    criteria: nextCriteria
                };
            });
            if (!didChange) {
                const sameKeys = Object.keys(current).length === Object.keys(next).length;
                const sameRefs = sameKeys && Object.keys(next).every((key) => current[key] === next[key]);
                if (sameRefs) {
                    return current;
                }
            }
            return next;
        });
    }, [criteriaRowCount, fieldIdSet, groupingEnabled, selectedFieldOrder]);
    const createNodeData = useCallback((table, tableFields) => {
        const sortedFields = [...tableFields].sort((a, b) => a.name.localeCompare(b.name));
        return {
            tableId: table.id,
            label: table.name,
            subtitle: table.schemaName ? `${table.schemaName}.${table.physicalName}` : table.physicalName,
            meta: table.status ? `Status: ${table.status}` : undefined,
            fields: sortedFields.map((field) => ({
                id: field.id,
                name: field.name,
                type: field.fieldType,
                description: field.description ?? null
            }))
        };
    }, []);
    useEffect(() => {
        if (hasHydratedDraftRef.current) {
            return;
        }
        if (typeof window === 'undefined') {
            hasHydratedDraftRef.current = true;
            return;
        }
        if (nodes.length > 0) {
            hasHydratedDraftRef.current = true;
            return;
        }
        if (tables.length === 0 || fields.length === 0) {
            return;
        }
        const raw = window.localStorage.getItem(designerStorageKey);
        if (!raw) {
            hasHydratedDraftRef.current = true;
            return;
        }
        try {
            const parsed = JSON.parse(raw);
            if (!parsed) {
                hasHydratedDraftRef.current = true;
                return;
            }
            const normalizedCriteriaCount = Math.max(1, parsed.criteriaRowCount ?? 1);
            const groupingFromDraft = Boolean(parsed.groupingEnabled);
            const restoredNodes = (parsed.tables ?? [])
                .filter((placement) => tablesById.has(placement.tableId))
                .map((placement) => {
                const table = tablesById.get(placement.tableId);
                const tableFields = fieldsByTable.get(placement.tableId) ?? [];
                const savedWidth = placement.dimensions?.width;
                const savedHeight = placement.dimensions?.height;
                const width = typeof savedWidth === 'number' && savedWidth >= minNodeWidth
                    ? savedWidth
                    : defaultNodeWidth;
                const height = typeof savedHeight === 'number' && savedHeight >= minNodeHeight
                    ? savedHeight
                    : defaultNodeHeight;
                return {
                    id: placement.tableId,
                    type: 'reportTable',
                    position: {
                        x: placement.position?.x ?? 0,
                        y: placement.position?.y ?? 0
                    },
                    draggable: true,
                    selectable: true,
                    width,
                    height,
                    style: {
                        width,
                        height,
                        minWidth: minNodeWidth,
                        minHeight: minNodeHeight
                    },
                    data: createNodeData(table, tableFields)
                };
            });
            const restoredNodeIds = new Set(restoredNodes.map((node) => node.id));
            const restoredEdges = (parsed.joins ?? [])
                .filter((join) => restoredNodeIds.has(join.sourceTableId) && restoredNodeIds.has(join.targetTableId))
                .map((join) => {
                const sourceFieldId = join.sourceFieldId ?? undefined;
                const targetFieldId = join.targetFieldId ?? undefined;
                const joinType = join.joinType ?? 'inner';
                const baseEdge = {
                    id: join.id ?? `${join.sourceTableId}-${join.targetTableId}`,
                    source: join.sourceTableId,
                    target: join.targetTableId,
                    sourceHandle: sourceFieldId ? `source:${sourceFieldId}` : undefined,
                    targetHandle: targetFieldId ? `target:${targetFieldId}` : undefined,
                    data: {
                        sourceFieldId,
                        targetFieldId,
                        joinType
                    }
                };
                return {
                    ...baseEdge,
                    ...joinEdgePresentation,
                    style: { ...joinEdgePresentation.style },
                    markerEnd: { ...joinEdgePresentation.markerEnd }
                };
            });
            const orderedColumns = [...(parsed.columns ?? [])].sort((a, b) => a.order - b.order);
            const restoredFieldOrder = [];
            const restoredFieldSettings = {};
            orderedColumns.forEach((column) => {
                if (!fieldIdSet.has(column.fieldId)) {
                    return;
                }
                const normalizedSort = column.sort === 'asc' || column.sort === 'desc' ? column.sort : 'none';
                const normalizedAggregate = groupingFromDraft
                    ? column.aggregate ?? 'groupBy'
                    : null;
                restoredFieldOrder.push(column.fieldId);
                restoredFieldSettings[column.fieldId] = {
                    show: column.show ?? true,
                    sort: normalizedSort,
                    aggregate: normalizedAggregate,
                    criteria: Array.from({ length: normalizedCriteriaCount }, (_, index) => column.criteria?.[index] ?? '')
                };
            });
            setCriteriaRowCount(normalizedCriteriaCount);
            setGroupingEnabled(groupingFromDraft);
            setNodes(restoredNodes);
            setEdges(restoredEdges);
            setSelectedFieldOrder(restoredFieldOrder);
            setFieldSettings(restoredFieldSettings);
            setStatusBanner({
                message: 'Restored saved reporting draft from your last session.',
                color: 'success.main'
            });
        }
        catch (error) {
            console.warn('Unable to hydrate reporting designer draft', error);
            setStatusBanner({
                message: 'Stored draft could not be restored. Starting with a fresh canvas.',
                color: 'warning.main'
            });
        }
        finally {
            hasHydratedDraftRef.current = true;
        }
    }, [
        createNodeData,
        fieldIdSet,
        fields.length,
        fieldsByTable,
        joinEdgePresentation,
        nodes.length,
        tables.length,
        tablesById,
        setNodes,
        setEdges,
        setSelectedFieldOrder,
        setFieldSettings,
        setStatusBanner,
        setCriteriaRowCount,
        setGroupingEnabled
    ]);
    useEffect(() => {
        setEdges((current) => current.map((edge) => ({
            ...edge,
            ...joinEdgePresentation,
            style: { ...joinEdgePresentation.style },
            markerEnd: { ...joinEdgePresentation.markerEnd }
        })));
    }, [joinEdgePresentation, setEdges]);
    const canvasPositionFor = useCallback((index) => ({
        x: (index % 3) * 260,
        y: Math.floor(index / 3) * 200
    }), []);
    const removeNodeById = useCallback((nodeId) => {
        const tableName = tablesById.get(nodeId)?.name ?? 'Table';
        setNodes((current) => current.filter((node) => node.id !== nodeId));
        setEdges((current) => current.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
        setSelectedFieldOrder((current) => current.filter((fieldId) => fieldsById.get(fieldId)?.tableId !== nodeId));
        setFieldSettings((current) => {
            const relatedFields = fieldsByTable.get(nodeId) ?? [];
            if (relatedFields.length === 0) {
                return current;
            }
            const next = { ...current };
            relatedFields.forEach((field) => {
                delete next[field.id];
            });
            return next;
        });
        setStatusBanner({
            message: `${tableName} removed from the Relationship Canvas.`,
            color: 'warning.main'
        });
    }, [fieldsById, fieldsByTable, setEdges, setFieldSettings, setNodes, setSelectedFieldOrder, setStatusBanner, tablesById]);
    const handleRemoveNode = useCallback((nodeId) => {
        const tableName = tablesById.get(nodeId)?.name ?? 'this table';
        const proceed = typeof window === 'undefined'
            ? true
            : window.confirm(`Remove ${tableName} from the Relationship Canvas? Joins and selected columns that reference it will also be cleared.`);
        if (!proceed) {
            return;
        }
        removeNodeById(nodeId);
    }, [removeNodeById, tablesById]);
    const addTableNode = useCallback((table, position) => {
        let createdNode = null;
        setNodes((current) => {
            const existing = current.find((node) => node.id === table.id);
            if (existing) {
                createdNode = existing;
                return current;
            }
            const nextPosition = position ?? canvasPositionFor(current.length);
            const tableFields = fieldsByTable.get(table.id) ?? [];
            const next = {
                id: table.id,
                type: 'reportTable',
                position: nextPosition,
                draggable: true,
                selectable: true,
                width: defaultNodeWidth,
                height: defaultNodeHeight,
                style: {
                    width: defaultNodeWidth,
                    height: defaultNodeHeight,
                    minWidth: minNodeWidth,
                    minHeight: minNodeHeight
                },
                data: {
                    ...createNodeData(table, tableFields),
                    onRemoveTable: handleRemoveNode
                }
            };
            createdNode = next;
            return [...current, next];
        });
        return createdNode;
    }, [canvasPositionFor, createNodeData, fieldsByTable, handleRemoveNode, setNodes]);
    const focusNode = useCallback((node) => {
        const instance = reactFlowInstanceRef.current;
        if (!node) {
            return;
        }
        if (!instance) {
            return;
        }
        requestAnimationFrame(() => {
            const nodeWidth = node.width ?? (typeof node.style?.width === 'number' ? node.style.width : defaultNodeWidth);
            const nodeHeight = node.height ?? (typeof node.style?.height === 'number' ? node.style.height : defaultNodeHeight);
            const centerX = node.position.x + nodeWidth / 2;
            const centerY = node.position.y + nodeHeight / 2;
            if (typeof instance.setCenter === 'function') {
                instance.setCenter(centerX, centerY, { zoom: 1, duration: 200 });
            }
            else if (typeof instance.fitView === 'function') {
                instance.fitView({ nodes: [node], padding: 0.4, duration: 200 });
            }
        });
    }, []);
    const handlePaletteAdd = useCallback((table) => {
        const node = addTableNode(table);
        focusNode(node);
    }, [addTableNode, focusNode]);
    useEffect(() => {
        setNodes((current) => {
            if (current.length === 0) {
                return current;
            }
            let didChange = false;
            const nextNodes = current.map((node) => {
                const table = tables.find((item) => item.id === node.id);
                if (!table) {
                    return node;
                }
                const tableFields = fieldsByTable.get(node.id) ?? [];
                const updatedData = createNodeData(table, tableFields);
                const existingData = node.data;
                const sameMeta = existingData.label === updatedData.label &&
                    existingData.subtitle === updatedData.subtitle &&
                    existingData.meta === updatedData.meta;
                const sameFields = Array.isArray(existingData.fields) &&
                    existingData.fields.length === updatedData.fields.length &&
                    existingData.fields.every((field, index) => {
                        const other = updatedData.fields[index];
                        return (field.id === other.id &&
                            field.name === other.name &&
                            field.type === other.type &&
                            field.description === other.description);
                    });
                if (sameMeta && sameFields) {
                    return node;
                }
                didChange = true;
                return {
                    ...node,
                    data: {
                        ...updatedData,
                        selectedFieldIds: existingData.selectedFieldIds,
                        onFieldAdd: existingData.onFieldAdd,
                        onFieldDragEnd: existingData.onFieldDragEnd,
                        onFieldDragStart: existingData.onFieldDragStart,
                        onFieldJoin: existingData.onFieldJoin,
                        onFieldToggle: existingData.onFieldToggle,
                        onRemoveTable: existingData.onRemoveTable ?? handleRemoveNode
                    },
                    draggable: true,
                    selectable: true
                };
            });
            return didChange ? nextNodes : current;
        });
    }, [createNodeData, fieldsByTable, handleRemoveNode, setNodes, tables]);
    const filteredTables = useMemo(() => {
        const term = searchTerm.trim().toLowerCase();
        if (!term) {
            return tables;
        }
        return tables.filter((item) => item.name.toLowerCase().includes(term) ||
            item.physicalName.toLowerCase().includes(term) ||
            (item.schemaName ? item.schemaName.toLowerCase().includes(term) : false));
    }, [searchTerm, tables]);
    const nodesById = useMemo(() => new Set(nodes.map((node) => node.id)), [nodes]);
    const aggregateOptions = useMemo(() => [
        { label: 'Group By', value: 'groupBy' },
        { label: 'Sum', value: 'sum' },
        { label: 'Avg', value: 'avg' },
        { label: 'Min', value: 'min' },
        { label: 'Max', value: 'max' },
        { label: 'Count', value: 'count' },
        { label: 'First', value: 'first' },
        { label: 'Last', value: 'last' },
        { label: 'Expression', value: 'expression' },
        { label: 'Where', value: 'where' }
    ], []);
    const joinTypeOptions = useMemo(() => [
        {
            value: 'inner',
            label: 'Inner Join',
            helper: 'Only include rows where the joined fields match in both tables.'
        },
        {
            value: 'left',
            label: 'Left Join',
            helper: 'Include all rows from the left table and matching rows from the right table.'
        },
        {
            value: 'right',
            label: 'Right Join',
            helper: 'Include all rows from the right table and matching rows from the left table.'
        }
    ], []);
    const canConfirmJoin = Boolean(joinDialogState?.sourceFieldId && joinDialogState?.targetFieldId);
    const joinDialogSourceName = joinDialogState
        ? tablesById.get(joinDialogState.sourceTableId)?.name ?? joinDialogState.sourceTableId
        : '';
    const joinDialogTargetName = joinDialogState
        ? tablesById.get(joinDialogState.targetTableId)?.name ?? joinDialogState.targetTableId
        : '';
    const joinDialogMode = joinDialogState?.mode ?? 'create';
    const joinDialogIsEdit = joinDialogMode === 'edit';
    const joinDialogTitleText = joinDialogIsEdit ? 'Edit Join' : 'Configure Join';
    const joinDialogConfirmLabel = joinDialogIsEdit ? 'Save Changes' : 'Add Join';
    const joinDialogDescription = joinDialogIsEdit
        ? `Update how ${joinDialogSourceName} links to ${joinDialogTargetName}.`
        : `Define how ${joinDialogSourceName} links to ${joinDialogTargetName}.`;
    const selectedFieldSet = useMemo(() => new Set(selectedFieldOrder), [selectedFieldOrder]);
    const ensureFieldAdded = useCallback((fieldId) => {
        if (!fieldIdSet.has(fieldId) || selectedFieldSet.has(fieldId)) {
            return;
        }
        setSelectedFieldOrder((current) => (current.includes(fieldId) ? current : [...current, fieldId]));
        setFieldSettings((current) => {
            const existing = current[fieldId];
            const nextCriteria = Array.from({ length: criteriaRowCount }, (_, index) => existing?.criteria[index] ?? '');
            if (existing) {
                const hasMatchingCriteria = existing.criteria.length === criteriaRowCount && existing.criteria.every((value, index) => value === nextCriteria[index]);
                const aggregateValue = groupingEnabled ? existing.aggregate ?? 'groupBy' : null;
                if (hasMatchingCriteria && existing.aggregate === aggregateValue) {
                    return current;
                }
                return {
                    ...current,
                    [fieldId]: {
                        ...existing,
                        aggregate: aggregateValue,
                        criteria: nextCriteria
                    }
                };
            }
            return {
                ...current,
                [fieldId]: {
                    show: true,
                    sort: 'none',
                    aggregate: groupingEnabled ? 'groupBy' : null,
                    criteria: nextCriteria
                }
            };
        });
    }, [criteriaRowCount, fieldIdSet, groupingEnabled, selectedFieldSet]);
    const handleFieldDragStart = useCallback((payload) => {
        draggedFieldRef.current = payload;
    }, []);
    const handleFieldDragEnd = useCallback(() => {
        draggedFieldRef.current = null;
    }, []);
    const handleFieldJoin = useCallback((source, target) => {
        const resolvedSource = source ?? draggedFieldRef.current;
        if (!resolvedSource) {
            setStatusBanner({
                message: 'Drag a field from another table to define a join.',
                color: 'warning.main'
            });
            return;
        }
        const sourceField = fieldsById.get(resolvedSource.fieldId);
        const targetField = fieldsById.get(target.fieldId);
        if (!sourceField || !targetField) {
            setStatusBanner({
                message: 'Unable to resolve field metadata for the requested join.',
                color: 'error.main'
            });
            draggedFieldRef.current = null;
            return;
        }
        const sourceTableId = sourceField.tableId;
        const targetTableId = targetField.tableId;
        if (sourceTableId === targetTableId) {
            setStatusBanner({
                message: 'Select fields from different tables to create a join.',
                color: 'warning.main'
            });
            draggedFieldRef.current = null;
            return;
        }
        if (!nodesById.has(sourceTableId) || !nodesById.has(targetTableId)) {
            setStatusBanner({
                message: 'Add both tables to the canvas before defining a join.',
                color: 'warning.main'
            });
            draggedFieldRef.current = null;
            return;
        }
        const alreadyExists = edges.some((edge) => {
            const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
            const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
            if (!matchesDirect && !matchesReverse) {
                return false;
            }
            const edgeData = edge.data;
            if (!edgeData) {
                return false;
            }
            const directMatch = matchesDirect &&
                edgeData.sourceFieldId === resolvedSource.fieldId &&
                edgeData.targetFieldId === target.fieldId;
            const reverseMatch = matchesReverse &&
                edgeData.sourceFieldId === target.fieldId &&
                edgeData.targetFieldId === resolvedSource.fieldId;
            return directMatch || reverseMatch;
        });
        if (alreadyExists) {
            const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
            const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;
            setStatusBanner({
                message: `A join between ${sourceTableName}.${sourceField.name} and ${targetTableName}.${targetField.name} already exists.`,
                color: 'text.secondary'
            });
            draggedFieldRef.current = null;
            return;
        }
        setJoinDialogState({
            mode: 'create',
            sourceTableId,
            sourceFieldId: sourceField.id,
            targetTableId,
            targetFieldId: targetField.id,
            joinType: 'inner'
        });
        draggedFieldRef.current = null;
    }, [edges, fieldsById, nodesById, setStatusBanner, tablesById]);
    const handleJoinDialogClose = useCallback(() => {
        draggedFieldRef.current = null;
        setJoinDialogState(null);
    }, []);
    const handleJoinDialogFieldChange = useCallback((side, fieldId) => {
        setJoinDialogState((current) => {
            if (!current) {
                return current;
            }
            return side === 'source'
                ? { ...current, sourceFieldId: fieldId }
                : { ...current, targetFieldId: fieldId };
        });
    }, []);
    const handleJoinDialogTypeChange = useCallback((value) => {
        setJoinDialogState((current) => (current ? { ...current, joinType: value } : current));
    }, []);
    const handleJoinDialogConfirm = useCallback(() => {
        if (!joinDialogState) {
            return;
        }
        const { mode, edgeId, sourceTableId, sourceFieldId, targetTableId, targetFieldId, joinType } = joinDialogState;
        const isEditMode = mode === 'edit';
        const sourceField = fieldsById.get(sourceFieldId);
        const targetField = fieldsById.get(targetFieldId);
        if (!sourceField || !targetField) {
            setStatusBanner({
                message: 'Unable to resolve field metadata for the requested join.',
                color: 'error.main'
            });
            draggedFieldRef.current = null;
            setJoinDialogState(null);
            return;
        }
        let duplicate = false;
        let created = false;
        let updated = false;
        setEdges((existing) => {
            const duplicateEdge = existing.some((edge) => {
                if (isEditMode && edge.id === edgeId) {
                    return false;
                }
                const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
                const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
                if (!matchesDirect && !matchesReverse) {
                    return false;
                }
                const edgeData = edge.data;
                if (!edgeData) {
                    return false;
                }
                const directMatch = matchesDirect &&
                    edgeData.sourceFieldId === sourceFieldId &&
                    edgeData.targetFieldId === targetFieldId;
                const reverseMatch = matchesReverse &&
                    edgeData.sourceFieldId === targetFieldId &&
                    edgeData.targetFieldId === sourceFieldId;
                return directMatch || reverseMatch;
            });
            if (duplicateEdge) {
                duplicate = true;
                return existing;
            }
            if (isEditMode && edgeId) {
                let didChange = false;
                const nextEdges = existing.map((edge) => {
                    if (edge.id !== edgeId) {
                        return edge;
                    }
                    const nextEdge = {
                        ...edge,
                        source: sourceTableId,
                        target: targetTableId,
                        sourceHandle: `source:${sourceFieldId}`,
                        targetHandle: `target:${targetFieldId}`,
                        data: {
                            sourceFieldId,
                            targetFieldId,
                            joinType
                        },
                        ...joinEdgePresentation,
                        style: { ...joinEdgePresentation.style },
                        markerEnd: { ...joinEdgePresentation.markerEnd }
                    };
                    const currentData = edge.data;
                    const sameConnection = edge.source === nextEdge.source &&
                        edge.target === nextEdge.target &&
                        edge.sourceHandle === nextEdge.sourceHandle &&
                        edge.targetHandle === nextEdge.targetHandle &&
                        (currentData?.sourceFieldId ?? null) === sourceFieldId &&
                        (currentData?.targetFieldId ?? null) === targetFieldId &&
                        (currentData?.joinType ?? 'inner') === joinType;
                    if (!sameConnection) {
                        didChange = true;
                    }
                    return nextEdge;
                });
                updated = didChange;
                return nextEdges;
            }
            const existingIds = new Set(existing.map((edge) => edge.id));
            const idBase = `join:${sourceTableId}:${targetTableId}`;
            let nextEdgeId = idBase;
            let counter = 1;
            while (existingIds.has(nextEdgeId)) {
                nextEdgeId = `${idBase}:${counter++}`;
            }
            created = true;
            return [
                ...existing,
                {
                    id: nextEdgeId,
                    source: sourceTableId,
                    target: targetTableId,
                    sourceHandle: `source:${sourceFieldId}`,
                    targetHandle: `target:${targetFieldId}`,
                    data: {
                        sourceFieldId,
                        targetFieldId,
                        joinType
                    },
                    ...joinEdgePresentation,
                    style: { ...joinEdgePresentation.style },
                    markerEnd: { ...joinEdgePresentation.markerEnd }
                }
            ];
        });
        const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
        const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;
        if (duplicate) {
            setStatusBanner({
                message: `A join between ${sourceTableName}.${sourceField.name} and ${targetTableName}.${targetField.name} already exists.`,
                color: 'text.secondary'
            });
            return;
        }
        const joinLabel = joinTypeOptions.find((option) => option.value === joinType)?.label ?? 'Join';
        if (isEditMode) {
            if (updated) {
                setStatusBanner({
                    message: `${joinLabel} updated: ${sourceTableName}.${sourceField.name} ↔ ${targetTableName}.${targetField.name}.`,
                    color: 'success.main'
                });
            }
            else {
                setStatusBanner({
                    message: 'No changes detected for the selected join.',
                    color: 'text.secondary'
                });
            }
        }
        else if (created) {
            setStatusBanner({
                message: `${joinLabel} added: ${sourceTableName}.${sourceField.name} ↔ ${targetTableName}.${targetField.name}.`,
                color: 'success.main'
            });
        }
        draggedFieldRef.current = null;
        setJoinDialogState(null);
    }, [fieldsById, joinDialogState, joinEdgePresentation, joinTypeOptions, setEdges, setStatusBanner, tablesById]);
    const handleJoinDialogDelete = useCallback(() => {
        if (!joinDialogState || joinDialogState.mode !== 'edit') {
            return;
        }
        const { edgeId, sourceTableId, sourceFieldId, targetTableId, targetFieldId } = joinDialogState;
        const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
        const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;
        const sourceFieldName = fieldsById.get(sourceFieldId)?.name ?? sourceFieldId;
        const targetFieldName = fieldsById.get(targetFieldId)?.name ?? targetFieldId;
        const proceed = typeof window === 'undefined'
            ? true
            : window.confirm(`Remove the join between ${sourceTableName}.${sourceFieldName} and ${targetTableName}.${targetFieldName}?`);
        if (!proceed) {
            return;
        }
        setEdges((current) => current.filter((edge) => {
            if (edgeId && edge.id === edgeId) {
                return false;
            }
            const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
            const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
            if (!matchesDirect && !matchesReverse) {
                return true;
            }
            const edgeData = edge.data;
            if (!edgeData) {
                return true;
            }
            const directMatch = matchesDirect &&
                edgeData.sourceFieldId === sourceFieldId &&
                edgeData.targetFieldId === targetFieldId;
            const reverseMatch = matchesReverse &&
                edgeData.sourceFieldId === targetFieldId &&
                edgeData.targetFieldId === sourceFieldId;
            return !(directMatch || reverseMatch);
        }));
        setStatusBanner({
            message: `Join removed: ${sourceTableName}.${sourceFieldName} ↔ ${targetTableName}.${targetFieldName}.`,
            color: 'warning.main'
        });
        draggedFieldRef.current = null;
        setJoinDialogState(null);
    }, [fieldsById, joinDialogState, setEdges, setJoinDialogState, setStatusBanner, tablesById]);
    const handleConnect = useCallback((connection) => {
        const { source, target, sourceHandle, targetHandle } = connection;
        if (!source || !target) {
            setStatusBanner({
                message: 'Select both a source and target field to define a join.',
                color: 'warning.main'
            });
            return;
        }
        const sourceFieldId = extractFieldIdFromHandle(sourceHandle);
        const targetFieldId = extractFieldIdFromHandle(targetHandle);
        if (!sourceFieldId || !targetFieldId) {
            setStatusBanner({
                message: 'Use the field-level connection points to define joins.',
                color: 'warning.main'
            });
            return;
        }
        handleFieldJoin({ tableId: source, fieldId: sourceFieldId }, { tableId: target, fieldId: targetFieldId });
    }, [extractFieldIdFromHandle, handleFieldJoin, setStatusBanner]);
    const handleEdgeClick = useCallback((event, edge) => {
        event.stopPropagation();
        if (!edge.source || !edge.target) {
            return;
        }
        const edgeData = edge.data;
        const sourceFieldId = edgeData?.sourceFieldId;
        const targetFieldId = edgeData?.targetFieldId;
        if (!sourceFieldId || !targetFieldId) {
            setStatusBanner({
                message: 'Only joins created in the designer can be edited.',
                color: 'warning.main'
            });
            return;
        }
        setJoinDialogState({
            mode: 'edit',
            edgeId: edge.id,
            sourceTableId: edge.source,
            sourceFieldId,
            targetTableId: edge.target,
            targetFieldId,
            joinType: edgeData?.joinType ?? 'inner'
        });
    }, [setStatusBanner]);
    const selectedFieldColumns = useMemo(() => {
        const columns = [];
        selectedFieldOrder.forEach((fieldId) => {
            const field = fieldsById.get(fieldId);
            if (!field) {
                return;
            }
            const table = tablesById.get(field.tableId);
            const settings = fieldSettings[fieldId] ?? {
                show: true,
                sort: 'none',
                aggregate: groupingEnabled ? 'groupBy' : null,
                criteria: Array(criteriaRowCount).fill('')
            };
            columns.push({ fieldId, field, table, settings });
        });
        return columns;
    }, [criteriaRowCount, fieldSettings, fieldsById, groupingEnabled, selectedFieldOrder, tablesById]);
    const outputColumnSensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 6 } }), useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }));
    const outputColumnRowDescriptors = useMemo(() => {
        const rows = [
            { key: 'field', label: 'Field', paddingY: 1 },
            { key: 'table', label: 'Table', paddingY: 1 }
        ];
        if (groupingEnabled) {
            rows.push({ key: 'group', label: 'Group By', paddingY: 0.75 });
        }
        rows.push({ key: 'sort', label: 'Sort', paddingY: 0.75 });
        rows.push({ key: 'show', label: 'Show', paddingY: 0.75, justifyContent: 'center' });
        for (let index = 0; index < criteriaRowCount; index += 1) {
            rows.push({ key: `criteria-${index}`, label: index === 0 ? 'Criteria' : 'Or', paddingY: 0.75 });
        }
        return rows;
    }, [criteriaRowCount, groupingEnabled]);
    const handleOutputColumnsDragEnd = useCallback((event) => {
        const { active, over } = event;
        if (!over || active.id === over.id) {
            return;
        }
        const activeId = String(active.id);
        const overId = String(over.id);
        setSelectedFieldOrder((current) => {
            const oldIndex = current.indexOf(activeId);
            const newIndex = current.indexOf(overId);
            if (oldIndex === -1 || newIndex === -1 || oldIndex === newIndex) {
                return current;
            }
            const next = arrayMove(current, oldIndex, newIndex);
            setStatusBanner({
                message: 'Reordered selected columns.',
                color: 'text.secondary'
            });
            return next;
        });
    }, [setSelectedFieldOrder, setStatusBanner]);
    const reportDefinition = useMemo(() => {
        const tableSnapshots = nodes.map((node) => {
            const table = tablesById.get(node.id);
            const width = node.width ?? (typeof node.style?.width === 'number' ? node.style.width : undefined);
            const height = node.height ?? (typeof node.style?.height === 'number' ? node.style.height : undefined);
            const dimensions = typeof width === 'number' || typeof height === 'number'
                ? {
                    ...(typeof width === 'number' ? { width } : {}),
                    ...(typeof height === 'number' ? { height } : {})
                }
                : undefined;
            return {
                tableId: node.id,
                label: table?.name ?? node.data.label,
                physicalName: table?.physicalName ?? node.data.subtitle ?? node.data.label,
                schemaName: table?.schemaName ?? null,
                alias: null,
                position: {
                    x: node.position.x,
                    y: node.position.y
                },
                ...(dimensions ? { dimensions } : {})
            };
        });
        const joinSnapshots = edges.map((edge) => {
            const edgeData = edge.data;
            return {
                id: edge.id ?? `${edge.source}-${edge.target}`,
                sourceTableId: edge.source,
                targetTableId: edge.target,
                sourceFieldId: edgeData?.sourceFieldId ?? null,
                targetFieldId: edgeData?.targetFieldId ?? null,
                joinType: edgeData?.joinType ?? 'inner'
            };
        });
        const columnSnapshots = selectedFieldColumns.map((column, index) => ({
            order: index,
            fieldId: column.fieldId,
            fieldName: column.field.name,
            fieldDescription: column.field.description ?? null,
            tableId: column.field.tableId,
            tableName: column.table?.name ?? null,
            show: column.settings.show,
            sort: column.settings.sort,
            aggregate: column.settings.aggregate ?? null,
            criteria: column.settings.criteria.map((value) => value.trim())
        }));
        return {
            tables: tableSnapshots,
            joins: joinSnapshots,
            columns: columnSnapshots,
            criteriaRowCount,
            groupingEnabled
        };
    }, [criteriaRowCount, edges, groupingEnabled, nodes, selectedFieldColumns, tablesById]);
    const handleSortChange = useCallback((fieldId, sort) => {
        setFieldSettings((current) => {
            const existing = current[fieldId];
            if (!existing || existing.sort === sort) {
                if (!existing && sort === 'none') {
                    return current;
                }
                if (existing && existing.sort === sort) {
                    return current;
                }
            }
            const base = existing ?? {
                show: true,
                sort: 'none',
                aggregate: groupingEnabled ? 'groupBy' : null,
                criteria: Array(criteriaRowCount).fill('')
            };
            return {
                ...current,
                [fieldId]: {
                    ...base,
                    aggregate: groupingEnabled ? base.aggregate ?? 'groupBy' : null,
                    sort
                }
            };
        });
    }, [criteriaRowCount, groupingEnabled]);
    const handleShowToggle = useCallback((fieldId, value) => {
        setFieldSettings((current) => {
            const existing = current[fieldId];
            if (!existing) {
                return {
                    ...current,
                    [fieldId]: {
                        show: value,
                        sort: 'none',
                        aggregate: groupingEnabled ? 'groupBy' : null,
                        criteria: Array(criteriaRowCount).fill('')
                    }
                };
            }
            if (existing.show === value) {
                return current;
            }
            return {
                ...current,
                [fieldId]: {
                    ...existing,
                    aggregate: groupingEnabled ? existing.aggregate ?? 'groupBy' : null,
                    show: value
                }
            };
        });
    }, [criteriaRowCount, groupingEnabled]);
    const handleCriteriaChange = useCallback((fieldId, index, value) => {
        setFieldSettings((current) => {
            const existing = current[fieldId];
            if (!existing) {
                const criteria = Array(criteriaRowCount).fill('');
                criteria[index] = value;
                return {
                    ...current,
                    [fieldId]: {
                        show: true,
                        sort: 'none',
                        aggregate: groupingEnabled ? 'groupBy' : null,
                        criteria
                    }
                };
            }
            if (existing.criteria[index] === value) {
                return current;
            }
            const criteria = existing.criteria.map((item, idx) => (idx === index ? value : item));
            return {
                ...current,
                [fieldId]: {
                    ...existing,
                    aggregate: groupingEnabled ? existing.aggregate ?? 'groupBy' : null,
                    criteria
                }
            };
        });
    }, [criteriaRowCount, groupingEnabled]);
    const handleAggregateChange = useCallback((fieldId, aggregate) => {
        if (!groupingEnabled) {
            return;
        }
        setFieldSettings((current) => {
            const existing = current[fieldId];
            if (!existing) {
                return {
                    ...current,
                    [fieldId]: {
                        show: true,
                        sort: 'none',
                        aggregate,
                        criteria: Array(criteriaRowCount).fill('')
                    }
                };
            }
            if (existing.aggregate === aggregate) {
                return current;
            }
            return {
                ...current,
                [fieldId]: {
                    ...existing,
                    aggregate
                }
            };
        });
    }, [criteriaRowCount, groupingEnabled]);
    const handleAddCriteriaRow = useCallback(() => {
        setCriteriaRowCount((count) => Math.min(count + 1, 5));
    }, []);
    const handleRemoveCriteriaRow = useCallback(() => {
        setCriteriaRowCount((count) => Math.max(1, count - 1));
    }, []);
    const toggleField = useCallback((fieldId) => {
        const field = fieldsById.get(fieldId);
        if (!field) {
            return;
        }
        const removing = selectedFieldSet.has(fieldId);
        setNodes((current) => current.map((node) => {
            if (node.id !== field.tableId) {
                return node;
            }
            const nodeData = node.data;
            const currentSelected = nodeData.selectedFieldIds ?? [];
            const nextSelected = removing
                ? currentSelected.filter((id) => id !== fieldId)
                : currentSelected.includes(fieldId)
                    ? currentSelected
                    : [...currentSelected, fieldId];
            if (currentSelected.length === nextSelected.length &&
                currentSelected.every((value, index) => value === nextSelected[index])) {
                return node;
            }
            return {
                ...node,
                data: {
                    ...nodeData,
                    selectedFieldIds: nextSelected,
                    onFieldAdd: ensureFieldAdded,
                    onFieldDragEnd: handleFieldDragEnd,
                    onFieldDragStart: handleFieldDragStart,
                    onFieldJoin: handleFieldJoin,
                    onFieldToggle: toggleField,
                    onRemoveTable: handleRemoveNode
                }
            };
        }));
        if (removing) {
            setSelectedFieldOrder((current) => current.filter((id) => id !== fieldId));
            setFieldSettings((current) => {
                if (!(fieldId in current)) {
                    return current;
                }
                const next = { ...current };
                delete next[fieldId];
                return next;
            });
            return;
        }
        ensureFieldAdded(fieldId);
    }, [ensureFieldAdded, fieldsById, handleFieldDragEnd, handleFieldDragStart, handleFieldJoin, handleRemoveNode, selectedFieldSet, setNodes]);
    useEffect(() => {
        setNodes((current) => {
            if (current.length === 0) {
                return current;
            }
            let mutated = false;
            const nextNodes = current.map((node) => {
                const nodeData = node.data;
                const tableFieldSet = new Set((fieldsByTable.get(node.id) ?? []).map((field) => field.id));
                const selectedIds = selectedFieldOrder.filter((fieldId) => tableFieldSet.has(fieldId));
                const existingSelected = nodeData.selectedFieldIds ?? [];
                const sameSelection = existingSelected.length === selectedIds.length && existingSelected.every((value, index) => value === selectedIds[index]);
                const sameAddHandler = nodeData.onFieldAdd === ensureFieldAdded;
                const sameToggleHandler = nodeData.onFieldToggle === toggleField;
                const sameJoinHandler = nodeData.onFieldJoin === handleFieldJoin;
                const sameDragStartHandler = nodeData.onFieldDragStart === handleFieldDragStart;
                const sameDragEndHandler = nodeData.onFieldDragEnd === handleFieldDragEnd;
                const sameRemoveHandler = nodeData.onRemoveTable === handleRemoveNode;
                if (sameSelection && sameAddHandler && sameToggleHandler && sameJoinHandler && sameDragStartHandler && sameDragEndHandler && sameRemoveHandler) {
                    return node;
                }
                mutated = true;
                return {
                    ...node,
                    data: {
                        ...nodeData,
                        selectedFieldIds: selectedIds,
                        onFieldAdd: ensureFieldAdded,
                        onFieldDragEnd: handleFieldDragEnd,
                        onFieldDragStart: handleFieldDragStart,
                        onFieldJoin: handleFieldJoin,
                        onFieldToggle: toggleField,
                        onRemoveTable: handleRemoveNode
                    }
                };
            });
            return mutated ? nextNodes : current;
        });
    }, [ensureFieldAdded, fieldsByTable, handleFieldDragEnd, handleFieldDragStart, handleFieldJoin, handleRemoveNode, nodes, selectedFieldOrder, setNodes, toggleField]);
    const selectedFieldCount = selectedFieldColumns.length;
    const actionButtonsDisabled = nodes.length === 0 || selectedFieldCount === 0;
    const handlePreviewResults = useCallback(async () => {
        setPreviewOpen(true);
        setPreviewLoading(true);
        setPreviewError(null);
        setPreviewData(null);
        try {
            const result = await fetchReportPreview(reportDefinition, 100);
            setPreviewData(result);
            setStatusBanner({
                message: `Preview ready. Displaying ${result.rowCount} row${result.rowCount === 1 ? '' : 's'} (limit ${result.limit}).`,
                color: 'success.main'
            });
        }
        catch (error) {
            const detail = isAxiosError(error)
                ? error.response?.data?.detail ?? error.message
                : error instanceof Error
                    ? error.message
                    : 'Unable to run the preview. Please try again.';
            setPreviewError(detail);
            setStatusBanner({
                message: 'Preview failed. Check the dialog for details.',
                color: 'error.main'
            });
        }
        finally {
            setPreviewLoading(false);
        }
    }, [reportDefinition, setStatusBanner]);
    const handleSaveDraft = useCallback(() => {
        if (typeof window === 'undefined') {
            setStatusBanner({
                message: 'Unable to persist draft outside a browser environment.',
                color: 'error.main'
            });
            return;
        }
        try {
            window.localStorage.setItem(designerStorageKey, JSON.stringify(reportDefinition));
            setStatusBanner({
                message: 'Draft stored locally. Replace this with a reporting API call once backend endpoints land.',
                color: 'success.main'
            });
        }
        catch (error) {
            console.error('Failed to persist reporting draft', error);
            setStatusBanner({
                message: 'Unable to persist draft locally. Refer to console for details.',
                color: 'error.main'
            });
        }
    }, [reportDefinition]);
    const handlePreviewClose = useCallback(() => {
        setPreviewOpen(false);
        setPreviewLoading(false);
        setPreviewError(null);
        setPreviewData(null);
    }, []);
    const handleCopyPreviewSql = useCallback(() => {
        if (!previewData?.sql) {
            return;
        }
        if (typeof navigator !== 'undefined' && navigator.clipboard) {
            navigator.clipboard.writeText(previewData.sql).then(() => {
                setStatusBanner({
                    message: 'Preview SQL copied to clipboard.',
                    color: 'success.main'
                });
            }).catch(() => {
                setStatusBanner({
                    message: 'Unable to copy preview SQL to clipboard.',
                    color: 'error.main'
                });
            });
        }
    }, [previewData, setStatusBanner]);
    const formatPreviewValue = useCallback((value) => {
        if (value === null || value === undefined) {
            return '—';
        }
        if (value instanceof Date) {
            return value.toISOString();
        }
        if (typeof value === 'object') {
            try {
                return JSON.stringify(value);
            }
            catch {
                return String(value);
            }
        }
        return String(value);
    }, []);
    const handlePublish = useCallback(() => {
        console.groupCollapsed('Reporting designer publish payload');
        console.info(reportDefinition);
        console.groupEnd();
        setStatusBanner({
            message: 'Publish workflow is not wired yet. Payload logged for integration.',
            color: 'text.secondary'
        });
    }, [reportDefinition]);
    const handleGroupingToggle = useCallback(() => {
        setGroupingEnabled((prev) => {
            const next = !prev;
            setFieldSettings((current) => {
                if (!next) {
                    let changed = false;
                    const reset = {};
                    Object.entries(current).forEach(([fieldId, existing]) => {
                        if (!existing) {
                            return;
                        }
                        if (existing.aggregate) {
                            changed = true;
                        }
                        reset[fieldId] = {
                            ...existing,
                            aggregate: null
                        };
                    });
                    if (!changed) {
                        return current;
                    }
                    return reset;
                }
                let changed = false;
                const seeded = { ...current };
                selectedFieldOrder.forEach((fieldId) => {
                    const existing = current[fieldId];
                    if (existing && existing.aggregate) {
                        return;
                    }
                    changed = true;
                    seeded[fieldId] = {
                        ...(existing ?? {
                            show: true,
                            sort: 'none',
                            criteria: Array(criteriaRowCount).fill('')
                        }),
                        aggregate: 'groupBy'
                    };
                });
                return changed ? seeded : current;
            });
            setStatusBanner({
                message: next
                    ? 'Grouping enabled. Configure aggregates for each output column.'
                    : 'Grouping disabled. Aggregates cleared from output columns.',
                color: 'text.secondary'
            });
            return next;
        });
    }, [criteriaRowCount, selectedFieldOrder, setFieldSettings, setStatusBanner]);
    const handleResetDraft = useCallback(() => {
        const proceed = typeof window === 'undefined'
            ? true
            : window.confirm('Clear the current reporting draft? This removes your local snapshot and resets the canvas.');
        if (!proceed) {
            return;
        }
        if (typeof window !== 'undefined') {
            window.localStorage.removeItem(designerStorageKey);
        }
        setNodes([]);
        setEdges([]);
        setSelectedFieldOrder([]);
        setFieldSettings({});
        setCriteriaRowCount(2);
        setGroupingEnabled(false);
        setPreviewOpen(false);
        setPreviewData(null);
        setPreviewError(null);
        setPreviewLoading(false);
        hasHydratedDraftRef.current = true;
        setStatusBanner({
            message: 'Draft cleared and canvas reset.',
            color: 'warning.main'
        });
    }, [setEdges, setFieldSettings, setGroupingEnabled, setNodes, setSelectedFieldOrder, setStatusBanner]);
    const handleDragStart = useCallback((event, table) => {
        const payload = { tableId: table.id };
        event.dataTransfer.setData(tableDragMimeType, JSON.stringify(payload));
        event.dataTransfer.effectAllowed = 'move';
    }, []);
    const handleDragOver = useCallback((event) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
    }, []);
    const handleDrop = useCallback((event) => {
        event.preventDefault();
        const raw = event.dataTransfer.getData(tableDragMimeType);
        if (!raw) {
            return;
        }
        let parsed = null;
        try {
            parsed = JSON.parse(raw);
        }
        catch (error) {
            return;
        }
        if (!parsed?.tableId) {
            return;
        }
        const table = tables.find((item) => item.id === parsed.tableId);
        if (!table || !reactFlowWrapper.current) {
            return;
        }
        const instance = reactFlowInstanceRef.current;
        if (!instance) {
            return;
        }
        const flowPoint = typeof instance.screenToFlowPosition === 'function'
            ? instance.screenToFlowPosition({ x: event.clientX, y: event.clientY })
            : instance.project({
                x: event.clientX - reactFlowWrapper.current.getBoundingClientRect().left,
                y: event.clientY - reactFlowWrapper.current.getBoundingClientRect().top
            });
        const node = addTableNode(table, flowPoint);
        focusNode(node);
    }, [addTableNode, focusNode, tables]);
    const handleOutputDragOver = useCallback((event) => {
        const types = Array.from(event.dataTransfer?.types ?? []);
        if (!types.includes(REPORTING_FIELD_DRAG_TYPE)) {
            return;
        }
        event.preventDefault();
        event.dataTransfer.dropEffect = 'copy';
        setOutputDropActive(true);
    }, []);
    const handleOutputDragLeave = useCallback(() => {
        setOutputDropActive(false);
    }, []);
    const handleFieldDrop = useCallback((event) => {
        event.preventDefault();
        setOutputDropActive(false);
        const raw = event.dataTransfer.getData(REPORTING_FIELD_DRAG_TYPE);
        if (!raw) {
            return;
        }
        let parsed = null;
        try {
            parsed = JSON.parse(raw);
        }
        catch (error) {
            return;
        }
        if (!parsed?.fieldId) {
            return;
        }
        ensureFieldAdded(parsed.fieldId);
    }, [ensureFieldAdded]);
    return (_jsxs(Box, { sx: { display: 'flex', flexDirection: 'column', gap: 3, minHeight: '100vh' }, children: [_jsxs(Box, { children: [_jsx(Typography, { variant: "h4", sx: { fontWeight: 600, mb: 1 }, children: "Reporting Designer" }), _jsx(Typography, { variant: "body1", color: "text.secondary", children: "Compose relational-style report definitions by combining enterprise tables, mapping joins, and shaping output fields prior to publishing." })] }), _jsxs(Paper, { elevation: 2, sx: { p: 2.5, flex: 1, display: 'flex', flexDirection: 'column', gap: 2, minHeight: 0 }, children: [_jsxs(Stack, { direction: { xs: 'column', lg: 'row' }, spacing: 2, alignItems: { xs: 'stretch', lg: 'flex-start' }, sx: { flex: 1, minHeight: 0 }, children: [_jsxs(Box, { sx: { width: { xs: '100%', lg: 300 }, flexShrink: 0, display: 'flex', flexDirection: 'column', gap: 2 }, children: [_jsx(Typography, { variant: "subtitle1", sx: { fontWeight: 600 }, children: "Table Palette" }), _jsx(TextField, { value: searchTerm, onChange: (event) => setSearchTerm(event.target.value), size: "small", placeholder: "Search tables", InputProps: {
                                            startAdornment: (_jsx(InputAdornment, { position: "start", children: _jsx(SearchIcon, { fontSize: "small" }) }))
                                        } }), _jsxs(Paper, { variant: "outlined", sx: { flex: 1, minHeight: 280, overflowY: 'auto' }, children: [tablesLoading && (_jsxs(Stack, { sx: { py: 6 }, alignItems: "center", spacing: 1, children: [_jsx(CircularProgress, { size: 24 }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Loading tables\u2026" })] })), tablesError && (_jsx(Stack, { sx: { py: 6, px: 2 }, spacing: 1, children: _jsx(Typography, { variant: "body2", color: "error", children: "Unable to load table catalog. Verify backend availability." }) })), !tablesLoading && !tablesError && (_jsxs(List, { dense: true, disablePadding: true, children: [filteredTables.length === 0 && (_jsx(ListItem, { children: _jsx(ListItemText, { primary: "No matching tables", primaryTypographyProps: { variant: 'body2', color: 'text.secondary' } }) })), filteredTables.map((item) => {
                                                        const alreadySelected = nodesById.has(item.id);
                                                        const subtitle = item.schemaName ? `${item.schemaName}.${item.physicalName}` : item.physicalName;
                                                        return (_jsx(ListItem, { secondaryAction: _jsx(Tooltip, { title: alreadySelected ? 'Already on canvas' : 'Add to canvas', children: _jsx("span", { children: _jsx(IconButton, { size: "small", color: "primary", disabled: alreadySelected, onClick: () => handlePaletteAdd(item), children: _jsx(AddCircleOutlineIcon, { fontSize: "small" }) }) }) }), children: _jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", draggable: !alreadySelected, onDragStart: (event) => handleDragStart(event, item), sx: { cursor: alreadySelected ? 'default' : 'grab', flex: 1 }, children: [_jsx(DragIndicatorIcon, { fontSize: "small", color: alreadySelected ? 'disabled' : 'action' }), _jsx(ListItemText, { primary: item.name, secondary: subtitle, primaryTypographyProps: { fontWeight: 600, fontSize: 14 }, secondaryTypographyProps: { fontSize: 12 } })] }) }, item.id));
                                                    })] }))] })] }), _jsx(Divider, { flexItem: true, orientation: "vertical", sx: { display: { xs: 'none', lg: 'block' } } }), _jsxs(Box, { sx: {
                                    flex: 1,
                                    display: 'flex',
                                    flexDirection: 'column',
                                    gap: 2,
                                    minHeight: { xs: 320, md: 360 },
                                    minWidth: 0
                                }, children: [_jsx(Typography, { variant: "subtitle1", sx: { fontWeight: 600 }, children: "Relationship Canvas" }), _jsx(Paper, { variant: "outlined", sx: {
                                            minHeight: 280,
                                            height: { xs: '48vh', md: '56vh', lg: '60vh' },
                                            borderRadius: 2,
                                            overflow: 'hidden',
                                            position: 'relative'
                                        }, ref: reactFlowWrapper, onDrop: handleDrop, onDragOver: handleDragOver, children: _jsxs(ReactFlow, { nodes: nodes, edges: edges, onNodesChange: onNodesChange, onEdgesChange: onEdgesChange, onConnect: handleConnect, onEdgeClick: handleEdgeClick, nodeTypes: nodeTypes, fitView: true, minZoom: 0.4, style: { width: '100%', height: '100%', background: '#f5f7fb' }, onInit: (instance) => { reactFlowInstanceRef.current = instance; }, children: [_jsx(MiniMap, { pannable: true, zoomable: true }), _jsx(Controls, { position: "bottom-right" }), _jsx(Background, { gap: 16, size: 0.6 })] }) }), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 1, children: [_jsx(Button, { variant: "contained", startIcon: _jsx(PlayArrowIcon, {}), disabled: actionButtonsDisabled, onClick: handlePreviewResults, children: "Preview Results" }), _jsx(Button, { variant: "outlined", startIcon: _jsx(SaveIcon, {}), disabled: actionButtonsDisabled, onClick: handleSaveDraft, children: "Save Draft" }), _jsx(Button, { variant: "outlined", startIcon: _jsx(PublishIcon, {}), disabled: actionButtonsDisabled, onClick: handlePublish, children: "Publish to Data Object" }), _jsx(Button, { variant: "text", color: "warning", startIcon: _jsx(RestartAltIcon, {}), onClick: handleResetDraft, children: "Reset Draft" })] }), statusBanner && (_jsx(Typography, { variant: "caption", sx: { color: statusBanner.color, mt: 0.5 }, children: statusBanner.message }))] })] }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle1", sx: { fontWeight: 600, mb: 1 }, children: "Output Fields" }), _jsx(Paper, { variant: "outlined", sx: { p: nodes.length === 0 ? 3 : 0, minHeight: 320 }, children: nodes.length === 0 ? (_jsx(Stack, { spacing: 1, alignItems: "center", justifyContent: "center", sx: { height: '100%' }, children: _jsx(Typography, { variant: "body2", color: "text.secondary", align: "center", children: "Add tables to the canvas to begin defining result columns." }) })) : (_jsxs(Box, { sx: { p: 2 }, children: [_jsxs(Stack, { direction: "row", alignItems: "center", justifyContent: "space-between", sx: { mb: 1 }, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600 }, children: "Selected Output Columns" }), _jsx(Button, { variant: groupingEnabled ? 'contained' : 'outlined', color: "secondary", size: "small", onClick: handleGroupingToggle, disabled: selectedFieldColumns.length === 0, children: groupingEnabled ? 'Disable Group By' : 'Enable Group By' })] }), _jsx(Box, { sx: {
                                                mt: 1,
                                                overflowX: 'auto',
                                                outline: outputDropActive ? `2px dashed ${theme.palette.secondary.main}` : 'none',
                                                outlineOffset: 6,
                                                borderRadius: 1,
                                                transition: 'outline-color 120ms ease'
                                            }, onDragOver: handleOutputDragOver, onDragLeave: handleOutputDragLeave, onDrop: handleFieldDrop, children: selectedFieldColumns.length === 0 ? (_jsxs(Stack, { spacing: 1, alignItems: "center", justifyContent: "center", sx: { py: 6 }, children: [_jsx(Typography, { variant: "body2", color: "text.secondary", align: "center", children: "Use the Relationship Canvas to add columns or drag fields here to seed the result set." }), _jsx(Typography, { variant: "caption", color: "text.secondary", align: "center", children: "Tip: click the selector in any table node to toggle a column." })] })) : (_jsxs(_Fragment, { children: [_jsxs(Box, { sx: { display: 'inline-flex', minWidth: '100%' }, children: [_jsx(Box, { sx: {
                                                                    width: 140,
                                                                    flexShrink: 0,
                                                                    border: `1px solid ${theme.palette.divider}`,
                                                                    borderRight: `1px solid ${theme.palette.divider}`,
                                                                    borderRadius: '8px 0 0 8px',
                                                                    overflow: 'hidden',
                                                                    backgroundColor: alpha(theme.palette.primary.light, 0.12)
                                                                }, children: outputColumnRowDescriptors.map((row, index) => (_jsx(Box, { sx: {
                                                                        px: 1.25,
                                                                        py: row.paddingY,
                                                                        display: 'flex',
                                                                        alignItems: row.justifyContent === 'center' ? 'center' : 'flex-start',
                                                                        justifyContent: row.justifyContent ?? 'flex-start',
                                                                        fontWeight: 600,
                                                                        borderBottom: index === outputColumnRowDescriptors.length - 1
                                                                            ? 'none'
                                                                            : `1px solid ${theme.palette.divider}`
                                                                    }, children: row.label }, row.key))) }), _jsx(DndContext, { sensors: outputColumnSensors, collisionDetection: closestCenter, onDragEnd: handleOutputColumnsDragEnd, children: _jsx(SortableContext, { items: selectedFieldColumns.map((column) => column.fieldId), strategy: horizontalListSortingStrategy, children: _jsx(Box, { sx: {
                                                                            display: 'flex',
                                                                            flex: 1,
                                                                            border: `1px solid ${theme.palette.divider}`,
                                                                            borderLeft: 'none',
                                                                            borderRadius: '0 8px 8px 0',
                                                                            overflow: 'hidden',
                                                                            backgroundColor: alpha(theme.palette.background.paper, 0.85)
                                                                        }, children: selectedFieldColumns.map((column, index) => (_jsx(SortableOutputColumn, { column: column, isFirst: index === 0, isLast: index === selectedFieldColumns.length - 1, groupingEnabled: groupingEnabled, aggregateOptions: aggregateOptions, rowDescriptors: outputColumnRowDescriptors, criteriaRowCount: criteriaRowCount, onRemove: toggleField, onSortChange: handleSortChange, onAggregateChange: handleAggregateChange, onShowToggle: handleShowToggle, onCriteriaChange: handleCriteriaChange }, column.fieldId))) }) }) })] }), _jsxs(Stack, { direction: "row", spacing: 1, sx: { mt: 1 }, children: [_jsx(Button, { variant: "text", size: "small", onClick: handleAddCriteriaRow, children: "Add criteria row" }), criteriaRowCount > 1 && (_jsx(Button, { variant: "text", size: "small", onClick: handleRemoveCriteriaRow, children: "Remove last criteria row" }))] })] })) })] })) })] })] }), _jsxs(Paper, { elevation: 0, variant: "outlined", sx: { p: 2.5 }, children: [_jsx(Typography, { variant: "subtitle1", sx: { fontWeight: 600, mb: 1 }, children: "Upcoming Capabilities" }), _jsxs(List, { dense: true, children: [_jsx(ListItem, { children: _jsx(ListItemText, { primary: "Drag tables directly onto the canvas with relationship-aware placement." }) }), _jsx(ListItem, { children: _jsx(ListItemText, { primary: "Author joins and filters, then preview dataset results inline." }) }), _jsx(ListItem, { children: _jsx(ListItemText, { primary: "Curate output columns with sorting, grouping, and aggregation controls." }) }), _jsx(ListItem, { children: _jsx(ListItemText, { primary: "Persist report definitions, manage versions, and publish to data objects." }) })] })] }), _jsxs(Dialog, { open: Boolean(joinDialogState), onClose: handleJoinDialogClose, maxWidth: "sm", fullWidth: true, children: [_jsx(DialogTitle, { sx: { pb: 1.5 }, children: _jsx(Typography, { variant: "h6", sx: { fontWeight: 600 }, children: joinDialogTitleText }) }), joinDialogState && (_jsxs(_Fragment, { children: [_jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 3, children: [_jsx(Typography, { variant: "body2", color: "text.secondary", children: joinDialogDescription }), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 2, children: [_jsxs(Box, { sx: { flex: 1 }, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600, mb: 1 }, children: "Source" }), _jsx(TextField, { label: "Source Table", size: "small", value: joinDialogSourceName, InputProps: { readOnly: true }, fullWidth: true }), _jsx(TextField, { select: true, label: "Source Field", size: "small", sx: { mt: 1.5 }, value: joinDialogState.sourceFieldId, onChange: (event) => handleJoinDialogFieldChange('source', event.target.value), fullWidth: true, children: (fieldsByTable.get(joinDialogState.sourceTableId) ?? []).map((field) => (_jsx(MenuItem, { value: field.id, children: field.name }, field.id))) })] }), _jsxs(Box, { sx: { flex: 1 }, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600, mb: 1 }, children: "Target" }), _jsx(TextField, { label: "Target Table", size: "small", value: joinDialogTargetName, InputProps: { readOnly: true }, fullWidth: true }), _jsx(TextField, { select: true, label: "Target Field", size: "small", sx: { mt: 1.5 }, value: joinDialogState.targetFieldId, onChange: (event) => handleJoinDialogFieldChange('target', event.target.value), fullWidth: true, children: (fieldsByTable.get(joinDialogState.targetTableId) ?? []).map((field) => (_jsx(MenuItem, { value: field.id, children: field.name }, field.id))) })] })] }), _jsxs(FormControl, { component: "fieldset", sx: { width: '100%' }, children: [_jsx(FormLabel, { component: "legend", sx: { fontWeight: 600, mb: 0.5 }, children: "Join Type" }), _jsx(RadioGroup, { value: joinDialogState.joinType, onChange: (event) => handleJoinDialogTypeChange(event.target.value), children: joinTypeOptions.map((option) => (_jsx(FormControlLabel, { value: option.value, control: _jsx(Radio, { size: "small" }), label: (_jsxs(Box, { children: [_jsx(Typography, { variant: "body2", sx: { fontWeight: 600 }, children: option.label }), _jsx(Typography, { variant: "caption", color: "text.secondary", children: option.helper })] })), sx: { alignItems: 'flex-start', mr: 0, mt: 0.25 } }, option.value))) })] })] }) }), _jsxs(DialogActions, { sx: { px: 2.5, py: 1.5 }, children: [joinDialogIsEdit && (_jsx(Button, { color: "error", onClick: handleJoinDialogDelete, children: "Remove Join" })), _jsx(Button, { onClick: handleJoinDialogClose, children: "Cancel" }), _jsx(Button, { onClick: handleJoinDialogConfirm, variant: "contained", disabled: !canConfirmJoin, children: joinDialogConfirmLabel })] })] }))] }), _jsxs(Dialog, { open: previewOpen, onClose: handlePreviewClose, maxWidth: "lg", fullWidth: true, children: [_jsx(DialogTitle, { sx: { pb: 1.5 }, children: _jsxs(Stack, { direction: "row", alignItems: "center", justifyContent: "space-between", children: [_jsx(Typography, { variant: "h6", sx: { fontWeight: 600 }, children: "Preview Results" }), _jsx(Tooltip, { title: "Copy SQL", children: _jsx("span", { children: _jsx(IconButton, { size: "small", onClick: handleCopyPreviewSql, disabled: !previewData?.sql || previewLoading, children: _jsx(ContentCopyIcon, { fontSize: "small" }) }) }) })] }) }), _jsx(DialogContent, { dividers: true, children: previewLoading ? (_jsx(Box, { sx: { py: 6, display: 'flex', alignItems: 'center', justifyContent: 'center' }, children: _jsx(CircularProgress, { size: 32 }) })) : previewError ? (_jsx(Alert, { severity: "error", children: previewError })) : previewData ? (_jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: "row", spacing: 1, flexWrap: "wrap", useFlexGap: true, children: [_jsx(Chip, { label: `Rows: ${previewData.rowCount}`, size: "small" }), _jsx(Chip, { label: `Limit: ${previewData.limit}`, size: "small" }), _jsx(Chip, { label: `Duration: ${previewData.durationMs.toFixed(1)} ms`, size: "small" })] }), _jsx(TableContainer, { component: Paper, variant: "outlined", sx: { maxHeight: 360 }, children: _jsxs(MuiTable, { size: "small", stickyHeader: true, children: [_jsx(MuiTableHead, { children: _jsx(MuiTableRow, { children: previewData.columns.map((column) => (_jsx(MuiTableCell, { sx: { fontWeight: 600 }, children: column }, column))) }) }), _jsx(MuiTableBody, { children: previewData.rows.length === 0 ? (_jsx(MuiTableRow, { children: _jsx(MuiTableCell, { colSpan: Math.max(previewData.columns.length, 1), align: "center", children: _jsx(Typography, { variant: "body2", color: "text.secondary", children: "No rows returned for the current selection." }) }) })) : (previewData.rows.map((row, rowIndex) => (_jsx(MuiTableRow, { hover: true, children: previewData.columns.map((column) => {
                                                        const cellValue = row[column];
                                                        return (_jsx(MuiTableCell, { sx: { whiteSpace: 'nowrap', maxWidth: 240 }, children: formatPreviewValue(cellValue) }, `${rowIndex}-${column}`));
                                                    }) }, rowIndex)))) })] }) }), _jsxs(Box, { children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600, mb: 0.5 }, children: "Generated SQL" }), _jsx(Box, { component: "pre", sx: {
                                                whiteSpace: 'pre-wrap',
                                                wordBreak: 'break-word',
                                                fontFamily: 'Roboto Mono, monospace',
                                                fontSize: 13,
                                                m: 0,
                                                p: 1.5,
                                                backgroundColor: alpha(theme.palette.primary.light, 0.08),
                                                borderRadius: 1
                                            }, children: previewData.sql })] })] })) : (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "Configure tables, fields, and joins, then select \u201CPreview Results\u201D to fetch a sample of the dataset." })) }), _jsx(DialogActions, { sx: { px: 2.5, py: 1.5 }, children: _jsx(Button, { onClick: handlePreviewClose, variant: "outlined", children: "Close" }) })] })] }));
};
const ReportingDesignerPage = () => (_jsx(ReactFlowProvider, { children: _jsx(ReportingDesignerContent, {}) }));
export default ReportingDesignerPage;
