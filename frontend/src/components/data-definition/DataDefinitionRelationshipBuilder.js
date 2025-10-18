import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { createContext, memo, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { Background, Controls, Handle, MiniMap, Position, ReactFlow, ReactFlowProvider, useReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Button, Chip, Collapse, Dialog, DialogActions, DialogContent, DialogTitle, Divider, FormControl, IconButton, InputLabel, List, ListItem, ListItemText, MenuItem, Paper, Select, Stack, TextField, Tooltip, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import LinkIcon from '@mui/icons-material/Link';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ConfirmDialog from '../common/ConfirmDialog';
import { useToast } from '../../hooks/useToast';
const RelationshipHoverContext = createContext({
    hoveredFieldId: null,
    connectionHandleId: null,
    setHoveredFieldId: () => { }
});
const useRelationshipHover = () => useContext(RelationshipHoverContext);
const relationshipTypeOptions = [
    { value: 'one_to_one', label: 'One to One' },
    { value: 'one_to_many', label: 'One to Many' },
    { value: 'many_to_one', label: 'Many to One' },
    { value: 'many_to_many', label: 'Many to Many' }
];
const relationshipTypeLabel = (value) => {
    const match = relationshipTypeOptions.find((option) => option.value === value);
    return match ? match.label : value;
};
const TableNode = memo(({ data }) => {
    const { table, canEdit, disabled } = data;
    const { hoveredFieldId, connectionHandleId, setHoveredFieldId } = useRelationshipHover();
    const tableName = table.alias || table.table.name;
    const physicalName = table.table.schemaName
        ? `${table.table.schemaName}.${table.table.physicalName}`
        : table.table.physicalName;
    const activeHighlightId = hoveredFieldId ?? connectionHandleId;
    const [dragOverFieldId, setDragOverFieldId] = useState(null);
    const [isDraggingConnection, setIsDraggingConnection] = useState(false);
    return (_jsxs(Paper, { variant: "outlined", sx: { p: 1.5, minWidth: 240 }, children: [_jsxs(Stack, { spacing: 0.5, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 600 }, children: tableName }), _jsx(Typography, { variant: "caption", color: "text.secondary", children: physicalName })] }), _jsx(Divider, { sx: { my: 1 } }), _jsx(Stack, { spacing: 0.75, onMouseEnter: () => setIsDraggingConnection(false), children: table.fields.length === 0 ? (_jsx(Typography, { variant: "caption", color: "text.secondary", children: "No fields in this definition." })) : (table.fields.map((definitionField) => {
                    const isHighlighted = activeHighlightId === definitionField.id;
                    const isDragOver = dragOverFieldId === definitionField.id;
                    return (_jsxs(Box, { sx: {
                            position: 'relative',
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            py: 0.75,
                            pl: 3,
                            pr: 3,
                            borderRadius: 1,
                            cursor: canEdit && !disabled ? 'grab' : 'default',
                            transition: 'all 120ms ease',
                            backgroundColor: isHighlighted || isDragOver ? '#e3f2fd' : 'transparent',
                            boxShadow: isDragOver
                                ? `inset 0 0 0 2px #1976d2, 0 0 8px rgba(25, 118, 210, 0.3)`
                                : isHighlighted
                                    ? `inset 0 0 0 1px #1976d2`
                                    : 'none',
                            border: isDragOver ? `2px solid #1976d2` : 'none',
                            '&:active': { cursor: 'grabbing' }
                        }, onMouseEnter: () => {
                            if (!canEdit || disabled)
                                return;
                            setHoveredFieldId(definitionField.id);
                        }, onMouseLeave: () => {
                            if (!canEdit || disabled)
                                return;
                            setHoveredFieldId(null);
                            setDragOverFieldId(null);
                        }, children: [_jsx(Handle, { type: "target", position: Position.Left, id: definitionField.id, isConnectable: canEdit && !disabled, onConnect: () => setIsDraggingConnection(true), style: {
                                    background: 'transparent',
                                    width: '100%',
                                    height: '100%',
                                    left: -15,
                                    top: 0,
                                    borderRadius: 0,
                                    border: 'none',
                                    transform: 'none',
                                    pointerEvents: canEdit && !disabled ? 'auto' : 'none'
                                } }), _jsx(Box, { sx: {
                                    position: 'absolute',
                                    left: 2,
                                    width: 12,
                                    height: 12,
                                    borderRadius: '50%',
                                    backgroundColor: isHighlighted || isDragOver ? '#1565c0' : '#1976d2',
                                    boxShadow: isHighlighted || isDragOver ? '0 0 8px rgba(21, 101, 192, 0.6)' : 'none',
                                    transition: 'all 120ms ease',
                                    cursor: canEdit && !disabled ? 'crosshair' : 'default'
                                } }), _jsx(Typography, { variant: "body2", sx: {
                                    flexGrow: 1,
                                    fontWeight: isHighlighted || isDragOver ? 600 : 400,
                                    transition: 'font-weight 120ms ease'
                                }, children: definitionField.field.name }), _jsx(Handle, { type: "source", position: Position.Right, id: definitionField.id, isConnectable: canEdit && !disabled, onConnect: () => setIsDraggingConnection(true), style: {
                                    background: 'transparent',
                                    width: '100%',
                                    height: '100%',
                                    right: -15,
                                    top: 0,
                                    borderRadius: 0,
                                    border: 'none',
                                    transform: 'none',
                                    pointerEvents: canEdit && !disabled ? 'auto' : 'none'
                                } }), _jsx(Box, { sx: {
                                    position: 'absolute',
                                    right: 2,
                                    width: 12,
                                    height: 12,
                                    borderRadius: '50%',
                                    backgroundColor: isHighlighted || isDragOver ? '#6a1b9a' : '#9c27b0',
                                    boxShadow: isHighlighted || isDragOver ? '0 0 8px rgba(106, 27, 154, 0.6)' : 'none',
                                    transition: 'all 120ms ease',
                                    cursor: canEdit && !disabled ? 'crosshair' : 'default'
                                } })] }, definitionField.fieldId));
                })) })] }));
});
TableNode.displayName = 'TableNode';
const nodeTypes = { table: TableNode };
const sanitizeNotes = (value) => {
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
};
const RelationshipFlow = memo(({ nodes, edges, canEdit, disabled, onConnect, onEdgeClick, onConnectStart, onConnectEnd }) => {
    const { fitView } = useReactFlow();
    useEffect(() => {
        const timer = setTimeout(() => {
            fitView({ padding: 0.2, duration: 300 });
        }, 50);
        return () => clearTimeout(timer);
    }, [fitView, nodes.length]);
    return (_jsxs(ReactFlow, { nodes: nodes, edges: edges, nodeTypes: nodeTypes, fitView: true, nodesDraggable: canEdit && !disabled, nodesConnectable: canEdit && !disabled, onConnect: onConnect, onEdgeClick: (event, edge) => {
            event.preventDefault();
            onEdgeClick(edge);
        }, onConnectStart: onConnectStart, onConnectEnd: onConnectEnd, elementsSelectable: canEdit && !disabled, panOnScroll: true, panOnDrag: [2, 4], minZoom: 0.4, maxZoom: 1.5, children: [_jsx(MiniMap, { pannable: true, zoomable: true, style: { height: 100 } }), _jsx(Controls, { showInteractive: false }), _jsx(Background, { gap: 24, size: 1 })] }));
});
RelationshipFlow.displayName = 'RelationshipFlow';
const DataDefinitionRelationshipBuilder = ({ tables, relationships, canEdit, busy = false, onCreateRelationship, onUpdateRelationship, onDeleteRelationship, initialPrimaryFieldId, initialForeignFieldId }) => {
    const toast = useToast();
    const [expanded, setExpanded] = useState(false);
    const [dialogState, setDialogState] = useState(null);
    const [dialogSubmitting, setDialogSubmitting] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [hoveredFieldId, setHoveredFieldId] = useState(null);
    const [connectionHandleId, setConnectionHandleId] = useState(null);
    const theme = useTheme();
    const hoverContextValue = useMemo(() => ({
        hoveredFieldId,
        connectionHandleId,
        setHoveredFieldId
    }), [hoveredFieldId, connectionHandleId]);
    const fieldLookup = useMemo(() => {
        const map = new Map();
        tables.forEach((table) => {
            table.fields.forEach((definitionField) => {
                map.set(definitionField.id, { definitionField, table });
            });
        });
        return map;
    }, [tables]);
    const handleLabel = useCallback((fieldId) => {
        const entry = fieldLookup.get(fieldId);
        if (!entry) {
            return 'Unknown field';
        }
        const { definitionField, table } = entry;
        const tableName = table.alias || table.table.name;
        return `${tableName}.${definitionField.field.name}`;
    }, [fieldLookup]);
    // Initialize dialog with provided field IDs
    useEffect(() => {
        if (initialPrimaryFieldId && initialForeignFieldId) {
            setExpanded(true);
            setDialogState({
                mode: 'create',
                primaryFieldId: initialPrimaryFieldId,
                foreignFieldId: initialForeignFieldId,
                relationshipType: 'one_to_one',
                notes: ''
            });
        }
    }, [initialPrimaryFieldId, initialForeignFieldId]);
    const nodes = useMemo(() => {
        if (!tables.length) {
            return [];
        }
        const columns = Math.max(1, Math.min(3, Math.ceil(Math.sqrt(tables.length))));
        const horizontalGap = 320;
        const verticalGap = 260;
        return tables.map((table, index) => {
            const column = index % columns;
            const row = Math.floor(index / columns);
            return {
                id: table.id,
                type: 'table',
                position: { x: column * horizontalGap, y: row * verticalGap },
                data: {
                    table,
                    canEdit,
                    disabled: busy
                }
            };
        });
    }, [tables, canEdit, busy]);
    const edges = useMemo(() => relationships.map((relationship) => ({
        id: relationship.id,
        source: relationship.primaryTableId,
        target: relationship.foreignTableId,
        sourceHandle: relationship.primaryFieldId,
        targetHandle: relationship.foreignFieldId,
        label: relationshipTypeLabel(relationship.relationshipType),
        animated: false,
        type: 'smoothstep',
        style: { strokeWidth: 2 },
        labelStyle: {
            fill: '#374151',
            fontWeight: 600
        }
    })), [relationships]);
    const openCreateDialog = useCallback((connection) => {
        if (!connection.sourceHandle || !connection.targetHandle) {
            return;
        }
        setDialogState({
            mode: 'create',
            primaryFieldId: connection.sourceHandle,
            foreignFieldId: connection.targetHandle,
            relationshipType: 'one_to_one',
            notes: ''
        });
    }, []);
    const openEditDialog = useCallback((relationship) => {
        setDialogState({
            mode: 'edit',
            relationshipId: relationship.id,
            primaryFieldId: relationship.primaryFieldId,
            foreignFieldId: relationship.foreignFieldId,
            relationshipType: relationship.relationshipType,
            notes: relationship.notes ?? ''
        });
    }, []);
    const handleConnect = useCallback((connection) => {
        if (!canEdit || busy) {
            return;
        }
        if (!connection.source || !connection.target || !connection.sourceHandle || !connection.targetHandle) {
            return;
        }
        if (connection.source === connection.target) {
            toast.showError('Choose fields from different tables.');
            return;
        }
        if (connection.sourceHandle === connection.targetHandle) {
            toast.showError('Primary and foreign fields must be different.');
            return;
        }
        const exists = relationships.some((relationship) => relationship.primaryFieldId === connection.sourceHandle &&
            relationship.foreignFieldId === connection.targetHandle);
        if (exists) {
            toast.showInfo('This relationship already exists.');
            return;
        }
        openCreateDialog(connection);
    }, [busy, canEdit, openCreateDialog, relationships, toast]);
    const handleEdgeClick = useCallback((edge) => {
        if (!canEdit || busy) {
            return;
        }
        const relationship = relationships.find((item) => item.id === edge.id);
        if (relationship) {
            openEditDialog(relationship);
        }
    }, [busy, canEdit, openEditDialog, relationships]);
    const handleConnectStart = useCallback((_event, params) => {
        if (!canEdit || busy) {
            return;
        }
        setConnectionHandleId(params.handleId ?? null);
    }, [busy, canEdit]);
    const handleConnectEnd = useCallback(() => {
        setConnectionHandleId(null);
        setHoveredFieldId(null);
    }, []);
    const closeDialog = useCallback(() => {
        setDialogState(null);
    }, []);
    const handleSubmitDialog = useCallback(async () => {
        if (!dialogState) {
            return;
        }
        setDialogSubmitting(true);
        try {
            if (dialogState.mode === 'create') {
                if (!onCreateRelationship) {
                    return;
                }
                const success = await onCreateRelationship({
                    primaryFieldId: dialogState.primaryFieldId,
                    foreignFieldId: dialogState.foreignFieldId,
                    relationshipType: dialogState.relationshipType,
                    notes: sanitizeNotes(dialogState.notes) ?? undefined
                });
                if (success) {
                    setDialogState(null);
                }
            }
            else if (dialogState.relationshipId) {
                if (!onUpdateRelationship) {
                    return;
                }
                const success = await onUpdateRelationship(dialogState.relationshipId, {
                    relationshipType: dialogState.relationshipType,
                    notes: sanitizeNotes(dialogState.notes)
                });
                if (success) {
                    setDialogState(null);
                }
            }
        }
        finally {
            setDialogSubmitting(false);
        }
    }, [dialogState, onCreateRelationship, onUpdateRelationship]);
    const handleDelete = useCallback(async () => {
        if (!deleteTarget || !onDeleteRelationship || busy) {
            return;
        }
        const success = await onDeleteRelationship(deleteTarget.id);
        if (success) {
            setDeleteTarget(null);
        }
    }, [busy, deleteTarget, onDeleteRelationship]);
    const handleDeleteFromDialog = useCallback(async () => {
        if (!dialogState ||
            dialogState.mode !== 'edit' ||
            !dialogState.relationshipId ||
            !onDeleteRelationship ||
            busy) {
            return;
        }
        setDialogSubmitting(true);
        try {
            const success = await onDeleteRelationship(dialogState.relationshipId);
            if (success) {
                setDialogState(null);
            }
        }
        finally {
            setDialogSubmitting(false);
        }
    }, [busy, dialogState, onDeleteRelationship]);
    const toggleExpanded = useCallback(() => {
        setExpanded((prev) => !prev);
    }, []);
    const renderRelationshipDialog = () => {
        if (!dialogState) {
            return null;
        }
        const primaryLabel = handleLabel(dialogState.primaryFieldId);
        const foreignLabel = handleLabel(dialogState.foreignFieldId);
        const handleTypeChange = (event) => {
            const value = event.target.value;
            setDialogState((prev) => (prev ? { ...prev, relationshipType: value } : prev));
        };
        return (_jsxs(Dialog, { open: true, onClose: dialogSubmitting ? undefined : closeDialog, fullWidth: true, maxWidth: "sm", children: [_jsx(DialogTitle, { children: dialogState.mode === 'create' ? 'Create Relationship' : 'Edit Relationship' }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { spacing: 0.5, children: [_jsx(Typography, { variant: "caption", color: "text.secondary", children: "Primary Field" }), _jsx(Typography, { variant: "body2", children: primaryLabel })] }), _jsxs(Stack, { spacing: 0.5, children: [_jsx(Typography, { variant: "caption", color: "text.secondary", children: "Foreign Field" }), _jsx(Typography, { variant: "body2", children: foreignLabel })] }), _jsxs(FormControl, { size: "small", children: [_jsx(InputLabel, { id: "relationship-type-label", children: "Relationship Type" }), _jsx(Select, { labelId: "relationship-type-label", value: dialogState.relationshipType, label: "Relationship Type", onChange: handleTypeChange, children: relationshipTypeOptions.map((option) => (_jsx(MenuItem, { value: option.value, children: option.label }, option.value))) })] }), _jsx(TextField, { label: "Notes", value: dialogState.notes, onChange: (event) => setDialogState((prev) => (prev ? { ...prev, notes: event.target.value } : prev)), multiline: true, minRows: 2 })] }) }), _jsxs(DialogActions, { children: [dialogState.mode === 'edit' && onDeleteRelationship ? (_jsx(Button, { color: "error", onClick: handleDeleteFromDialog, disabled: dialogSubmitting, sx: { mr: 'auto' }, children: "Delete" })) : null, _jsx(Button, { onClick: closeDialog, disabled: dialogSubmitting, children: "Cancel" }), _jsx(Button, { variant: "contained", onClick: handleSubmitDialog, disabled: dialogSubmitting, children: dialogState.mode === 'create' ? 'Create' : 'Save' })] })] }));
    };
    return (_jsxs(Paper, { variant: "outlined", sx: { p: 2, bgcolor: theme.palette.common.white, borderColor: alpha(theme.palette.info.main, 0.25), borderLeft: `4px solid ${theme.palette.info.main}`, boxShadow: `0 4px 12px ${alpha(theme.palette.common.black, 0.05)}` }, children: [_jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: "row", alignItems: "center", spacing: 1, justifyContent: "space-between", children: [_jsxs(Stack, { direction: "row", alignItems: "center", spacing: 1, children: [_jsx(LinkIcon, { color: "primary" }), _jsx(Typography, { variant: "h6", children: "Table Relationships" }), _jsx(Chip, { label: `${relationships.length} relationship${relationships.length === 1 ? '' : 's'}`, size: "small" })] }), _jsx(IconButton, { onClick: toggleExpanded, size: "small", "aria-label": expanded ? 'Collapse relationships' : 'Expand relationships', "aria-expanded": expanded, children: expanded ? _jsx(ExpandLessIcon, {}) : _jsx(ExpandMoreIcon, {}) })] }), _jsx(Collapse, { in: expanded, unmountOnExit: true, children: _jsxs(Stack, { spacing: 2, children: [_jsx(Typography, { variant: "body2", color: "text.secondary", children: "Drag from a primary field to a foreign field to define a relationship. Existing connections can be clicked or managed in the list below." }), _jsx(Box, { sx: { height: 360 }, children: _jsx(RelationshipHoverContext.Provider, { value: hoverContextValue, children: _jsx(ReactFlowProvider, { children: _jsx(RelationshipFlow, { nodes: nodes, edges: edges, canEdit: canEdit, disabled: busy, onConnect: handleConnect, onEdgeClick: handleEdgeClick, onConnectStart: handleConnectStart, onConnectEnd: handleConnectEnd }) }) }) }), _jsx(Divider, {}), _jsxs(Stack, { spacing: 1.5, children: [_jsx(Typography, { variant: "subtitle2", children: "Defined Relationships" }), relationships.length === 0 ? (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "No relationships yet. Create one by connecting fields in the diagram above." })) : (_jsx(List, { disablePadding: true, children: relationships.map((relationship) => {
                                                const primaryLabel = handleLabel(relationship.primaryFieldId);
                                                const foreignLabel = handleLabel(relationship.foreignFieldId);
                                                const secondary = relationship.notes ? relationship.notes : undefined;
                                                return (_jsx(ListItem, { disableGutters: true, secondaryAction: canEdit ? (_jsxs(Stack, { direction: "row", spacing: 1, children: [_jsx(IconButton, { edge: "end", "aria-label": "Edit relationship", onClick: () => openEditDialog(relationship), disabled: busy, size: "small", children: _jsx(EditIcon, { fontSize: "small" }) }), _jsx(IconButton, { edge: "end", "aria-label": "Delete relationship", onClick: () => setDeleteTarget(relationship), disabled: busy, size: "small", children: _jsx(DeleteIcon, { fontSize: "small" }) })] })) : undefined, children: _jsx(ListItemText, { primary: `${primaryLabel} → ${foreignLabel}`, primaryTypographyProps: { variant: 'body2' }, secondaryTypographyProps: { component: 'div' }, secondary: _jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", children: [_jsx(Chip, { label: relationshipTypeLabel(relationship.relationshipType), size: "small", color: "primary", variant: "outlined" }), secondary && (_jsx(Tooltip, { title: secondary, children: _jsx(Typography, { variant: "caption", color: "text.secondary", children: secondary }) }))] }) }) }, relationship.id));
                                            }) }))] })] }) })] }), renderRelationshipDialog(), _jsx(ConfirmDialog, { open: Boolean(deleteTarget), title: "Delete Relationship", description: "Are you sure you want to remove this relationship?", confirmLabel: "Delete", confirmColor: "error", onClose: () => setDeleteTarget(null), onConfirm: handleDelete })] }));
};
export default DataDefinitionRelationshipBuilder;
