import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Box, Button, Chip, Collapse, Dialog, DialogActions, DialogContent, DialogTitle, Divider, FormControl, IconButton, InputLabel, List, ListItem, ListItemText, MenuItem, Paper, Select, Stack, TextField, Tooltip, Typography, Accordion, AccordionSummary, AccordionDetails } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import LinkIcon from '@mui/icons-material/Link';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ZoomInIcon from '@mui/icons-material/ZoomIn';
import ZoomOutIcon from '@mui/icons-material/ZoomOut';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import ConfirmDialog from '../common/ConfirmDialog';
import { useToast } from '../../hooks/useToast';
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
const sanitizeNotes = (value) => {
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
};
// Table card component for two-column layout
const TableCard = ({ table, side, isSource, selectedFieldId, onFieldSelect, onFieldDragStart, onFieldDragEnd, onFieldDragOver, onFieldDrop, onFieldRefChange }) => {
    const tableName = table.alias || table.table?.name || 'Unknown table';
    const physicalName = table.table?.schemaName
        ? `${table.table.schemaName}.${table.table.physicalName}`
        : table.table?.physicalName || 'Unknown physical name';
    // Use only blue header color
    const headerColor = '#3b82f6';
    const headerGradient = 'linear-gradient(90deg, #3b82f6 0%, #2563eb 100%)';
    // Group fields by groupingTab
    const displayFields = useMemo(() => table.fields.filter((field) => Boolean(field?.id && field?.field)), [table.fields]);
    const groupedFields = useMemo(() => {
        const groups = new Map();
        const ungroupedFields = [];
        displayFields.forEach((field) => {
            const groupKey = field.field?.groupingTab || '__ungrouped__';
            if (!groups.has(groupKey)) {
                groups.set(groupKey, []);
            }
            groups.get(groupKey).push(field);
        });
        // Ensure ungrouped fields are listed first if they exist
        if (groups.has('__ungrouped__')) {
            ungroupedFields.push(...groups.get('__ungrouped__'));
            groups.delete('__ungrouped__');
        }
        return { groups: Array.from(groups.entries()), ungroupedFields };
    }, [displayFields]);
    const [expandedGroups, setExpandedGroups] = useState(new Set(groupedFields.groups.map(([key]) => key)));
    const toggleGroup = useCallback((groupKey) => {
        setExpandedGroups((prev) => {
            const next = new Set(prev);
            if (next.has(groupKey)) {
                next.delete(groupKey);
            }
            else {
                next.add(groupKey);
            }
            return next;
        });
    }, []);
    const renderFieldBox = (definitionField) => {
        if (!definitionField?.id || !definitionField?.field) {
            return null;
        }
        const isSelected = selectedFieldId === definitionField.id;
        const boxRef = (el) => {
            if (el && onFieldRefChange) {
                setTimeout(() => {
                    // Get the canvas container and field element positions
                    const fieldRect = el.getBoundingClientRect();
                    const canvasEl = el.closest('[data-canvas]');
                    if (canvasEl) {
                        const canvasRect = canvasEl.getBoundingClientRect();
                        // Calculate position relative to canvas (using right edge as the x coordinate)
                        const relativeX = fieldRect.right - canvasRect.left;
                        const relativeY = fieldRect.top - canvasRect.top + fieldRect.height / 2;
                        const width = fieldRect.width;
                        console.debug(`Field ref calculated for ${definitionField.id}:`, {
                            fieldName: definitionField.field?.name || 'unknown',
                            relativeX,
                            relativeY,
                            width,
                            fieldRect: { top: fieldRect.top, right: fieldRect.right, bottom: fieldRect.bottom, left: fieldRect.left },
                            canvasRect: { top: canvasRect.top, left: canvasRect.left }
                        });
                        onFieldRefChange(definitionField.id, {
                            tableId: table.id,
                            fieldId: definitionField.id,
                            x: relativeX,
                            y: relativeY,
                            width: width
                        });
                    }
                }, 0);
            }
        };
        return (_jsxs(Box, { ref: boxRef, draggable: true, onClick: () => onFieldSelect?.(definitionField.id), onDragStart: (e) => onFieldDragStart?.(definitionField.id, e), onDragEnd: (e) => onFieldDragEnd?.(e), onDragOver: (e) => {
                e.preventDefault();
                onFieldDragOver?.(e);
            }, onDrop: (e) => {
                e.preventDefault();
                onFieldDrop?.(definitionField.id, e);
            }, sx: {
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                padding: '8px 10px',
                borderRadius: '6px',
                cursor: 'grab',
                background: isSelected
                    ? alpha(headerColor, 0.15)
                    : 'rgba(249, 250, 251, 0.6)',
                border: isSelected ? `1.5px solid ${headerColor}` : '1px solid rgba(0, 0, 0, 0.08)',
                transition: 'all 150ms ease',
                '&:active': {
                    cursor: 'grabbing'
                },
                '&:hover': {
                    background: alpha(headerColor, 0.1),
                    borderColor: headerColor
                }
            }, children: [_jsx(Box, { sx: {
                        width: 8,
                        height: 8,
                        borderRadius: '50%',
                        background: headerColor,
                        boxShadow: `0 0 6px ${alpha(headerColor, 0.5)}`
                    } }), _jsx(Typography, { variant: "body2", sx: {
                        flex: 1,
                        fontWeight: isSelected ? 600 : 500,
                        color: isSelected ? headerColor : '#374151',
                        fontSize: '0.875rem'
                    }, children: definitionField.field?.name || 'Unknown field' })] }, definitionField.id));
    };
    return (_jsxs(Paper, { "data-side": side, "data-role": isSource ? 'source' : 'target', sx: {
            borderRadius: '8px',
            overflow: 'hidden',
            border: `1px solid ${alpha(headerColor, 0.2)}`,
            boxShadow: `0 4px 12px ${alpha(headerColor, 0.08)}`,
            transition: 'all 200ms ease',
            '&:hover': {
                boxShadow: `0 8px 24px ${alpha(headerColor, 0.12)}`
            }
        }, children: [_jsx(Box, { sx: {
                    background: headerGradient,
                    padding: '12px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1
                }, children: _jsx(Typography, { sx: {
                        fontWeight: 700,
                        color: 'white',
                        fontSize: '0.9rem',
                        flex: 1
                    }, children: tableName }) }), _jsxs(Box, { sx: { p: 1.5 }, children: [_jsx(Typography, { variant: "caption", sx: {
                            color: '#666',
                            display: 'block',
                            mb: 1,
                            fontSize: '0.75rem',
                            fontWeight: 500
                        }, children: physicalName }), _jsx(Stack, { spacing: 0.75, children: displayFields.length === 0 ? (_jsx(Typography, { variant: "caption", color: "text.secondary", children: "No fields" })) : (_jsxs(_Fragment, { children: [groupedFields.ungroupedFields.length > 0 && (_jsx(Stack, { spacing: 0.5, children: groupedFields.ungroupedFields.map((field) => renderFieldBox(field)) })), groupedFields.groups.map(([groupKey, fields]) => (_jsxs(Accordion, { defaultExpanded: expandedGroups.has(groupKey), onChange: () => toggleGroup(groupKey), sx: {
                                        boxShadow: 'none',
                                        border: `1px solid ${alpha(headerColor, 0.15)}`,
                                        '&.Mui-expanded': {
                                            margin: 0
                                        },
                                        '&:before': {
                                            display: 'none'
                                        }
                                    }, children: [_jsx(AccordionSummary, { expandIcon: _jsx(ExpandMoreIcon, {}), sx: {
                                                background: alpha(headerColor, 0.08),
                                                padding: '8px 12px',
                                                minHeight: 'auto',
                                                '&.Mui-expanded': {
                                                    minHeight: 'auto'
                                                }
                                            }, children: _jsx(Typography, { variant: "caption", sx: {
                                                    fontWeight: 600,
                                                    color: headerColor,
                                                    fontSize: '0.8rem'
                                                }, children: groupKey }) }), _jsx(AccordionDetails, { sx: { padding: '8px 12px' }, children: _jsx(Stack, { spacing: 0.5, children: fields.map((field) => renderFieldBox(field)) }) })] }, groupKey)))] })) })] })] }));
};
const DataDefinitionRelationshipBuilder = ({ tables, relationships, canEdit, busy = false, onCreateRelationship, onUpdateRelationship, onDeleteRelationship, initialPrimaryFieldId, initialForeignFieldId, onInitialRelationshipConsumed }) => {
    const toast = useToast();
    const theme = useTheme();
    const [expanded, setExpanded] = useState(false);
    const [dialogState, setDialogState] = useState(null);
    const [dialogSubmitting, setDialogSubmitting] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [draggedFieldId, setDraggedFieldId] = useState(null);
    const [tablePositions, setTablePositions] = useState(() => {
        const positions = new Map();
        tables.forEach((table, idx) => {
            const col = idx % 2;
            const row = Math.floor(idx / 2);
            positions.set(table.id, {
                tableId: table.id,
                x: col * 450 + 20,
                y: row * 320 + 20
            });
        });
        return positions;
    });
    const [draggingTableId, setDraggingTableId] = useState(null);
    const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
    const [fieldRefs, setFieldRefs] = useState(new Map());
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });
    const [isPanning, setIsPanning] = useState(false);
    const [panStart, setPanStart] = useState({ x: 0, y: 0 });
    const tableLookup = useMemo(() => {
        const byDefinitionId = new Map();
        const byPhysicalId = new Map();
        tables.forEach((table) => {
            byDefinitionId.set(table.id, table);
            if (table.tableId) {
                byPhysicalId.set(table.tableId, table);
            }
        });
        return {
            byDefinitionId,
            byPhysicalId
        };
    }, [tables]);
    const fieldLookup = useMemo(() => {
        const map = new Map();
        const addEntry = (key, definitionField, tableHint) => {
            if (!key) {
                return;
            }
            const existing = map.get(key);
            const nextDefinitionField = definitionField ?? existing?.definitionField ?? null;
            let resolvedTable = tableHint ?? existing?.table ?? null;
            if (!resolvedTable && nextDefinitionField?.definitionTableId) {
                resolvedTable =
                    tableLookup.byDefinitionId.get(nextDefinitionField.definitionTableId) ?? null;
            }
            if (!resolvedTable && nextDefinitionField?.field?.tableId) {
                resolvedTable = tableLookup.byPhysicalId.get(nextDefinitionField.field.tableId) ?? null;
            }
            map.set(key, {
                definitionField: nextDefinitionField,
                table: resolvedTable ?? null
            });
        };
        tables.forEach((table) => {
            table.fields.forEach((definitionField) => {
                if (!definitionField?.id) {
                    return;
                }
                addEntry(definitionField.id, definitionField, table);
                addEntry(definitionField.fieldId, definitionField, table);
            });
        });
        relationships.forEach((relationship) => {
            addEntry(relationship.primaryFieldId, relationship.primaryField, tableLookup.byDefinitionId.get(relationship.primaryTableId) ?? null);
            addEntry(relationship.foreignFieldId, relationship.foreignField, tableLookup.byDefinitionId.get(relationship.foreignTableId) ?? null);
        });
        return map;
    }, [relationships, tableLookup, tables]);
    const relationshipExists = useCallback((primaryFieldId, foreignFieldId) => relationships.some((relationship) => relationship.primaryFieldId === primaryFieldId &&
        relationship.foreignFieldId === foreignFieldId), [relationships]);
    const handleLabel = useCallback((fieldId) => {
        const entry = fieldLookup.get(fieldId);
        if (entry) {
            const { definitionField } = entry;
            const resolvedTable = entry.table ??
                (definitionField?.definitionTableId
                    ? tableLookup.byDefinitionId.get(definitionField.definitionTableId) ?? null
                    : null) ??
                (definitionField?.field?.tableId
                    ? tableLookup.byPhysicalId.get(definitionField.field.tableId) ?? null
                    : null);
            const tableName = resolvedTable?.alias || resolvedTable?.table?.name || 'Unknown table';
            const fieldName = definitionField?.field?.name || definitionField?.field?.id || definitionField?.fieldId || 'Unknown field';
            return `${tableName}.${fieldName}`;
        }
        const relationship = relationships.find((rel) => rel.primaryFieldId === fieldId || rel.foreignFieldId === fieldId);
        if (relationship) {
            const isPrimary = relationship.primaryFieldId === fieldId;
            const definitionField = isPrimary ? relationship.primaryField : relationship.foreignField;
            const table = tableLookup.byDefinitionId.get(isPrimary ? relationship.primaryTableId : relationship.foreignTableId) ?? null;
            const tableName = table?.alias || table?.table?.name || 'Unknown table';
            const fieldName = definitionField?.field?.name || definitionField?.field?.id || definitionField?.fieldId || 'Unknown field';
            return `${tableName}.${fieldName}`;
        }
        return 'Unknown field';
    }, [fieldLookup, relationships, tableLookup]);
    // Initialize dialog with provided field IDs
    useEffect(() => {
        if (!initialPrimaryFieldId || !initialForeignFieldId) {
            return;
        }
        const primaryEntry = fieldLookup.get(initialPrimaryFieldId);
        const foreignEntry = fieldLookup.get(initialForeignFieldId);
        if (!primaryEntry || !foreignEntry) {
            toast.showError('Unable to locate the selected fields for relationship creation.');
            onInitialRelationshipConsumed?.();
            return;
        }
        setExpanded(true);
        setDialogState({
            mode: 'create',
            primaryFieldId: initialPrimaryFieldId,
            foreignFieldId: initialForeignFieldId,
            relationshipType: 'one_to_one',
            notes: ''
        });
        onInitialRelationshipConsumed?.();
    }, [fieldLookup, initialPrimaryFieldId, initialForeignFieldId, onInitialRelationshipConsumed, toast]);
    const openCreateDialog = useCallback((primaryFieldId, foreignFieldId) => {
        setDialogState({
            mode: 'create',
            primaryFieldId,
            foreignFieldId,
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
    const handleFieldDragStart = useCallback((fieldId) => {
        setDraggedFieldId(fieldId);
    }, []);
    const handleFieldDragEnd = useCallback(() => {
        setDraggedFieldId(null);
    }, []);
    const handleFieldDragOver = useCallback(() => {
        // Allow drop
    }, []);
    const handleFieldDrop = useCallback((targetFieldId) => {
        if (!draggedFieldId)
            return;
        if (draggedFieldId === targetFieldId) {
            toast.showError('Select different fields');
            return;
        }
        const draggedEntry = fieldLookup.get(draggedFieldId);
        const targetEntry = fieldLookup.get(targetFieldId);
        if (!draggedEntry || !targetEntry) {
            toast.showError('Invalid field selection');
            return;
        }
        if (!draggedEntry.table || !targetEntry.table) {
            toast.showError('Unable to resolve table metadata for the selected fields.');
            return;
        }
        // Check if they're from different tables
        if (draggedEntry.table.id === targetEntry.table.id) {
            toast.showError('Select fields from different tables');
            return;
        }
        if (relationshipExists(draggedFieldId, targetFieldId)) {
            toast.showError('A relationship between these fields already exists.');
            setDraggedFieldId(null);
            return;
        }
        // Create relationship from dragged field to target field
        openCreateDialog(draggedFieldId, targetFieldId);
        setDraggedFieldId(null);
    }, [draggedFieldId, fieldLookup, openCreateDialog, relationshipExists, toast]);
    const handleTableMouseDown = useCallback((tableId, e) => {
        if (!canEdit || busy)
            return;
        if (e.target.closest('[data-no-drag]'))
            return; // Prevent dragging from certain elements
        const position = tablePositions.get(tableId);
        if (!position)
            return;
        setDraggingTableId(tableId);
        setDragOffset({
            x: e.clientX - position.x,
            y: e.clientY - position.y
        });
    }, [canEdit, busy, tablePositions]);
    useEffect(() => {
        const handleMouseMove = (e) => {
            if (!draggingTableId)
                return;
            const position = tablePositions.get(draggingTableId);
            if (!position)
                return;
            const newX = Math.max(0, e.clientX - dragOffset.x);
            const newY = Math.max(0, e.clientY - dragOffset.y);
            setTablePositions((prev) => {
                const next = new Map(prev);
                next.set(draggingTableId, {
                    ...position,
                    x: newX,
                    y: newY
                });
                return next;
            });
        };
        const handleMouseUp = () => {
            setDraggingTableId(null);
        };
        if (draggingTableId) {
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
            return () => {
                document.removeEventListener('mousemove', handleMouseMove);
                document.removeEventListener('mouseup', handleMouseUp);
            };
        }
    }, [draggingTableId, dragOffset, tablePositions]);
    const closeDialog = useCallback(() => {
        setDialogState(null);
    }, []);
    const handleSubmitDialog = useCallback(async () => {
        if (!dialogState) {
            return;
        }
        if (dialogState.mode === 'create' &&
            relationshipExists(dialogState.primaryFieldId, dialogState.foreignFieldId)) {
            toast.showError('A relationship between these fields already exists.');
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
    }, [dialogState, onCreateRelationship, onUpdateRelationship, relationshipExists, toast]);
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
    const handleZoomIn = useCallback(() => {
        setZoom((prev) => Math.min(prev + 0.2, 3));
    }, []);
    const handleZoomOut = useCallback(() => {
        setZoom((prev) => Math.max(prev - 0.2, 0.5));
    }, []);
    const handleZoomReset = useCallback(() => {
        setZoom(1);
        setPan({ x: 0, y: 0 });
    }, []);
    const handleCanvasWheel = useCallback((e) => {
        if (!e.ctrlKey && !e.metaKey)
            return;
        e.preventDefault();
        const delta = e.deltaY > 0 ? -0.1 : 0.1;
        setZoom((prev) => Math.max(0.5, Math.min(prev + delta, 3)));
    }, []);
    const handleCanvasMouseDown = useCallback((e) => {
        if ((e.button === 2 || e.ctrlKey) && !draggingTableId) {
            setIsPanning(true);
            setPanStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
        }
    }, [pan, draggingTableId]);
    const handleCanvasMouseMove = useCallback((e) => {
        if (isPanning && !draggingTableId) {
            setPan({
                x: e.clientX - panStart.x,
                y: e.clientY - panStart.y
            });
        }
    }, [isPanning, panStart, draggingTableId]);
    const handleCanvasMouseUp = useCallback(() => {
        setIsPanning(false);
    }, []);
    const handleRelationshipLineDoubleClick = useCallback((relationship) => {
        setDialogState({
            mode: 'edit',
            primaryFieldId: relationship.primaryFieldId,
            foreignFieldId: relationship.foreignFieldId,
            relationshipId: relationship.id,
            relationshipType: relationship.relationshipType,
            notes: relationship.notes ?? ''
        });
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
    return (_jsxs(Paper, { variant: "outlined", sx: {
            p: 2,
            bgcolor: theme.palette.common.white,
            borderColor: alpha(theme.palette.primary.main, 0.2),
            borderLeft: `5px solid ${theme.palette.primary.main}`,
            boxShadow: `0 4px 16px ${alpha(theme.palette.common.black, 0.08)}`,
            borderRadius: '8px',
            background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98) 0%, rgba(249, 250, 251, 0.98) 100%)'
        }, children: [_jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: "row", alignItems: "center", spacing: 1.5, justifyContent: "space-between", children: [_jsxs(Stack, { direction: "row", alignItems: "center", spacing: 1.5, children: [_jsx(Box, { sx: {
                                            p: 1,
                                            borderRadius: '6px',
                                            background: 'linear-gradient(135deg, #2563eb 0%, #3b82f6 100%)',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'center'
                                        }, children: _jsx(LinkIcon, { sx: { color: 'white', fontSize: '1.25rem' } }) }), _jsx(Stack, { spacing: 0.25, children: _jsx(Typography, { variant: "h6", sx: {
                                                fontWeight: 700,
                                                background: 'linear-gradient(135deg, #1e40af 0%, #2563eb 100%)',
                                                backgroundClip: 'text',
                                                WebkitBackgroundClip: 'text',
                                                WebkitTextFillColor: 'transparent'
                                            }, children: "Table Relationships" }) }), _jsx(Chip, { label: `${relationships.length} relationship${relationships.length === 1 ? '' : 's'}`, size: "small", sx: {
                                            background: 'linear-gradient(135deg, #dbeafe 0%, #e0e7ff 100%)',
                                            color: '#1e40af',
                                            fontWeight: 600
                                        } })] }), _jsx(IconButton, { onClick: toggleExpanded, size: "small", "aria-label": expanded ? 'Collapse relationships' : 'Expand relationships', "aria-expanded": expanded, sx: {
                                    transition: 'all 200ms ease',
                                    '&:hover': {
                                        background: alpha(theme.palette.primary.main, 0.08)
                                    }
                                }, children: expanded ? _jsx(ExpandLessIcon, {}) : _jsx(ExpandMoreIcon, {}) })] }), _jsx(Collapse, { in: expanded, unmountOnExit: true, children: _jsxs(Stack, { spacing: 2, children: [_jsx(Typography, { variant: "body2", color: "text.secondary", sx: {
                                        fontSize: '0.875rem',
                                        lineHeight: 1.6,
                                        color: '#666'
                                    }, children: "Drag fields between tables to create relationships. Drag table headers to reposition. Double-click connection lines to edit. Use Ctrl+Scroll or zoom buttons to zoom." }), _jsxs(Stack, { direction: "row", spacing: 1, sx: { mb: 1 }, children: [_jsx(Tooltip, { title: "Zoom In (Ctrl+Scroll)", children: _jsx(IconButton, { size: "small", onClick: handleZoomIn, disabled: zoom >= 3, children: _jsx(ZoomInIcon, { fontSize: "small" }) }) }), _jsx(Tooltip, { title: "Zoom Out (Ctrl+Scroll)", children: _jsx(IconButton, { size: "small", onClick: handleZoomOut, disabled: zoom <= 0.5, children: _jsx(ZoomOutIcon, { fontSize: "small" }) }) }), _jsx(Tooltip, { title: "Reset Zoom", children: _jsx(IconButton, { size: "small", onClick: handleZoomReset, children: _jsx(RestartAltIcon, { fontSize: "small" }) }) }), _jsxs(Typography, { variant: "caption", sx: { ml: 1, display: 'flex', alignItems: 'center' }, children: [Math.round(zoom * 100), "%"] })] }), _jsx(Box, { "data-canvas": true, onWheel: handleCanvasWheel, onMouseDown: handleCanvasMouseDown, onMouseMove: handleCanvasMouseMove, onMouseUp: handleCanvasMouseUp, onMouseLeave: handleCanvasMouseUp, sx: {
                                        position: 'relative',
                                        minHeight: 600,
                                        p: 2,
                                        borderRadius: '8px',
                                        background: 'linear-gradient(135deg, rgba(249, 250, 251, 0.5) 0%, rgba(240, 249, 255, 0.5) 100%)',
                                        border: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                                        overflow: 'auto',
                                        cursor: isPanning ? 'grabbing' : 'default'
                                    }, children: _jsxs(Box, { sx: {
                                            position: 'relative',
                                            width: '100%',
                                            minHeight: 600,
                                            transform: `scale(${zoom}) translate(${pan.x / zoom}px, ${pan.y / zoom}px)`,
                                            transformOrigin: '0 0',
                                            transition: isPanning ? 'none' : 'transform 0.1s ease-out'
                                        }, children: [_jsxs("svg", { width: "100%", height: "100%", style: {
                                                    position: 'absolute',
                                                    top: 0,
                                                    left: 0,
                                                    pointerEvents: 'none',
                                                    zIndex: 1,
                                                    overflow: 'visible'
                                                }, children: [_jsx("defs", { children: _jsx("marker", { id: "arrowhead", markerWidth: "10", markerHeight: "10", refX: "9", refY: "3", orient: "auto", children: _jsx("polygon", { points: "0 0, 10 3, 0 6", fill: "#3b82f6" }) }) }), relationships.map((rel) => {
                                                        const primaryRef = fieldRefs.get(rel.primaryFieldId);
                                                        const foreignRef = fieldRefs.get(rel.foreignFieldId);
                                                        if (!primaryRef || !foreignRef) {
                                                            console.debug(`Missing refs for relationship ${rel.id}:`, {
                                                                primaryFieldId: rel.primaryFieldId,
                                                                foreignFieldId: rel.foreignFieldId,
                                                                primaryRef: primaryRef ? 'exists' : 'MISSING',
                                                                foreignRef: foreignRef ? 'exists' : 'MISSING',
                                                                totalRefs: fieldRefs.size,
                                                                allRefIds: Array.from(fieldRefs.keys())
                                                            });
                                                            return null;
                                                        }
                                                        console.debug(`Rendering path for relationship ${rel.id}:`, { primaryRef, foreignRef });
                                                        // Connection points on the right edge of primary field and left edge of foreign field
                                                        const x1 = primaryRef.x;
                                                        const y1 = primaryRef.y;
                                                        const x2 = foreignRef.x - (foreignRef.width || 0);
                                                        const y2 = foreignRef.y;
                                                        const midX = (x1 + x2) / 2;
                                                        const midY = (y1 + y2) / 2;
                                                        return (_jsxs("g", { children: [_jsx("path", { d: `M ${x1} ${y1} Q ${midX} ${y1}, ${midX} ${midY} T ${x2} ${y2}`, stroke: "#3b82f6", strokeWidth: "2", fill: "none", opacity: "0.6", style: {
                                                                        cursor: 'pointer',
                                                                        pointerEvents: 'auto'
                                                                    }, onDoubleClick: () => handleRelationshipLineDoubleClick(rel) }), _jsx("path", { d: `M ${x1} ${y1} Q ${midX} ${y1}, ${midX} ${midY} T ${x2} ${y2}`, stroke: "transparent", strokeWidth: "10", fill: "none", style: {
                                                                        cursor: 'pointer',
                                                                        pointerEvents: 'auto'
                                                                    }, onDoubleClick: () => handleRelationshipLineDoubleClick(rel) }), _jsx("rect", { x: midX - 35, y: midY - 12, width: "70", height: "24", fill: "white", stroke: "#3b82f6", strokeWidth: "1", rx: "4" }), _jsx("text", { x: midX, y: midY + 4, textAnchor: "middle", fontSize: "11", fill: "#3b82f6", fontWeight: "600", style: {
                                                                        pointerEvents: 'none',
                                                                        userSelect: 'none'
                                                                    }, children: relationshipTypeLabel(rel.relationshipType).substring(0, 8) })] }, `line-${rel.id}`));
                                                    })] }), _jsx(Box, { sx: { position: 'relative', zIndex: 2 }, children: tables.map((table) => {
                                                    const position = tablePositions.get(table.id);
                                                    if (!position)
                                                        return null;
                                                    return (_jsx(Box, { onMouseDown: (e) => handleTableMouseDown(table.id, e), sx: {
                                                            position: 'absolute',
                                                            left: `${position.x}px`,
                                                            top: `${position.y}px`,
                                                            width: '320px',
                                                            cursor: draggingTableId === table.id ? 'grabbing' : 'grab',
                                                            userSelect: 'none'
                                                        }, children: _jsx(TableCard, { table: table, side: "left", isSource: true, selectedFieldId: draggedFieldId, onFieldDragStart: handleFieldDragStart, onFieldDragEnd: handleFieldDragEnd, onFieldDragOver: handleFieldDragOver, onFieldDrop: handleFieldDrop, onFieldRefChange: (fieldId, ref) => {
                                                                setFieldRefs((prev) => {
                                                                    const next = new Map(prev);
                                                                    next.set(fieldId, ref);
                                                                    return next;
                                                                });
                                                            } }) }, table.id));
                                                }) })] }) }), _jsx(Divider, { sx: { background: 'linear-gradient(90deg, transparent, #e0e7ff, transparent)' } }), _jsxs(Stack, { spacing: 1.5, children: [_jsx(Typography, { variant: "subtitle2", sx: {
                                                fontWeight: 700,
                                                color: '#1e40af',
                                                fontSize: '0.95rem'
                                            }, children: "Defined Relationships" }), relationships.length === 0 ? (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "No relationships yet. Create one by clicking fields in the diagram above." })) : (_jsx(List, { disablePadding: true, children: relationships.map((relationship) => {
                                                const primaryLabel = handleLabel(relationship.primaryFieldId);
                                                const foreignLabel = handleLabel(relationship.foreignFieldId);
                                                const secondary = relationship.notes ? relationship.notes : undefined;
                                                return (_jsx(ListItem, { disableGutters: true, sx: {
                                                        p: 1.5,
                                                        mb: 0.75,
                                                        borderRadius: '6px',
                                                        background: 'linear-gradient(135deg, rgba(243, 244, 246, 0.6) 0%, rgba(249, 250, 251, 0.6) 100%)',
                                                        border: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                                                        transition: 'all 200ms ease',
                                                        '&:hover': {
                                                            background: 'linear-gradient(135deg, rgba(219, 234, 254, 0.4) 0%, rgba(240, 249, 255, 0.4) 100%)',
                                                            boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.1)}`
                                                        }
                                                    }, secondaryAction: canEdit ? (_jsxs(Stack, { direction: "row", spacing: 0.5, children: [_jsx(IconButton, { edge: "end", "aria-label": "Edit relationship", onClick: () => openEditDialog(relationship), disabled: busy, size: "small", sx: {
                                                                    color: theme.palette.primary.main,
                                                                    '&:hover': {
                                                                        background: alpha(theme.palette.primary.main, 0.1)
                                                                    }
                                                                }, children: _jsx(EditIcon, { fontSize: "small" }) }), _jsx(IconButton, { edge: "end", "aria-label": "Delete relationship", onClick: () => setDeleteTarget(relationship), disabled: busy, size: "small", sx: {
                                                                    color: theme.palette.error.main,
                                                                    '&:hover': {
                                                                        background: alpha(theme.palette.error.main, 0.1)
                                                                    }
                                                                }, children: _jsx(DeleteIcon, { fontSize: "small" }) })] })) : undefined, children: _jsx(ListItemText, { primary: `${primaryLabel} → ${foreignLabel}`, primaryTypographyProps: {
                                                            variant: 'body2',
                                                            sx: { fontWeight: 600, color: '#1e40af' }
                                                        }, secondaryTypographyProps: { component: 'div' }, secondary: _jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", sx: { mt: 0.75 }, children: [_jsx(Chip, { label: relationshipTypeLabel(relationship.relationshipType), size: "small", sx: {
                                                                        background: 'linear-gradient(135deg, #dbeafe 0%, #e0e7ff 100%)',
                                                                        color: '#1e40af',
                                                                        fontWeight: 600,
                                                                        fontSize: '0.75rem'
                                                                    } }), secondary && (_jsx(Tooltip, { title: secondary, children: _jsx(Typography, { variant: "caption", color: "text.secondary", sx: { fontSize: '0.75rem' }, children: secondary }) }))] }) }) }, relationship.id));
                                            }) }))] })] }) })] }), renderRelationshipDialog(), _jsx(ConfirmDialog, { open: Boolean(deleteTarget), title: "Delete Relationship", description: "Are you sure you want to remove this relationship?", confirmLabel: "Delete", confirmColor: "error", onClose: () => setDeleteTarget(null), onConfirm: handleDelete })] }));
};
export default DataDefinitionRelationshipBuilder;
