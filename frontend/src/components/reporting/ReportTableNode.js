import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from 'react';
import { alpha, useTheme } from '@mui/material/styles';
import { Box, Checkbox, Divider, IconButton, Stack, Tooltip, Typography } from '@mui/material';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked';
import { Handle, Position } from 'reactflow';
import { NodeResizer } from '@reactflow/node-resizer';
import '@reactflow/node-resizer/dist/style.css';
export const REPORTING_FIELD_DRAG_TYPE = 'application/reporting-field';
const ReportTableNode = ({ data, selected }) => {
    const theme = useTheme();
    const selectedFieldSet = useMemo(() => new Set(data.selectedFieldIds ?? []), [data.selectedFieldIds]);
    const [activeDropFieldId, setActiveDropFieldId] = useState(null);
    const handleToggle = (fieldId) => {
        data.onFieldToggle?.(fieldId);
    };
    const handleFieldDragOver = (event, targetFieldId) => {
        if (!data.onFieldJoin) {
            return;
        }
        event.preventDefault();
        event.dataTransfer.dropEffect = 'link';
        setActiveDropFieldId(targetFieldId);
    };
    const handleFieldDragLeave = () => {
        setActiveDropFieldId(null);
    };
    const handleFieldDrop = (event, targetFieldId) => {
        if (!data.onFieldJoin) {
            return;
        }
        event.preventDefault();
        event.stopPropagation();
        setActiveDropFieldId(null);
        const raw = event.dataTransfer.getData(REPORTING_FIELD_DRAG_TYPE);
        let payload = null;
        try {
            if (raw) {
                const parsed = JSON.parse(raw);
                if (parsed?.fieldId && parsed?.tableId) {
                    payload = parsed;
                }
            }
        }
        catch (error) {
            // Ignore malformed payloads
        }
        data.onFieldJoin?.(payload, { tableId: data.tableId, fieldId: targetFieldId });
        data.onFieldDragEnd?.();
    };
    const handleDragStart = (event, fieldId) => {
        event.dataTransfer.setData(REPORTING_FIELD_DRAG_TYPE, JSON.stringify({ tableId: data.tableId, fieldId }));
        event.dataTransfer.effectAllowed = 'copyMove';
        data.onFieldDragStart?.({ tableId: data.tableId, fieldId });
    };
    const handleDragEnd = () => {
        data.onFieldDragEnd?.();
        setActiveDropFieldId(null);
    };
    const handleRemove = (event) => {
        event.stopPropagation();
        data.onRemoveTable?.(data.tableId);
    };
    return (_jsxs(Box, { sx: {
            position: 'relative',
            display: 'flex',
            flexDirection: 'column',
            width: '100%',
            height: '100%',
            borderRadius: 2,
            border: `1px solid ${alpha(theme.palette.primary.main, selected ? 0.75 : 0.35)}`,
            boxShadow: selected
                ? `0 12px 28px ${alpha(theme.palette.primary.main, 0.22)}`
                : `0 6px 18px ${alpha(theme.palette.grey[700], 0.12)}`,
            backgroundColor: theme.palette.background.paper,
            minWidth: 220,
            minHeight: 200,
            cursor: 'grab',
            overflow: 'hidden',
            '&:active': {
                cursor: 'grabbing'
            }
        }, children: [_jsx(NodeResizer, { color: alpha(theme.palette.primary.main, 0.8), isVisible: selected, minWidth: 220, minHeight: 200, handleStyle: {
                    width: 14,
                    height: 14,
                    borderRadius: 4,
                    border: `1px solid ${alpha(theme.palette.primary.dark, 0.65)}`,
                    background: alpha(theme.palette.background.paper, 0.95)
                }, lineStyle: {
                    borderWidth: 1,
                    borderStyle: 'dashed',
                    borderColor: alpha(theme.palette.primary.main, 0.6)
                } }), _jsx(Box, { sx: {
                    background: `linear-gradient(90deg, ${alpha(theme.palette.primary.main, 0.92)} 0%, ${alpha(theme.palette.primary.dark, 0.85)} 100%)`,
                    borderBottom: `1px solid ${alpha(theme.palette.primary.dark, 0.55)}`,
                    px: 1.6,
                    py: 1.2,
                    flexShrink: 0
                }, children: _jsxs(Stack, { direction: "row", alignItems: "flex-start", spacing: 1, children: [_jsx(Box, { sx: { flex: 1, minWidth: 0 }, children: _jsxs(Stack, { spacing: 0.25, children: [_jsx(Typography, { variant: "subtitle2", sx: { fontWeight: 700, color: theme.palette.common.white, letterSpacing: 0.2 }, children: data.label }), data.subtitle && (_jsx(Typography, { variant: "caption", sx: { color: alpha(theme.palette.common.white, 0.75) }, children: data.subtitle })), data.meta && (_jsx(Typography, { variant: "caption", sx: { color: alpha(theme.palette.common.white, 0.65) }, children: data.meta }))] }) }), data.onRemoveTable && (_jsx(Tooltip, { title: "Remove table", children: _jsx(IconButton, { size: "small", onClick: handleRemove, sx: {
                                    color: alpha(theme.palette.common.white, 0.9),
                                    '&:hover': {
                                        color: theme.palette.error.light,
                                        backgroundColor: alpha(theme.palette.error.light, 0.12)
                                    }
                                }, children: _jsx(DeleteOutlineIcon, { fontSize: "small" }) }) }))] }) }), _jsx(Divider, { sx: { borderColor: alpha(theme.palette.primary.main, 0.22), flexShrink: 0 } }), _jsx(Box, { sx: {
                    px: 1.4,
                    py: 1.1,
                    flex: 1,
                    overflowY: 'auto',
                    backgroundColor: alpha(theme.palette.background.default, 0.85)
                }, children: data.fields.length === 0 ? (_jsx(Typography, { variant: "caption", color: "text.secondary", children: "No fields available" })) : (_jsx(Stack, { spacing: 0.6, children: data.fields.map((field) => {
                        const isSelected = selectedFieldSet.has(field.id);
                        const isDropTarget = activeDropFieldId === field.id;
                        return (_jsxs(Box, { draggable: true, onDragStart: (event) => handleDragStart(event, field.id), onClick: (event) => {
                                event.stopPropagation();
                                handleToggle(field.id);
                            }, onDragOver: (event) => handleFieldDragOver(event, field.id), onDragLeave: handleFieldDragLeave, onDrop: (event) => handleFieldDrop(event, field.id), onDragEnd: handleDragEnd, sx: {
                                position: 'relative',
                                borderRadius: 1,
                                border: `1px solid ${alpha(theme.palette.primary.main, isDropTarget || isSelected ? 0.65 : 0.18)}`,
                                backgroundColor: isDropTarget
                                    ? alpha(theme.palette.secondary.light, 0.28)
                                    : isSelected
                                        ? alpha(theme.palette.primary.light, 0.28)
                                        : alpha(theme.palette.common.white, 0.9),
                                boxShadow: isDropTarget
                                    ? `0 6px 18px ${alpha(theme.palette.secondary.main, 0.28)}`
                                    : isSelected
                                        ? `0 4px 14px ${alpha(theme.palette.primary.main, 0.18)}`
                                        : `0 2px 8px ${alpha(theme.palette.grey[700], 0.08)}`,
                                padding: '6px 10px',
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.75,
                                cursor: 'pointer',
                                transition: 'all 150ms ease',
                                '&:hover': {
                                    borderColor: alpha(theme.palette.primary.main, 0.55),
                                    backgroundColor: alpha(theme.palette.primary.light, 0.18)
                                }
                            }, children: [_jsx(Handle, { type: "target", position: Position.Left, id: `target:${field.id}`, style: {
                                        position: 'absolute',
                                        top: '50%',
                                        left: -10,
                                        transform: 'translateY(-50%)',
                                        background: theme.palette.secondary.main,
                                        border: 'none',
                                        width: 10,
                                        height: 10,
                                        borderRadius: '50%',
                                        boxShadow: `0 0 0 2px ${theme.palette.background.paper}`
                                    } }), _jsx(Checkbox, { size: "small", checked: isSelected, icon: _jsx(RadioButtonUncheckedIcon, { fontSize: "small" }), checkedIcon: _jsx(CheckCircleIcon, { fontSize: "small" }), onClick: (event) => event.stopPropagation(), onChange: (event) => {
                                        event.stopPropagation();
                                        handleToggle(field.id);
                                    }, sx: {
                                        p: 0.25,
                                        color: alpha(theme.palette.text.secondary, 0.6),
                                        '&.Mui-checked': {
                                            color: theme.palette.primary.main
                                        }
                                    } }), _jsxs(Box, { sx: { flex: 1, minWidth: 0 }, children: [_jsx(Typography, { variant: "body2", sx: { fontWeight: 600, fontSize: 13 }, noWrap: true, children: field.name }), (field.type || field.description) && (_jsx(Typography, { variant: "caption", color: "text.secondary", noWrap: true, children: [field.type, field.description].filter(Boolean).join(' · ') }))] }), _jsx(Handle, { type: "source", position: Position.Right, id: `source:${field.id}`, style: {
                                        position: 'absolute',
                                        top: '50%',
                                        right: -10,
                                        transform: 'translateY(-50%)',
                                        background: theme.palette.primary.main,
                                        border: 'none',
                                        width: 10,
                                        height: 10,
                                        borderRadius: '50%',
                                        boxShadow: `0 0 0 2px ${theme.palette.background.paper}`
                                    } })] }, field.id));
                    }) })) })] }));
};
export default ReportTableNode;
