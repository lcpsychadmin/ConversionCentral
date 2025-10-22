import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Autocomplete, Box, Button, Chip, Checkbox, CircularProgress, Collapse, Dialog, DialogActions, DialogContent, DialogTitle, FormControlLabel, Grid, IconButton, InputAdornment, Link, ListItemIcon, ListItemText, Menu, MenuItem, Paper, Stack, TextField, Typography } from '@mui/material';
import TextareaAutosize from '@mui/material/TextareaAutosize';
import { alpha, styled, useTheme } from '@mui/material/styles';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import EditNoteIcon from '@mui/icons-material/EditNote';
import FileDownloadOutlinedIcon from '@mui/icons-material/FileDownloadOutlined';
import LibraryAddCheckOutlinedIcon from '@mui/icons-material/LibraryAddCheckOutlined';
import PostAddOutlinedIcon from '@mui/icons-material/PostAddOutlined';
import SearchIcon from '@mui/icons-material/Search';
import PreviewIcon from '@mui/icons-material/Preview';
import { DataGrid, useGridApiContext } from '@mui/x-data-grid';
import DataDefinitionRelationshipBuilder from './DataDefinitionRelationshipBuilder';
import ConnectionDataPreviewDialog from '../system-connection/ConnectionDataPreviewDialog';
import { fetchTablePreview } from '../../services/tableService';
import { useToast } from '../../hooks/useToast';
const FIELD_COLUMNS = [
    { key: 'name', label: 'Name', kind: 'text', minWidth: 200 },
    { key: 'description', label: 'Description', kind: 'text', multiline: true, minWidth: 260 },
    { key: 'applicationUsage', label: 'Application Usage', kind: 'text', multiline: true, minWidth: 220 },
    { key: 'businessDefinition', label: 'Business Definition', kind: 'text', multiline: true, minWidth: 260 },
    { key: 'enterpriseAttribute', label: 'Enterprise Attribute', kind: 'text', minWidth: 200 },
    { key: 'fieldType', label: 'Field Type', kind: 'text', minWidth: 160 },
    { key: 'fieldLength', label: 'Length', kind: 'number', minWidth: 100 },
    { key: 'decimalPlaces', label: 'Decimal Places', kind: 'number', minWidth: 120 },
    { key: 'systemRequired', label: 'System Required', kind: 'boolean' },
    { key: 'businessProcessRequired', label: 'Business Process Required', kind: 'boolean' },
    { key: 'suppressedField', label: 'Suppressed', kind: 'boolean' },
    { key: 'active', label: 'Active', kind: 'boolean' },
    {
        key: 'legalRegulatoryImplications',
        label: 'Legal / Regulatory Implications',
        kind: 'text',
        multiline: true,
        minWidth: 240
    },
    { key: 'securityClassification', label: 'Security Classification', kind: 'text', minWidth: 200 },
    { key: 'dataValidation', label: 'Data Validation', kind: 'text', multiline: true, minWidth: 220 },
    { key: 'referenceTable', label: 'Reference Table', kind: 'text', minWidth: 180 },
    { key: 'groupingTab', label: 'Grouping Tab', kind: 'text', minWidth: 160 }
];
const BOOLEAN_FIELD_KEYS = [
    'systemRequired',
    'businessProcessRequired',
    'suppressedField',
    'active'
];
const buildDraft = (field) => ({
    name: field.name,
    description: field.description ?? '',
    applicationUsage: field.applicationUsage ?? '',
    businessDefinition: field.businessDefinition ?? '',
    enterpriseAttribute: field.enterpriseAttribute ?? '',
    fieldType: field.fieldType,
    fieldLength: field.fieldLength?.toString() ?? '',
    decimalPlaces: field.decimalPlaces?.toString() ?? '',
    legalRegulatoryImplications: field.legalRegulatoryImplications ?? '',
    securityClassification: field.securityClassification ?? '',
    dataValidation: field.dataValidation ?? '',
    referenceTable: field.referenceTable ?? '',
    groupingTab: field.groupingTab ?? '',
    systemRequired: field.systemRequired,
    businessProcessRequired: field.businessProcessRequired,
    suppressedField: field.suppressedField,
    active: field.active
});
const mapDraftsFromDefinition = (definition) => {
    const drafts = {};
    definition.tables.forEach((table) => {
        table.fields.forEach((definitionField) => {
            if (definitionField.field) {
                drafts[definitionField.field.id] = buildDraft(definitionField.field);
            }
        });
    });
    return drafts;
};
const createEmptyDraft = () => ({
    name: '',
    description: '',
    applicationUsage: '',
    businessDefinition: '',
    enterpriseAttribute: '',
    fieldType: '',
    fieldLength: '',
    decimalPlaces: '',
    legalRegulatoryImplications: '',
    securityClassification: '',
    dataValidation: '',
    referenceTable: '',
    groupingTab: '',
    systemRequired: false,
    businessProcessRequired: false,
    suppressedField: false,
    active: true
});
const resolveRowClassName = (params) => {
    if (params.row.__isPlaceholder) {
        return 'placeholder-row';
    }
    return '';
};
const renderNumber = (value) => {
    if (value === null || value === undefined || value === '') {
        return '—';
    }
    return value;
};
const renderText = (value) => {
    if (!value) {
        return '—';
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : '—';
};
const toCsvString = (value) => {
    if (value === null || value === undefined) {
        return '""';
    }
    if (typeof value === 'boolean') {
        return value ? '"Yes"' : '"No"';
    }
    const stringValue = String(value).trim();
    return `"${stringValue.replace(/"/g, '""')}"`;
};
const parseBooleanCell = (value, defaultValue) => {
    if (!value) {
        return defaultValue;
    }
    const normalized = value.trim().toLowerCase();
    if (['yes', 'y', 'true', '1'].includes(normalized)) {
        return true;
    }
    if (['no', 'n', 'false', '0'].includes(normalized)) {
        return false;
    }
    return defaultValue;
};
const QuickFilterToolbar = () => {
    const theme = useTheme();
    const apiRef = useGridApiContext();
    const [filterValue, setFilterValue] = useState('');
    const handleFilterChange = (event) => {
        const value = event.target.value;
        setFilterValue(value);
        apiRef.current.setQuickFilterValues(value.split(/\s+/).filter(Boolean));
    };
    return (_jsx(Stack, { direction: { xs: 'column', sm: 'row' }, justifyContent: { xs: 'flex-start', sm: 'flex-start' }, alignItems: { xs: 'stretch', sm: 'center' }, sx: {
            px: 1.5,
            py: 1,
            gap: 1,
            backgroundColor: alpha(theme.palette.info.main, 0.05),
            borderBottom: `1px solid ${alpha(theme.palette.info.main, 0.15)}`
        }, children: _jsx(TextField, { placeholder: "Filter list...", value: filterValue, onChange: handleFilterChange, size: "small", InputProps: {
                startAdornment: (_jsx(InputAdornment, { position: "start", children: _jsx(SearchIcon, { sx: { color: alpha(theme.palette.primary.main, 0.5) } }) }))
            }, sx: {
                minWidth: { xs: '100%', sm: 280 },
                '& .MuiOutlinedInput-root': {
                    height: 40,
                    borderRadius: 2,
                    backgroundColor: alpha(theme.palette.common.white, 0.95),
                    boxShadow: `0 6px 16px ${alpha(theme.palette.primary.main, 0.12)}`,
                    transition: theme.transitions.create(['box-shadow', 'border-color'], {
                        duration: theme.transitions.duration.shorter
                    }),
                    '&:hover': {
                        boxShadow: `0 8px 20px ${alpha(theme.palette.primary.main, 0.16)}`
                    },
                    '&.Mui-focused': {
                        boxShadow: `0 8px 24px ${alpha(theme.palette.primary.main, 0.2)}`
                    },
                    '& fieldset': {
                        border: `1.5px solid ${alpha(theme.palette.primary.main, 0.35)}`
                    },
                    '&:hover fieldset': {
                        borderColor: alpha(theme.palette.primary.main, 0.5)
                    },
                    '&.Mui-focused fieldset': {
                        borderColor: theme.palette.primary.main
                    }
                },
                '& .MuiInputBase-input': {
                    fontSize: 13,
                    padding: theme.spacing(0.5, 1.5)
                }
            } }) }));
};
const GridTextarea = styled(TextareaAutosize)(({ theme }) => ({
    width: '100%',
    borderRadius: 6,
    border: `1px solid ${alpha(theme.palette.divider, 0.45)}`,
    backgroundColor: alpha(theme.palette.common.white, 0.96),
    fontFamily: theme.typography.fontFamily,
    fontSize: 13.5,
    lineHeight: 1.4,
    padding: theme.spacing(0.75, 1.1),
    paddingBottom: theme.spacing(1),
    resize: 'none',
    transition: theme.transitions.create(['border-color', 'box-shadow', 'background-color'], {
        duration: theme.transitions.duration.shortest
    }),
    '&:hover': {
        borderColor: alpha(theme.palette.primary.main, 0.7)
    },
    '&:focus': {
        outline: 'none',
        borderColor: theme.palette.primary.main,
        boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
        backgroundColor: alpha(theme.palette.common.white, 0.98)
    },
    '&:disabled': {
        backgroundColor: alpha(theme.palette.action.disabledBackground, 0.35),
        color: theme.palette.text.disabled,
        cursor: 'not-allowed'
    }
}));
const DataDefinitionDetails = ({ definition, canEdit = false, inlineSavingState, onInlineFieldSubmit, onEditField, onAddExistingFieldInline, onCreateFieldInline, availableFieldsByTable, tableSavingState, fieldActionsDisabled = false, onBulkPasteResult, onCreateRelationship, onUpdateRelationship, onDeleteRelationship, relationshipBusy = false }) => {
    const toast = useToast();
    const [expandedTables, setExpandedTables] = useState(() => {
        const initial = {};
        definition.tables.forEach((table) => {
            initial[table.id] = false;
        });
        return initial;
    });
    const tablesById = useMemo(() => {
        const map = new Map();
        definition.tables.forEach((table) => {
            map.set(table.id, table);
        });
        return map;
    }, [definition.tables]);
    const initialDrafts = useMemo(() => mapDraftsFromDefinition(definition), [definition]);
    const [drafts, setDrafts] = useState(initialDrafts);
    const [committed, setCommitted] = useState(initialDrafts);
    const [inlineRows, setInlineRows] = useState({});
    const [gridViewByTable, setGridViewByTable] = useState({});
    const [filterValue, setFilterValue] = useState('');
    const [newMenu, setNewMenu] = useState(null);
    const [activeDialog, setActiveDialog] = useState(null);
    const [dragSource, setDragSource] = useState(null);
    const [dropTargetTableId, setDropTargetTableId] = useState(null);
    const [intendedRelationship, setIntendedRelationship] = useState(null);
    const [previewOpen, setPreviewOpen] = useState(false);
    const [previewTableId, setPreviewTableId] = useState(null);
    const [previewTableName, setPreviewTableName] = useState('');
    const [previewSchemaName, setPreviewSchemaName] = useState(null);
    const [previewData, setPreviewData] = useState(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewError, setPreviewError] = useState(null);
    const theme = useTheme();
    const chipBaseSx = useMemo(() => ({
        height: 24,
        borderRadius: 12,
        fontWeight: 600,
        fontSize: 12,
        '& .MuiChip-label': {
            px: 1.2
        }
    }), []);
    const renderBooleanChip = useCallback((value) => {
        if (value === null || value === undefined) {
            return (_jsx(Chip, { label: "\u2014", size: "small", sx: {
                    ...chipBaseSx,
                    color: theme.palette.text.disabled,
                    bgcolor: alpha(theme.palette.text.disabled, 0.12)
                } }));
        }
        const isTrue = Boolean(value);
        const paletteColor = isTrue ? theme.palette.success : theme.palette.warning;
        return (_jsx(Chip, { label: isTrue ? 'Yes' : 'No', size: "small", sx: {
                ...chipBaseSx,
                bgcolor: alpha(paletteColor.main, isTrue ? 0.24 : 0.18),
                color: paletteColor.dark ?? paletteColor.main,
                textTransform: 'uppercase'
            } }));
    }, [chipBaseSx, theme]);
    const resolveFieldTypePalette = useCallback((fieldType) => {
        const normalized = fieldType.toLowerCase();
        if (normalized.includes('date') || normalized.includes('time')) {
            return theme.palette.info;
        }
        if (normalized.includes('id') || normalized.includes('key')) {
            return theme.palette.secondary;
        }
        if (normalized.includes('bool') || normalized.includes('flag')) {
            return theme.palette.success;
        }
        if (normalized.includes('amount') || normalized.includes('number') || normalized.includes('decimal')) {
            return theme.palette.warning;
        }
        if (normalized.includes('code')) {
            return theme.palette.info;
        }
        return theme.palette.primary;
    }, [theme]);
    const renderFieldTypeChip = useCallback((value) => {
        if (!value) {
            return '—';
        }
        const paletteColor = resolveFieldTypePalette(value);
        return (_jsx(Chip, { label: value, size: "small", sx: {
                ...chipBaseSx,
                textTransform: 'capitalize',
                bgcolor: alpha(paletteColor.main, 0.22),
                color: paletteColor.dark ?? paletteColor.main
            } }));
    }, [chipBaseSx, resolveFieldTypePalette]);
    useEffect(() => {
        setDrafts(initialDrafts);
        setCommitted(initialDrafts);
    }, [initialDrafts]);
    useEffect(() => {
        setExpandedTables((prev) => {
            const next = {};
            definition.tables.forEach((table) => {
                next[table.id] = prev[table.id] ?? false;
            });
            return next;
        });
    }, [definition.tables]);
    useEffect(() => {
        setGridViewByTable((prev) => {
            const next = {};
            definition.tables.forEach((table) => {
                next[table.id] = prev[table.id] ?? false;
            });
            return next;
        });
    }, [definition.tables]);
    const sortedTables = useMemo(() => definition.tables
        .slice()
        .sort((a, b) => {
        const left = a.loadOrder ?? Number.MAX_SAFE_INTEGER;
        const right = b.loadOrder ?? Number.MAX_SAFE_INTEGER;
        if (left === right) {
            return (a.table?.name ?? '').localeCompare(b.table?.name ?? '');
        }
        return left - right;
    }), [definition.tables]);
    const handleToggle = useCallback((tableId) => {
        setExpandedTables((prev) => ({
            ...prev,
            [tableId]: !(prev[tableId] ?? false)
        }));
    }, []);
    const getDraftForField = useCallback((field) => drafts[field.id] ?? buildDraft(field), [drafts]);
    const getCommittedForField = useCallback((field) => committed[field.id] ?? buildDraft(field), [committed]);
    const handleDraftChange = useCallback((field, key, value) => {
        setDrafts((prev) => {
            const previous = prev[field.id] ?? buildDraft(field);
            return {
                ...prev,
                [field.id]: {
                    ...previous,
                    [key]: value
                }
            };
        });
    }, []);
    const handleSubmitChange = useCallback(async (field, key) => {
        if (!canEdit || !onInlineFieldSubmit) {
            return;
        }
        const currentDraft = getDraftForField(field);
        const currentCommitted = getCommittedForField(field);
        if (currentDraft[key] === currentCommitted[key]) {
            return;
        }
        const success = await onInlineFieldSubmit(field, { [key]: currentDraft[key] });
        if (success) {
            setCommitted((prev) => {
                const previous = prev[field.id] ?? buildDraft(field);
                return {
                    ...prev,
                    [field.id]: {
                        ...previous,
                        [key]: currentDraft[key]
                    }
                };
            });
        }
        else {
            setDrafts((prev) => {
                const previous = prev[field.id] ?? buildDraft(field);
                return {
                    ...prev,
                    [field.id]: {
                        ...previous,
                        [key]: currentCommitted[key]
                    }
                };
            });
        }
    }, [canEdit, getCommittedForField, getDraftForField, onInlineFieldSubmit]);
    const handleBooleanToggle = useCallback(async (field, key, checked) => {
        if (!canEdit) {
            return;
        }
        const committedState = getCommittedForField(field);
        if (committedState[key] === checked) {
            return;
        }
        setDrafts((prev) => {
            const previous = prev[field.id] ?? buildDraft(field);
            return {
                ...prev,
                [field.id]: {
                    ...previous,
                    [key]: checked
                }
            };
        });
        if (!onInlineFieldSubmit) {
            return;
        }
        const success = await onInlineFieldSubmit(field, { [key]: checked });
        if (success) {
            setCommitted((prev) => {
                const previous = prev[field.id] ?? buildDraft(field);
                return {
                    ...prev,
                    [field.id]: {
                        ...previous,
                        [key]: checked
                    }
                };
            });
        }
        else {
            setDrafts((prev) => {
                const previous = prev[field.id] ?? buildDraft(field);
                return {
                    ...prev,
                    [field.id]: {
                        ...previous,
                        [key]: committedState[key]
                    }
                };
            });
        }
    }, [canEdit, getCommittedForField, onInlineFieldSubmit]);
    const clearInlineRow = useCallback((definitionTableId) => {
        setInlineRows((prev) => {
            if (!(definitionTableId in prev)) {
                return prev;
            }
            const next = { ...prev };
            delete next[definitionTableId];
            return next;
        });
        setActiveDialog((prev) => (prev?.tableId === definitionTableId ? null : prev));
    }, []);
    const updateExistingRow = useCallback((definitionTableId, updates) => {
        setInlineRows((prev) => {
            const current = prev[definitionTableId];
            if (!current || current.type !== 'add-existing') {
                return prev;
            }
            return {
                ...prev,
                [definitionTableId]: {
                    ...current,
                    ...updates
                }
            };
        });
    }, []);
    const updateCreateRow = useCallback((definitionTableId, updater) => {
        setInlineRows((prev) => {
            const current = prev[definitionTableId];
            if (!current || current.type !== 'create-new') {
                return prev;
            }
            return {
                ...prev,
                [definitionTableId]: updater(current)
            };
        });
    }, []);
    const handleStartAddExisting = useCallback((table, selectableFields) => {
        if (!selectableFields.length) {
            return;
        }
        setInlineRows((prev) => ({
            ...prev,
            [table.id]: {
                type: 'add-existing',
                fieldId: null,
                notes: ''
            }
        }));
        setActiveDialog({ tableId: table.id, type: 'add-existing' });
    }, []);
    const handleStartCreateNew = useCallback((table) => {
        setInlineRows((prev) => ({
            ...prev,
            [table.id]: {
                type: 'create-new',
                draft: createEmptyDraft(),
                notes: '',
                errors: {}
            }
        }));
        setActiveDialog({ tableId: table.id, type: 'create-new' });
    }, []);
    const handleExistingFieldSelect = useCallback((definitionTableId, field) => {
        updateExistingRow(definitionTableId, {
            fieldId: field?.id ?? null,
            error: undefined
        });
    }, [updateExistingRow]);
    const handleInlineNotesChange = useCallback((definitionTableId, value) => {
        setInlineRows((prev) => {
            const current = prev[definitionTableId];
            if (!current) {
                return prev;
            }
            if (current.type === 'add-existing') {
                return {
                    ...prev,
                    [definitionTableId]: {
                        ...current,
                        notes: value
                    }
                };
            }
            return {
                ...prev,
                [definitionTableId]: {
                    ...current,
                    notes: value,
                    errors: {
                        ...current.errors,
                        notes: undefined
                    }
                }
            };
        });
    }, []);
    const handleCreateDraftTextChange = useCallback((definitionTableId, key, value) => {
        updateCreateRow(definitionTableId, (state) => ({
            ...state,
            draft: {
                ...state.draft,
                [key]: value
            },
            errors: {
                ...state.errors,
                [key]: undefined
            }
        }));
    }, [updateCreateRow]);
    const handleCreateDraftBooleanChange = useCallback((definitionTableId, key, checked) => {
        updateCreateRow(definitionTableId, (state) => ({
            ...state,
            draft: {
                ...state.draft,
                [key]: checked
            }
        }));
    }, [updateCreateRow]);
    const handleInlineSave = useCallback(async (table, tableName, inlineState, tableSaving) => {
        if (fieldActionsDisabled || tableSaving) {
            return;
        }
        if (inlineState.type === 'add-existing') {
            if (!onAddExistingFieldInline) {
                return;
            }
            if (!inlineState.fieldId) {
                updateExistingRow(table.id, { error: 'Select a field to add.' });
                return;
            }
            const success = await onAddExistingFieldInline({
                tableId: table.tableId,
                tableName,
                fieldId: inlineState.fieldId,
                notes: inlineState.notes
            });
            if (success) {
                clearInlineRow(table.id);
            }
            return;
        }
        if (!onCreateFieldInline) {
            return;
        }
        const errors = {};
        if (!inlineState.draft.name.trim()) {
            errors.name = 'Required';
        }
        if (!inlineState.draft.fieldType.trim()) {
            errors.fieldType = 'Required';
        }
        if (Object.keys(errors).length) {
            updateCreateRow(table.id, (state) => ({
                ...state,
                errors: {
                    ...state.errors,
                    ...errors
                }
            }));
            return;
        }
        const success = await onCreateFieldInline({
            tableId: table.tableId,
            tableName,
            field: inlineState.draft,
            notes: inlineState.notes
        });
        if (success) {
            clearInlineRow(table.id);
        }
    }, [
        clearInlineRow,
        fieldActionsDisabled,
        onAddExistingFieldInline,
        onCreateFieldInline,
        updateCreateRow,
        updateExistingRow
    ]);
    const handlePaste = useCallback(async (event, table, tableName) => {
        if (!canEdit || !onCreateFieldInline) {
            return;
        }
        const target = event.target;
        if (target) {
            const tag = target.tagName?.toLowerCase();
            if (tag === 'input' || tag === 'textarea' || target.isContentEditable) {
                return;
            }
        }
        const inlineState = inlineRows[table.id];
        const tableSaving = Boolean(tableSavingState?.[table.tableId]);
        if (inlineState || tableSaving || fieldActionsDisabled) {
            return;
        }
        const clipboardText = event.clipboardData?.getData('text/plain');
        if (!clipboardText) {
            return;
        }
        if (!clipboardText.includes('\t') && !clipboardText.includes('\n')) {
            return;
        }
        const rows = clipboardText
            .split(/\r?\n/)
            .map((row) => row.split('\t').map((cell) => cell.trim()))
            .filter((cells) => cells.some((cell) => cell.length > 0));
        if (!rows.length) {
            return;
        }
        event.preventDefault();
        let succeeded = 0;
        let failed = 0;
        for (const cells of rows) {
            const [name, fieldType, fieldLength, decimalPlaces, systemRequired, businessProcessRequired, suppressedField, active, description, applicationUsage, businessDefinition, enterpriseAttribute, legalRegulatoryImplications, securityClassification, dataValidation, referenceTable, groupingTab, notes] = cells;
            const draft = {
                name: name ?? '',
                description: description ?? '',
                applicationUsage: applicationUsage ?? '',
                businessDefinition: businessDefinition ?? '',
                enterpriseAttribute: enterpriseAttribute ?? '',
                fieldType: fieldType ?? '',
                fieldLength: fieldLength ?? '',
                decimalPlaces: decimalPlaces ?? '',
                legalRegulatoryImplications: legalRegulatoryImplications ?? '',
                securityClassification: securityClassification ?? '',
                dataValidation: dataValidation ?? '',
                referenceTable: referenceTable ?? '',
                groupingTab: groupingTab ?? '',
                systemRequired: parseBooleanCell(systemRequired, false),
                businessProcessRequired: parseBooleanCell(businessProcessRequired, false),
                suppressedField: parseBooleanCell(suppressedField, false),
                active: parseBooleanCell(active, true)
            };
            if (!draft.name && !draft.fieldType) {
                continue;
            }
            if (!draft.name.trim() || !draft.fieldType.trim()) {
                failed += 1;
                continue;
            }
            const success = await onCreateFieldInline({
                tableId: table.tableId,
                tableName,
                field: draft,
                notes: notes ?? '',
                suppressNotifications: rows.length > 1
            });
            if (success) {
                succeeded += 1;
            }
            else {
                failed += 1;
            }
        }
        if ((succeeded > 0 || failed > 0) && onBulkPasteResult) {
            onBulkPasteResult({
                tableId: table.tableId,
                tableName,
                total: rows.length,
                succeeded,
                failed
            });
        }
    }, [canEdit, fieldActionsDisabled, inlineRows, onBulkPasteResult, onCreateFieldInline, tableSavingState]);
    const exportTableToCsv = useCallback((table, tableName) => {
        const headers = [...FIELD_COLUMNS.map((column) => column.label), 'Notes'];
        const body = table.fields
            .filter((definitionField) => definitionField.field != null)
            .map((definitionField) => {
            const field = definitionField.field;
            const values = FIELD_COLUMNS.map((column) => {
                switch (column.kind) {
                    case 'boolean':
                        return toCsvString(field[column.key]);
                    case 'number':
                        return toCsvString(field[column.key] ?? '');
                    default:
                        return toCsvString(field[column.key] ?? '');
                }
            });
            values.push(toCsvString(definitionField.notes ?? ''));
            return values.join(',');
        });
        const csv = [headers.map((header) => toCsvString(header)).join(','), ...body].join('\n');
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const sanitizedName = tableName
            .trim()
            .replace(/[^a-z0-9]+/gi, '_')
            .replace(/^_+|_+$/g, '')
            .toLowerCase() || 'data_definition';
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', `${sanitizedName}_fields.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, []);
    const handleOpenPreview = useCallback((table) => {
        setPreviewTableId(table.table.id);
        setPreviewTableName(table.table.name);
        setPreviewSchemaName(table.table.schemaName ?? null);
        setPreviewData(null);
        setPreviewError(null);
        setPreviewLoading(true);
        setPreviewOpen(true);
        const loadPreview = async () => {
            try {
                const data = await fetchTablePreview(table.table.id, 100);
                setPreviewData(data);
            }
            catch (error) {
                setPreviewError(error instanceof Error ? error.message : 'Failed to load preview');
            }
            finally {
                setPreviewLoading(false);
            }
        };
        loadPreview();
    }, []);
    const handleClosePreview = useCallback(() => {
        setPreviewOpen(false);
        setPreviewTableId(null);
        setPreviewTableName('');
        setPreviewSchemaName(null);
        setPreviewData(null);
        setPreviewError(null);
        setPreviewLoading(false);
    }, []);
    const handleRefreshPreview = useCallback(async () => {
        if (!previewTableId)
            return;
        setPreviewLoading(true);
        setPreviewError(null);
        try {
            const data = await fetchTablePreview(previewTableId, 100);
            setPreviewData(data);
        }
        catch (error) {
            setPreviewError(error instanceof Error ? error.message : 'Failed to load preview');
        }
        finally {
            setPreviewLoading(false);
        }
    }, [previewTableId]);
    return (_jsxs(Stack, { spacing: 3, sx: { p: 2 }, children: [definition.description && (_jsxs(Paper, { variant: "outlined", sx: {
                    p: 2,
                    bgcolor: alpha(theme.palette.info.main, 0.04),
                    borderColor: alpha(theme.palette.info.main, 0.25),
                    borderWidth: 1.5
                }, children: [_jsx(Typography, { variant: "subtitle2", color: "info.dark", sx: { fontWeight: 600, mb: 1 }, children: "Description" }), _jsx(Typography, { variant: "body1", children: definition.description })] })), _jsx(DataDefinitionRelationshipBuilder, { tables: definition.tables, relationships: definition.relationships, canEdit: Boolean(canEdit), onCreateRelationship: onCreateRelationship, onUpdateRelationship: onUpdateRelationship, onDeleteRelationship: onDeleteRelationship, busy: relationshipBusy, initialPrimaryFieldId: intendedRelationship?.primaryFieldId, initialForeignFieldId: intendedRelationship?.foreignFieldId, onInitialRelationshipConsumed: () => setIntendedRelationship(null) }), sortedTables.map((table) => {
                const isExpanded = expandedTables[table.id] ?? false;
                const isGridView = gridViewByTable[table.id] ?? false;
                const tableName = table.alias || table.table.name;
                const availableFields = availableFieldsByTable?.[table.tableId] ?? [];
                const currentFields = new Set(table.fields.map((item) => item.fieldId));
                const selectableFields = availableFields.filter((field) => !currentFields.has(field.id));
                const inlineState = inlineRows[table.id] ?? null;
                const tableSaving = Boolean(tableSavingState?.[table.tableId]);
                const inlineType = inlineState?.type ?? null;
                const isDialogOpen = Boolean(inlineState) && activeDialog?.tableId === table.id;
                const dialogTitleId = `inline-dialog-${table.id}`;
                const disableAdd = fieldActionsDisabled || tableSaving || Boolean(inlineState) || selectableFields.length === 0;
                const disableCreate = fieldActionsDisabled || tableSaving || Boolean(inlineState);
                const disableGridToggle = !canEdit || fieldActionsDisabled || tableSaving || Boolean(inlineState);
                const addTooltip = (() => {
                    if (selectableFields.length === 0) {
                        return 'All fields for this table are already included.';
                    }
                    if (inlineState) {
                        return 'Finish the current inline action first.';
                    }
                    if (tableSaving) {
                        return 'An operation is in progress for this table.';
                    }
                    return undefined;
                })();
                const createTooltip = (() => {
                    if (inlineState) {
                        return 'Finish the current inline action first.';
                    }
                    if (tableSaving) {
                        return 'An operation is in progress for this table.';
                    }
                    return undefined;
                })();
                const isNewMenuOpen = newMenu?.tableId === table.id;
                const menuAnchor = isNewMenuOpen ? newMenu.anchor : undefined;
                const rows = table.fields
                    .filter((definitionField) => definitionField.field != null)
                    .map((definitionField) => ({
                    id: definitionField.id,
                    notes: definitionField.notes ?? '',
                    __definitionField: definitionField,
                    ...getDraftForField(definitionField.field)
                }));
                if (isGridView) {
                    rows.push({
                        id: `__placeholder__-${table.id}`,
                        notes: '',
                        __isPlaceholder: true,
                        ...createEmptyDraft()
                    });
                }
                const baseColumns = FIELD_COLUMNS.map((column) => {
                    const baseMinWidth = 'minWidth' in column ? column.minWidth : undefined;
                    const align = column.kind === 'boolean' || column.kind === 'number' ? 'center' : 'left';
                    const isMultiline = column.kind === 'text' && Boolean(column.multiline);
                    return {
                        field: column.key,
                        headerName: column.label,
                        minWidth: baseMinWidth ?? (column.kind === 'text' ? 200 : 140),
                        flex: column.kind === 'text' ? 1 : 0.7,
                        sortable: true,
                        filterable: true,
                        align,
                        headerAlign: align,
                        type: column.kind === 'boolean' ? 'boolean' : column.kind === 'number' ? 'number' : 'string',
                        valueGetter: (params) => params.row[column.key],
                        cellClassName: isMultiline ? 'multiline-cell' : undefined,
                        sortComparator: column.kind === 'number'
                            ? (value1, value2) => Number(value1 ?? 0) - Number(value2 ?? 0)
                            : undefined,
                        renderCell: (params) => {
                            const { row, value } = params;
                            if (row.__isPlaceholder) {
                                if (isGridView && column.key === 'name') {
                                    return (_jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", children: [_jsx(Box, { sx: {
                                                    width: 10,
                                                    height: 10,
                                                    borderRadius: '50%',
                                                    border: `2px dashed ${alpha(theme.palette.primary.main, 0.4)}`
                                                } }), _jsx(Typography, { variant: "body2", color: "text.disabled", sx: { fontStyle: 'italic' }, children: "Paste new rows here" })] }));
                                }
                                return null;
                            }
                            const definitionField = row.__definitionField;
                            if (!definitionField) {
                                if (column.key === 'fieldType') {
                                    return renderFieldTypeChip(value);
                                }
                                if (column.kind === 'boolean') {
                                    return renderBooleanChip(value);
                                }
                                if (column.kind === 'number') {
                                    return renderNumber(value);
                                }
                                if (isMultiline) {
                                    const text = typeof value === 'string' ? value.trim() : '';
                                    return (_jsx(Typography, { variant: "body2", sx: {
                                            width: '100%',
                                            whiteSpace: 'pre-wrap',
                                            lineHeight: 1.4,
                                            color: text ? theme.palette.text.primary : theme.palette.text.disabled,
                                            fontStyle: text ? 'normal' : 'italic'
                                        }, children: text || '—' }));
                                }
                                const textValue = renderText(value);
                                return (_jsx(Typography, { variant: "body2", sx: {
                                        width: '100%',
                                        whiteSpace: 'nowrap',
                                        overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                        color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
                                    }, children: textValue }));
                            }
                            const saving = inlineSavingState?.[definitionField.field.id] ?? false;
                            if (isGridView && canEdit) {
                                if (column.kind === 'boolean') {
                                    return (_jsx(Checkbox, { size: "small", checked: Boolean(row[column.key]), onChange: (_, checked) => {
                                            void handleBooleanToggle(definitionField.field, column.key, checked);
                                        }, disabled: fieldActionsDisabled || saving }));
                                }
                                const draftKey = column.key;
                                const draftValue = row[draftKey];
                                const inputType = column.kind === 'number' ? 'number' : 'text';
                                if (isMultiline) {
                                    return (_jsx(GridTextarea, { value: draftValue ?? '', disabled: fieldActionsDisabled || saving, onChange: (event) => handleDraftChange(definitionField.field, draftKey, event.target.value), onBlur: () => {
                                            void handleSubmitChange(definitionField.field, draftKey);
                                        }, minRows: 3, maxRows: 8 }));
                                }
                                return (_jsx(TextField, { variant: "outlined", size: "small", fullWidth: true, value: draftValue, type: inputType, disabled: fieldActionsDisabled || saving, onChange: (event) => handleDraftChange(definitionField.field, draftKey, event.target.value), onBlur: () => {
                                        void handleSubmitChange(definitionField.field, draftKey);
                                    }, sx: {
                                        '& .MuiOutlinedInput-root': {
                                            borderRadius: 0.75,
                                            backgroundColor: alpha(theme.palette.common.white, 0.97),
                                            '& fieldset': {
                                                borderColor: alpha(theme.palette.divider, 0.45)
                                            },
                                            '&:hover fieldset': {
                                                borderColor: alpha(theme.palette.primary.main, 0.65)
                                            },
                                            '&.Mui-focused fieldset': {
                                                borderColor: theme.palette.primary.main,
                                                boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`
                                            }
                                        },
                                        '& .MuiOutlinedInput-input': {
                                            fontSize: 13.5,
                                            lineHeight: 1.35,
                                            padding: theme.spacing(0.55, 1)
                                        }
                                    } }));
                            }
                            if (!isGridView && column.key === 'name' && canEdit) {
                                const displayValue = renderText(value);
                                return (_jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", draggable: true, onDragStart: (e) => {
                                        setDragSource({ tableId: table.id, fieldId: definitionField.id });
                                        e.dataTransfer.effectAllowed = 'copy';
                                        e.dataTransfer.setData('application/json', JSON.stringify({
                                            tableId: table.id,
                                            definitionFieldId: definitionField.id,
                                            fieldId: definitionField.field.id,
                                            fieldName: definitionField.field.name
                                        }));
                                    }, onDragEnd: () => {
                                        setDragSource(null);
                                    }, sx: {
                                        cursor: 'grab',
                                        '&:active': { cursor: 'grabbing' },
                                        opacity: dragSource?.fieldId === definitionField.id ? 0.7 : 1,
                                        transition: 'opacity 0.2s'
                                    }, children: [saving && _jsx(CircularProgress, { size: 16 }), _jsx(Link, { component: "button", variant: "body2", underline: "hover", onClick: () => onEditField?.(table.tableId, tableName, definitionField.field), sx: { fontWeight: 600 }, children: displayValue })] }));
                            }
                            if (column.kind === 'boolean') {
                                return renderBooleanChip(value);
                            }
                            if (column.key === 'fieldType') {
                                return renderFieldTypeChip(value);
                            }
                            if (column.kind === 'number') {
                                return renderNumber(value);
                            }
                            const textValue = renderText(value);
                            return (_jsx(Typography, { variant: "body2", sx: {
                                    width: '100%',
                                    whiteSpace: 'nowrap',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
                                }, children: textValue }));
                        }
                    };
                });
                const notesColumn = {
                    field: 'notes',
                    headerName: 'Notes',
                    minWidth: 220,
                    flex: 1,
                    sortable: true,
                    filterable: true,
                    editable: false,
                    renderCell: (params) => {
                        if (params.row.__isPlaceholder) {
                            return null;
                        }
                        const textValue = renderText(params.value);
                        return (_jsx(Typography, { variant: "body2", sx: {
                                width: '100%',
                                whiteSpace: 'pre-wrap',
                                lineHeight: 1.4,
                                color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
                            }, children: textValue }));
                    }
                };
                const columns = [
                    {
                        field: '__indicator',
                        headerName: '',
                        width: 36,
                        minWidth: 36,
                        sortable: false,
                        filterable: false,
                        disableColumnMenu: true,
                        headerAlign: 'center',
                        align: 'center',
                        resizable: false,
                        valueGetter: () => null,
                        renderCell: ({ row }) => {
                            const isPlaceholder = Boolean(row.__isPlaceholder);
                            const isActive = Boolean(row.active);
                            const paletteColor = isActive ? theme.palette.primary.main : theme.palette.grey[500];
                            return (_jsx(Box, { sx: {
                                    display: 'inline-flex',
                                    width: isPlaceholder ? 10 : 12,
                                    height: isPlaceholder ? 10 : 12,
                                    borderRadius: '50%',
                                    border: `2px solid ${alpha(paletteColor, isPlaceholder ? 0.3 : 0.6)}`,
                                    bgcolor: isPlaceholder ? 'transparent' : alpha(paletteColor, 0.6),
                                    opacity: isActive || isPlaceholder ? 1 : 0.5,
                                    transition: theme.transitions.create(['background-color', 'border-color'], {
                                        duration: theme.transitions.duration.shortest
                                    })
                                } }));
                        }
                    },
                    ...baseColumns,
                    notesColumn
                ];
                return (_jsx(Paper, { variant: "outlined", sx: {
                        p: 2,
                        bgcolor: theme.palette.common.white,
                        borderColor: dropTargetTableId === table.id ? theme.palette.info.main : alpha(theme.palette.info.main, 0.25),
                        borderLeft: `4px solid ${theme.palette.info.main}`,
                        boxShadow: `0 4px 12px ${alpha(theme.palette.common.black, 0.05)}`,
                        transition: theme.transitions.create('border-color', { duration: 200 })
                    }, onDragOver: (e) => {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'copy';
                        setDropTargetTableId(table.id);
                    }, onDragLeave: (e) => {
                        // Only clear if leaving the Paper itself
                        if (e.target === e.currentTarget) {
                            setDropTargetTableId(null);
                        }
                    }, onDrop: (e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setDropTargetTableId(null);
                        try {
                            const data = JSON.parse(e.dataTransfer.getData('application/json'));
                            const { tableId: sourceTableId, definitionFieldId, fieldId: legacyFieldId } = data;
                            const sourceFieldCandidateId = definitionFieldId ?? legacyFieldId;
                            if (!sourceFieldCandidateId) {
                                toast.showError('Unable to determine the selected source field. Please try again.');
                                setDragSource(null);
                                return;
                            }
                            // Prevent dropping on the same table
                            if (sourceTableId === table.id) {
                                toast.showError('Select fields from different tables to create a relationship.');
                                setDragSource(null);
                                return;
                            }
                            const sourceTable = sourceTableId ? tablesById.get(sourceTableId) : undefined;
                            const sourceField = sourceTable?.fields.find((field) => (field.id === sourceFieldCandidateId || field.fieldId === sourceFieldCandidateId) &&
                                Boolean(field.field));
                            if (!sourceField) {
                                toast.showError('Unable to locate the selected source field in this definition.');
                                setDragSource(null);
                                return;
                            }
                            const targetField = table.fields.find((field) => Boolean(field?.id && field.field));
                            if (!targetField) {
                                toast.showError('The target table does not have any fields available for relationships.');
                                setDragSource(null);
                                return;
                            }
                            setDragSource(null);
                            setIntendedRelationship({
                                primaryFieldId: sourceField.id,
                                foreignFieldId: targetField.id
                            });
                        }
                        catch (error) {
                            console.error('Error processing drop:', error);
                        }
                    }, children: _jsxs(Stack, { spacing: 2, children: [_jsxs(Stack, { direction: { xs: 'column', md: 'row' }, justifyContent: "space-between", alignItems: { xs: 'flex-start', md: 'center' }, spacing: 1.5, sx: {
                                    pb: 1.5,
                                    borderBottom: `2px solid ${alpha(theme.palette.warning.main, 0.15)}`
                                }, children: [_jsxs(Stack, { spacing: 0.5, children: [_jsx(Typography, { variant: "h6", sx: {
                                                    color: theme.palette.primary.main,
                                                    fontWeight: 700,
                                                    letterSpacing: 0.3
                                                }, children: tableName }), _jsx(Typography, { variant: "body2", color: "text.secondary", sx: { fontStyle: 'italic' }, children: table.table.schemaName
                                                    ? `${table.table.schemaName}.${table.table.physicalName}`
                                                    : table.table.physicalName })] }), _jsxs(Stack, { direction: "row", spacing: 1, alignItems: "center", flexWrap: "wrap", children: [typeof table.loadOrder === 'number' && (_jsx(Chip, { label: `Load Order: ${table.loadOrder}`, size: "small", color: "primary", variant: "outlined" })), table.table.tableType && _jsx(Chip, { label: table.table.tableType, size: "small" }), table.table.status && (_jsx(Chip, { label: table.table.status, size: "small", color: table.table.status === 'active' ? 'success' : 'default' })), _jsx(IconButton, { onClick: () => handleToggle(table.id), "aria-label": isExpanded ? 'Collapse fields' : 'Expand fields', size: "small", children: isExpanded ? _jsx(ExpandLessIcon, {}) : _jsx(ExpandMoreIcon, {}) })] })] }), table.description && (_jsx(Typography, { variant: "body2", color: "text.secondary", children: table.description })), _jsxs(Stack, { direction: { xs: 'column', sm: 'row' }, spacing: 1, justifyContent: "flex-end", alignItems: { xs: 'stretch', sm: 'center' }, children: [_jsx(Button, { variant: "contained", size: "small", startIcon: _jsx(AddCircleOutlineIcon, {}), onClick: (event) => setNewMenu({ tableId: table.id, anchor: event.currentTarget }), disabled: !canEdit || fieldActionsDisabled || tableSaving, children: "New" }), _jsx(Button, { variant: isGridView ? 'contained' : 'outlined', size: "small", startIcon: _jsx(EditNoteIcon, {}), onClick: () => setGridViewByTable((prev) => ({
                                            ...prev,
                                            [table.id]: !isGridView
                                        })), disabled: disableGridToggle, children: isGridView ? 'Exit grid view' : 'Edit in grid' }), _jsx(Button, { variant: "outlined", size: "small", startIcon: _jsx(PreviewIcon, {}), onClick: () => handleOpenPreview(table), disabled: tableSaving, children: "Preview" }), _jsx(Button, { variant: "outlined", size: "small", startIcon: _jsx(FileDownloadOutlinedIcon, {}), onClick: () => exportTableToCsv(table, tableName), children: "Export to Excel" })] }), _jsxs(Menu, { anchorEl: menuAnchor, open: isNewMenuOpen, onClose: () => setNewMenu(null), keepMounted: true, children: [_jsxs(MenuItem, { onClick: () => {
                                            handleStartCreateNew(table);
                                            setNewMenu(null);
                                        }, disabled: disableCreate || !canEdit, children: [_jsx(ListItemIcon, { children: _jsx(PostAddOutlinedIcon, { fontSize: "small" }) }), _jsx(ListItemText, { primary: "Create field", secondary: disableCreate ? createTooltip : undefined })] }), _jsxs(MenuItem, { onClick: () => {
                                            handleStartAddExisting(table, selectableFields);
                                            setNewMenu(null);
                                        }, disabled: disableAdd || !canEdit, children: [_jsx(ListItemIcon, { children: _jsx(LibraryAddCheckOutlinedIcon, { fontSize: "small" }) }), _jsx(ListItemText, { primary: "Add existing field", secondary: disableAdd ? addTooltip : undefined })] })] }), _jsx(Collapse, { in: isExpanded, timeout: "auto", unmountOnExit: true, children: _jsxs(Stack, { spacing: 2, children: [_jsx(Box, { onPaste: (event) => {
                                                if (isGridView) {
                                                    handlePaste(event, table, tableName);
                                                }
                                            }, tabIndex: isGridView ? 0 : undefined, sx: {
                                                outline: 'none',
                                                borderRadius: 1.5,
                                                transition: theme.transitions.create(['box-shadow', 'background-color'], { duration: 200 }),
                                                '&:focus': {
                                                    boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.25)}, inset 0 0 0 2px ${theme.palette.primary.main}`,
                                                    backgroundColor: alpha(theme.palette.primary.main, 0.02)
                                                },
                                                '&:focus-visible': {
                                                    boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.25)}, inset 0 0 0 2px ${theme.palette.primary.main}`,
                                                    backgroundColor: alpha(theme.palette.primary.main, 0.02)
                                                },
                                                '&:hover': {
                                                    boxShadow: `0 0 0 1px ${alpha(theme.palette.primary.main, 0.2)}`
                                                }
                                            }, children: _jsx(DataGrid, { autoHeight: true, rows: rows, columns: columns, disableRowSelectionOnClick: true, disableColumnSelector: true, disableDensitySelector: true, hideFooter: true, density: "comfortable", sortingOrder: ['asc', 'desc'], getRowClassName: resolveRowClassName, columnHeaderHeight: isGridView ? 46 : 52, rowHeight: isGridView ? 68 : 52, showColumnVerticalBorder: true, showCellVerticalBorder: true, slots: { toolbar: QuickFilterToolbar }, sx: {
                                                    '--DataGrid-rowBorderColor': alpha(theme.palette.divider, 0.22),
                                                    '--DataGrid-columnSeparatorColor': alpha(theme.palette.divider, 0.22),
                                                    border: `1px solid ${alpha(theme.palette.divider, 0.45)}`,
                                                    borderRadius: isGridView ? 1.5 : 2,
                                                    bgcolor: alpha(theme.palette.background.paper, 0.98),
                                                    boxShadow: `0 6px 18px ${alpha(theme.palette.common.black, 0.05)}`,
                                                    '& .MuiDataGrid-columnHeaders': {
                                                        background: `linear-gradient(180deg, ${theme.palette.primary.main} 0%, ${alpha(theme.palette.primary.dark, 0.8)} 100%)`,
                                                        borderBottom: `1px solid ${theme.palette.primary.dark}`,
                                                        boxShadow: `inset 0 -2px 4px ${alpha(theme.palette.primary.dark, 0.35)}`,
                                                        textTransform: 'uppercase',
                                                        letterSpacing: 0.35,
                                                        color: theme.palette.primary.contrastText
                                                    },
                                                    '& .MuiDataGrid-columnHeader, & .MuiDataGrid-cell': {
                                                        outline: 'none'
                                                    },
                                                    '& .MuiDataGrid-columnHeaderTitle': {
                                                        fontWeight: 700,
                                                        fontSize: 12.5,
                                                        color: theme.palette.primary.contrastText,
                                                        textShadow: `0 1px 2px ${alpha(theme.palette.primary.dark, 0.4)}`
                                                    },
                                                    '& .MuiDataGrid-columnHeader .MuiSvgIcon-root': {
                                                        color: alpha(theme.palette.primary.contrastText, 0.9)
                                                    },
                                                    '& .MuiDataGrid-cell': {
                                                        display: 'flex',
                                                        alignItems: 'center',
                                                        borderBottom: `1px solid ${alpha(theme.palette.divider, 0.18)}`,
                                                        borderRight: `1px solid ${alpha(theme.palette.divider, 0.16)}`,
                                                        fontSize: 13.5,
                                                        color: theme.palette.text.primary,
                                                        padding: theme.spacing(0.75, 1.25),
                                                        lineHeight: 1.45,
                                                        transition: theme.transitions.create(['background-color', 'box-shadow'], {
                                                            duration: theme.transitions.duration.shortest
                                                        })
                                                    },
                                                    '& .MuiDataGrid-cell.multiline-cell': {
                                                        alignItems: 'flex-start',
                                                        paddingTop: theme.spacing(1.15),
                                                        paddingBottom: theme.spacing(1.15)
                                                    },
                                                    '& .MuiDataGrid-cell.multiline-cell .MuiTypography-root': {
                                                        width: '100%'
                                                    },
                                                    '& .MuiDataGrid-row .MuiDataGrid-cell:last-of-type': {
                                                        borderRight: 'none'
                                                    },
                                                    '& .MuiDataGrid-row:nth-of-type(even) .MuiDataGrid-cell': {
                                                        backgroundColor: alpha(theme.palette.action.hover, 0.12)
                                                    },
                                                    '& .MuiDataGrid-row:hover .MuiDataGrid-cell': {
                                                        backgroundColor: alpha(theme.palette.primary.main, 0.04)
                                                    },
                                                    '& .MuiDataGrid-row.Mui-selected .MuiDataGrid-cell': {
                                                        backgroundColor: alpha(theme.palette.primary.main, 0.08)
                                                    },
                                                    '& .MuiDataGrid-cell:focus-within': {
                                                        outline: `2px solid ${alpha(theme.palette.primary.main, 0.4)}`,
                                                        outlineOffset: -2,
                                                        backgroundColor: alpha(theme.palette.primary.main, 0.05)
                                                    },
                                                    '& .MuiDataGrid-cell.MuiDataGrid-cell--editable': {
                                                        backgroundColor: alpha(theme.palette.primary.light, 0.05)
                                                    },
                                                    '& .MuiDataGrid-cell.MuiDataGrid-cell--editing': {
                                                        backgroundColor: alpha(theme.palette.common.white, 0.98),
                                                        boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.primary.main, 0.45)}`
                                                    },
                                                    '& .MuiDataGrid-virtualScroller': {
                                                        backgroundColor: 'transparent'
                                                    },
                                                    '& .placeholder-row .MuiDataGrid-cell': {
                                                        fontStyle: 'italic',
                                                        color: theme.palette.text.disabled,
                                                        backgroundColor: alpha(theme.palette.info.main, 0.08),
                                                        transition: theme.transitions.create(['background-color', 'box-shadow', 'color'], {
                                                            duration: theme.transitions.duration.shortest
                                                        })
                                                    },
                                                    '& .placeholder-row .MuiDataGrid-cell:focus-within': {
                                                        outline: 'none',
                                                        backgroundColor: alpha(theme.palette.info.main, 0.15),
                                                        boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.info.main, 0.6)}, 0 0 8px ${alpha(theme.palette.info.main, 0.3)}`,
                                                        color: theme.palette.info.dark,
                                                        fontWeight: 500
                                                    },
                                                    '& .placeholder-row:hover .MuiDataGrid-cell': {
                                                        backgroundColor: alpha(theme.palette.info.main, 0.12),
                                                        color: theme.palette.text.secondary
                                                    }
                                                } }) }), isGridView && (_jsx(Stack, { spacing: 0.5, children: _jsxs(Typography, { variant: "caption", color: "text.secondary", sx: { fontWeight: 500 }, children: ["\uD83D\uDCA1 Tip: Click the grid above, then use ", _jsx("strong", { children: "Ctrl+V" }), " to paste from Excel or paste new rows in the blank row at the bottom."] }) }))] }) }), _jsxs(Dialog, { open: isDialogOpen, onClose: (_, reason) => {
                                    if (tableSaving) {
                                        return;
                                    }
                                    if (reason === 'backdropClick' && fieldActionsDisabled) {
                                        return;
                                    }
                                    clearInlineRow(table.id);
                                }, fullWidth: true, maxWidth: inlineType === 'create-new' ? 'md' : 'sm', "aria-labelledby": dialogTitleId, disableEscapeKeyDown: tableSaving, children: [inlineType === 'add-existing' && inlineState?.type === 'add-existing' && (_jsxs(_Fragment, { children: [_jsx(DialogTitle, { id: dialogTitleId, children: "Add existing field" }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, children: [_jsx(Autocomplete, { options: selectableFields, value: inlineState.fieldId
                                                                ? selectableFields.find((field) => field.id === inlineState.fieldId) ?? null
                                                                : null, onChange: (_, value) => handleExistingFieldSelect(table.id, value), getOptionLabel: (option) => `${option.name} (${option.fieldType})`, isOptionEqualToValue: (option, value) => option.id === value?.id, renderInput: (params) => (_jsx(TextField, { ...params, label: "Field", error: Boolean(inlineState.error), helperText: inlineState.error, size: "small" })), disabled: fieldActionsDisabled || tableSaving, fullWidth: true }), _jsx(TextField, { label: "Notes", value: inlineState.notes, onChange: (event) => handleInlineNotesChange(table.id, event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 3 })] }) }), _jsxs(DialogActions, { sx: { px: 3, py: 2, gap: 1 }, children: [tableSaving && _jsx(CircularProgress, { size: 18 }), _jsx(Button, { onClick: () => clearInlineRow(table.id), disabled: tableSaving, children: "Cancel" }), _jsx(Button, { variant: "contained", onClick: () => handleInlineSave(table, tableName, inlineState, tableSaving), disabled: fieldActionsDisabled || tableSaving || !inlineState.fieldId, children: "Add field" })] })] })), inlineType === 'create-new' && inlineState?.type === 'create-new' && (_jsxs(_Fragment, { children: [_jsx(DialogTitle, { id: dialogTitleId, children: "Create new field" }), _jsx(DialogContent, { dividers: true, children: _jsxs(Stack, { spacing: 2, children: [_jsx(TextField, { label: "Name", value: inlineState.draft.name, onChange: (event) => handleCreateDraftTextChange(table.id, 'name', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", error: Boolean(inlineState.errors.name), helperText: inlineState.errors.name }), _jsx(TextField, { label: "Description", value: inlineState.draft.description, onChange: (event) => handleCreateDraftTextChange(table.id, 'description', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 3 }), _jsx(TextField, { label: "Application Usage", value: inlineState.draft.applicationUsage, onChange: (event) => handleCreateDraftTextChange(table.id, 'applicationUsage', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 2 }), _jsx(TextField, { label: "Business Definition", value: inlineState.draft.businessDefinition, onChange: (event) => handleCreateDraftTextChange(table.id, 'businessDefinition', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 2 }), _jsx(TextField, { label: "Enterprise Attribute", value: inlineState.draft.enterpriseAttribute, onChange: (event) => handleCreateDraftTextChange(table.id, 'enterpriseAttribute', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small" }), _jsx(TextField, { label: "Field Type", value: inlineState.draft.fieldType, onChange: (event) => handleCreateDraftTextChange(table.id, 'fieldType', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", error: Boolean(inlineState.errors.fieldType), helperText: inlineState.errors.fieldType }), _jsxs(Grid, { container: true, spacing: 2, children: [_jsx(Grid, { item: true, xs: 6, children: _jsx(TextField, { label: "Length", value: inlineState.draft.fieldLength, onChange: (event) => handleCreateDraftTextChange(table.id, 'fieldLength', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", type: "number" }) }), _jsx(Grid, { item: true, xs: 6, children: _jsx(TextField, { label: "Decimal Places", value: inlineState.draft.decimalPlaces, onChange: (event) => handleCreateDraftTextChange(table.id, 'decimalPlaces', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", type: "number" }) })] }), _jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Legal / Regulatory Implications" }), _jsx(TextField, { value: inlineState.draft.legalRegulatoryImplications, onChange: (event) => handleCreateDraftTextChange(table.id, 'legalRegulatoryImplications', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 2 })] }), _jsxs(Stack, { spacing: 1, children: [_jsx(Typography, { variant: "subtitle2", color: "text.secondary", children: "Security Classification" }), _jsx(TextField, { value: inlineState.draft.securityClassification, onChange: (event) => handleCreateDraftTextChange(table.id, 'securityClassification', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small" })] }), _jsx(TextField, { label: "Data Validation", value: inlineState.draft.dataValidation, onChange: (event) => handleCreateDraftTextChange(table.id, 'dataValidation', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 2 }), _jsx(TextField, { label: "Reference Table", value: inlineState.draft.referenceTable, onChange: (event) => handleCreateDraftTextChange(table.id, 'referenceTable', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small" }), _jsx(TextField, { label: "Grouping Tab", value: inlineState.draft.groupingTab, onChange: (event) => handleCreateDraftTextChange(table.id, 'groupingTab', event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small" }), _jsx(TextField, { label: "Notes", value: inlineState.notes, onChange: (event) => handleInlineNotesChange(table.id, event.target.value), disabled: fieldActionsDisabled || tableSaving, fullWidth: true, size: "small", multiline: true, minRows: 3 }), _jsx(FormControlLabel, { control: _jsx(Checkbox, { checked: inlineState.draft.systemRequired, onChange: (event) => handleCreateDraftBooleanChange(table.id, 'systemRequired', event.target.checked), disabled: fieldActionsDisabled || tableSaving }), label: "System Required" }), _jsx(FormControlLabel, { control: _jsx(Checkbox, { checked: inlineState.draft.businessProcessRequired, onChange: (event) => handleCreateDraftBooleanChange(table.id, 'businessProcessRequired', event.target.checked), disabled: fieldActionsDisabled || tableSaving }), label: "Business Process Required" }), _jsx(FormControlLabel, { control: _jsx(Checkbox, { checked: inlineState.draft.suppressedField, onChange: (event) => handleCreateDraftBooleanChange(table.id, 'suppressedField', event.target.checked), disabled: fieldActionsDisabled || tableSaving }), label: "Suppressed" }), _jsx(FormControlLabel, { control: _jsx(Checkbox, { checked: inlineState.draft.active, onChange: (event) => handleCreateDraftBooleanChange(table.id, 'active', event.target.checked), disabled: fieldActionsDisabled || tableSaving }), label: "Active" })] }) }), _jsxs(DialogActions, { sx: { px: 3, py: 2, gap: 1 }, children: [tableSaving && _jsx(CircularProgress, { size: 18 }), _jsx(Button, { onClick: () => clearInlineRow(table.id), disabled: tableSaving, children: "Cancel" }), _jsx(Button, { variant: "contained", onClick: () => handleInlineSave(table, tableName, inlineState, tableSaving), disabled: fieldActionsDisabled || tableSaving || !inlineState.draft.name.trim() || !inlineState.draft.fieldType.trim(), children: "Create field" })] })] }))] })] }) }, table.id));
            }), _jsx(ConnectionDataPreviewDialog, { open: previewOpen, schemaName: previewSchemaName, tableName: previewTableName, loading: previewLoading, error: previewError, preview: previewData, onClose: handleClosePreview, onRefresh: handleRefreshPreview })] }));
};
export default DataDefinitionDetails;
