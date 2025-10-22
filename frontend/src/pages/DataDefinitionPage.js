import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { Alert, Autocomplete, Box, Button, CircularProgress, Paper, Stack, TextField, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import DataDefinitionDetails from '../components/data-definition/DataDefinitionDetails';
import DataDefinitionForm from '../components/data-definition/DataDefinitionForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useAuth } from '../context/AuthContext';
import { useDataDefinition } from '../hooks/useDataDefinition';
import { useDataObjects } from '../hooks/useDataObjects';
import { useToast } from '../hooks/useToast';
import CreateFieldDialog from '../components/data-definition/CreateFieldDialog';
import { createField, fetchFields, fetchTables, updateField } from '../services/tableService';
import { fetchProcessAreas } from '../services/processAreaService';
import { updateDataDefinition as updateDataDefinitionRequest } from '../services/dataDefinitionService';
const getErrorMessage = (error, fallback) => {
    if (error instanceof AxiosError) {
        return error.response?.data?.detail ?? error.message;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return fallback;
};
const sanitizeOptionalString = (value) => {
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
};
const parseOptionalNumber = (value) => {
    const trimmed = value.trim();
    if (trimmed === '') {
        return { value: null, valid: true };
    }
    const parsed = Number(trimmed);
    if (Number.isNaN(parsed)) {
        return { value: null, valid: false };
    }
    return { value: parsed, valid: true };
};
const DataDefinitionsPage = () => {
    const { hasRole } = useAuth();
    const canManage = hasRole('admin');
    const toast = useToast();
    const queryClient = useQueryClient();
    const { dataObjectsQuery } = useDataObjects();
    const { data: dataObjects = [], isLoading: dataObjectsLoading, isError: dataObjectsError, error: dataObjectsErrorDetails } = dataObjectsQuery;
    const tablesQuery = useQuery(['tables'], fetchTables);
    const { data: tables = [], isLoading: tablesLoading, isError: tablesError, error: tablesErrorDetails } = tablesQuery;
    const fieldsQuery = useQuery(['fields'], fetchFields);
    const { data: fields = [], isLoading: fieldsLoading, isError: fieldsError, error: fieldsErrorDetails } = fieldsQuery;
    const processAreasQuery = useQuery(['processAreas'], fetchProcessAreas);
    const { data: processAreas = [], isLoading: processAreasLoading, isError: processAreasError, error: processAreasErrorDetails } = processAreasQuery;
    const refreshMetadata = useCallback(async () => {
        await Promise.all([tablesQuery.refetch(), fieldsQuery.refetch()]);
    }, [tablesQuery, fieldsQuery]);
    const [selectedDataObjectId, setSelectedDataObjectId] = useState(null);
    const [selectedSystemId, setSelectedSystemId] = useState(null);
    const [selectedProcessAreaId, setSelectedProcessAreaId] = useState(null);
    const [formOpen, setFormOpen] = useState(false);
    const [formMode, setFormMode] = useState('create');
    const [confirmOpen, setConfirmOpen] = useState(false);
    const [fieldEditDialog, setFieldEditDialog] = useState(null);
    const [fieldEditLoading, setFieldEditLoading] = useState(false);
    const [inlineSavingFields, setInlineSavingFields] = useState({});
    const [tableSavingState, setTableSavingState] = useState({});
    useEffect(() => {
        if (!processAreas.length) {
            setSelectedProcessAreaId(null);
            return;
        }
        if (!selectedProcessAreaId || !processAreas.some((area) => area.id === selectedProcessAreaId)) {
            setSelectedProcessAreaId(processAreas[0].id);
        }
    }, [processAreas, selectedProcessAreaId]);
    const filteredDataObjects = useMemo(() => {
        if (!selectedProcessAreaId) {
            return dataObjects;
        }
        return dataObjects.filter((item) => item.processAreaId === selectedProcessAreaId);
    }, [dataObjects, selectedProcessAreaId]);
    useEffect(() => {
        if (!filteredDataObjects.length) {
            setSelectedDataObjectId(null);
            return;
        }
        if (!selectedDataObjectId || !filteredDataObjects.some((item) => item.id === selectedDataObjectId)) {
            setSelectedDataObjectId(filteredDataObjects[0].id);
        }
    }, [filteredDataObjects, selectedDataObjectId]);
    const selectedDataObject = useMemo(() => {
        return filteredDataObjects.find((item) => item.id === selectedDataObjectId) ?? null;
    }, [filteredDataObjects, selectedDataObjectId]);
    const systemOptions = useMemo(() => selectedDataObject?.systems ?? [], [selectedDataObject]);
    useEffect(() => {
        if (!systemOptions.length) {
            setSelectedSystemId(null);
            return;
        }
        if (!selectedSystemId || !systemOptions.some((system) => system.id === selectedSystemId)) {
            setSelectedSystemId(systemOptions[0].id);
        }
    }, [systemOptions, selectedSystemId]);
    const dataObjectId = selectedDataObjectId ?? undefined;
    const systemId = selectedSystemId ?? undefined;
    const { definitionQuery, createDataDefinition, updateDataDefinition: updateDefinitionMutation, deleteDataDefinition, creating, updating, deleting, createRelationship, updateRelationship, deleteRelationship, relationshipCreating, relationshipUpdating, relationshipDeleting } = useDataDefinition(dataObjectId, systemId);
    const fieldUpdateMutation = useMutation(({ fieldId, input }) => updateField(fieldId, input), {
        onSuccess: async () => {
            await Promise.all([
                queryClient.invalidateQueries(['fields']),
                definitionQuery.refetch()
            ]);
        }
    });
    const definition = definitionQuery.data ?? null;
    const relationshipBusy = relationshipCreating || relationshipUpdating || relationshipDeleting;
    const definitionLoading = Boolean(systemId && definitionQuery.isLoading);
    const definitionFetching = Boolean(systemId && definitionQuery.isFetching);
    const definitionErrorMessage = definitionQuery.isError
        ? getErrorMessage(definitionQuery.error, 'Unable to load the data definition.')
        : null;
    const tablesForSystem = useMemo(() => {
        if (!systemId)
            return [];
        return tables.filter((table) => table.systemId === systemId);
    }, [systemId, tables]);
    const fieldsForSystem = useMemo(() => {
        if (!tablesForSystem.length)
            return [];
        const tableIds = new Set(tablesForSystem.map((table) => table.id));
        return fields.filter((field) => tableIds.has(field.tableId));
    }, [fields, tablesForSystem]);
    const availableFieldsByTable = useMemo(() => {
        return fieldsForSystem.reduce((accumulator, field) => {
            if (!accumulator[field.tableId]) {
                accumulator[field.tableId] = [];
            }
            accumulator[field.tableId].push(field);
            return accumulator;
        }, {});
    }, [fieldsForSystem]);
    const busy = creating || updating || deleting;
    const metadataLoading = tablesLoading || fieldsLoading;
    const canInlineEdit = canManage && !busy && !metadataLoading;
    const anyTableSaving = Object.values(tableSavingState).some(Boolean);
    const fieldActionsDisabled = !canManage || busy || metadataLoading || fieldEditLoading || anyTableSaving;
    const handleCreateRelationship = useCallback(async (input) => {
        if (!definition) {
            toast.showError('Data definition is not available.');
            return false;
        }
        try {
            await createRelationship({ definitionId: definition.id, input });
            return true;
        }
        catch {
            return false;
        }
    }, [createRelationship, definition, toast]);
    const handleUpdateRelationship = useCallback(async (relationshipId, input) => {
        if (!definition) {
            toast.showError('Data definition is not available.');
            return false;
        }
        try {
            await updateRelationship({ definitionId: definition.id, relationshipId, input });
            return true;
        }
        catch {
            return false;
        }
    }, [definition, toast, updateRelationship]);
    const handleDeleteRelationship = useCallback(async (relationshipId) => {
        if (!definition) {
            toast.showError('Data definition is not available.');
            return false;
        }
        try {
            await deleteRelationship({ definitionId: definition.id, relationshipId });
            return true;
        }
        catch {
            return false;
        }
    }, [definition, deleteRelationship, toast]);
    const appendFieldsToDefinition = useCallback(async (tableId, additions) => {
        if (!definition) {
            toast.showError('Data definition is not available.');
            return false;
        }
        if (!additions.length) {
            return true;
        }
        let additionsApplied = false;
        const tablesPayload = definition.tables.map((table) => {
            const existingFields = table.fields.map((field) => ({
                fieldId: field.fieldId,
                notes: field.notes ?? null
            }));
            const newEntries = table.tableId === tableId
                ? additions.filter((addition) => !existingFields.some((existing) => existing.fieldId === addition.fieldId))
                : [];
            if (table.tableId === tableId && newEntries.length > 0) {
                additionsApplied = true;
            }
            return {
                tableId: table.tableId,
                alias: table.alias ?? null,
                description: table.description ?? null,
                loadOrder: table.loadOrder ?? null,
                fields: table.tableId === tableId ? [...existingFields, ...newEntries] : existingFields
            };
        });
        if (!additionsApplied) {
            toast.showInfo('The selected field is already part of the data definition.');
            return false;
        }
        try {
            await updateDataDefinitionRequest(definition.id, {
                tables: tablesPayload
            });
            await Promise.all([definitionQuery.refetch(), fieldsQuery.refetch()]);
            return true;
        }
        catch (error) {
            toast.showError(getErrorMessage(error, 'Unable to update the data definition.'));
            return false;
        }
    }, [definition, definitionQuery, fieldsQuery, toast]);
    const handleInlineFieldSubmit = useCallback(async (field, changes) => {
        if (!canManage) {
            return false;
        }
        const payload = {};
        let hasChange = false;
        const applyRequiredString = (value, current, key, label) => {
            if (value === undefined) {
                return true;
            }
            const trimmed = value.trim();
            if (!trimmed) {
                toast.showError(`${label} is required.`);
                return false;
            }
            if (trimmed !== current) {
                payload[key] = trimmed;
                hasChange = true;
            }
            return true;
        };
        if (!applyRequiredString(changes.name, field.name, 'name', 'Field name')) {
            return false;
        }
        if (!applyRequiredString(changes.fieldType, field.fieldType, 'fieldType', 'Field type')) {
            return false;
        }
        const applyOptionalString = (value, current, key) => {
            if (value === undefined) {
                return;
            }
            const sanitized = sanitizeOptionalString(value);
            const normalizedCurrent = (current ?? null);
            if (sanitized !== normalizedCurrent) {
                payload[key] = sanitized;
                hasChange = true;
            }
        };
        applyOptionalString(changes.description, field.description, 'description');
        applyOptionalString(changes.applicationUsage, field.applicationUsage, 'applicationUsage');
        applyOptionalString(changes.businessDefinition, field.businessDefinition, 'businessDefinition');
        applyOptionalString(changes.enterpriseAttribute, field.enterpriseAttribute, 'enterpriseAttribute');
        applyOptionalString(changes.legalRegulatoryImplications, field.legalRegulatoryImplications, 'legalRegulatoryImplications');
        applyOptionalString(changes.securityClassification, field.securityClassification, 'securityClassification');
        applyOptionalString(changes.dataValidation, field.dataValidation, 'dataValidation');
        applyOptionalString(changes.referenceTable, field.referenceTable, 'referenceTable');
        applyOptionalString(changes.groupingTab, field.groupingTab, 'groupingTab');
        const applyOptionalInteger = (value, current, key, label) => {
            if (value === undefined) {
                return true;
            }
            const parsed = parseOptionalNumber(value);
            if (!parsed.valid) {
                toast.showError(`${label} must be a number.`);
                return false;
            }
            if (parsed.value !== null && parsed.value < 0) {
                toast.showError(`${label} must be zero or greater.`);
                return false;
            }
            const normalizedCurrent = current ?? null;
            if (parsed.value !== normalizedCurrent) {
                payload[key] = parsed.value;
                hasChange = true;
            }
            return true;
        };
        if (!applyOptionalInteger(changes.fieldLength, field.fieldLength, 'fieldLength', 'Field length')) {
            return false;
        }
        if (!applyOptionalInteger(changes.decimalPlaces, field.decimalPlaces, 'decimalPlaces', 'Decimal places')) {
            return false;
        }
        const applyBoolean = (value, current, key) => {
            if (value === undefined || value === current) {
                return;
            }
            payload[key] = value;
            hasChange = true;
        };
        applyBoolean(changes.systemRequired, field.systemRequired, 'systemRequired');
        applyBoolean(changes.businessProcessRequired, field.businessProcessRequired, 'businessProcessRequired');
        applyBoolean(changes.suppressedField, field.suppressedField, 'suppressedField');
        applyBoolean(changes.active, field.active, 'active');
        if (!hasChange) {
            return true;
        }
        setInlineSavingFields((prev) => ({ ...prev, [field.id]: true }));
        try {
            await fieldUpdateMutation.mutateAsync({ fieldId: field.id, input: payload });
            toast.showSuccess('Field updated.');
            return true;
        }
        catch (error) {
            toast.showError(getErrorMessage(error, 'Unable to update the field.'));
            return false;
        }
        finally {
            setInlineSavingFields((prev) => {
                const next = { ...prev };
                delete next[field.id];
                return next;
            });
        }
    }, [canManage, fieldUpdateMutation, toast]);
    const handleOpenFieldEdit = useCallback((tableId, tableName, field) => {
        setFieldEditDialog({ tableId, tableName, field });
    }, []);
    const handleFieldDialogClose = useCallback(() => {
        if (fieldEditLoading) {
            return;
        }
        setFieldEditDialog(null);
    }, [fieldEditLoading]);
    const handleFieldDialogSubmit = useCallback(async (values) => {
        if (!fieldEditDialog) {
            return;
        }
        setFieldEditLoading(true);
        const success = await handleInlineFieldSubmit(fieldEditDialog.field, {
            name: values.name,
            description: values.description,
            applicationUsage: values.applicationUsage,
            businessDefinition: values.businessDefinition,
            enterpriseAttribute: values.enterpriseAttribute,
            fieldType: values.fieldType,
            fieldLength: values.fieldLength,
            decimalPlaces: values.decimalPlaces,
            systemRequired: values.systemRequired,
            businessProcessRequired: values.businessProcessRequired,
            suppressedField: values.suppressedField,
            active: values.active,
            legalRegulatoryImplications: values.legalRegulatoryImplications,
            securityClassification: values.securityClassification,
            dataValidation: values.dataValidation,
            referenceTable: values.referenceTable,
            groupingTab: values.groupingTab
        });
        setFieldEditLoading(false);
        if (success) {
            setFieldEditDialog(null);
        }
    }, [fieldEditDialog, handleInlineFieldSubmit]);
    const handleAddExistingFieldInline = useCallback(async ({ tableId, fieldId, notes }) => {
        setTableSavingState((prev) => ({ ...prev, [tableId]: true }));
        try {
            const updated = await appendFieldsToDefinition(tableId, [
                { fieldId, notes: sanitizeOptionalString(notes) }
            ]);
            if (updated) {
                toast.showSuccess('Field added to the data definition.');
            }
            return updated;
        }
        finally {
            setTableSavingState((prev) => {
                const next = { ...prev };
                delete next[tableId];
                return next;
            });
        }
    }, [appendFieldsToDefinition, toast]);
    const handleCreateFieldInline = useCallback(async ({ tableId, field, notes, suppressNotifications }) => {
        const trimmedName = field.name.trim();
        const trimmedFieldType = field.fieldType.trim();
        if (!trimmedName) {
            if (!suppressNotifications) {
                toast.showError('Field name is required.');
            }
            return false;
        }
        if (!trimmedFieldType) {
            if (!suppressNotifications) {
                toast.showError('Field type is required.');
            }
            return false;
        }
        const lengthParsed = parseOptionalNumber(field.fieldLength);
        if (!lengthParsed.valid) {
            if (!suppressNotifications) {
                toast.showError('Field length must be a number.');
            }
            return false;
        }
        if (lengthParsed.value !== null && lengthParsed.value < 0) {
            if (!suppressNotifications) {
                toast.showError('Field length must be zero or greater.');
            }
            return false;
        }
        const decimalParsed = parseOptionalNumber(field.decimalPlaces);
        if (!decimalParsed.valid) {
            if (!suppressNotifications) {
                toast.showError('Decimal places must be a number.');
            }
            return false;
        }
        if (decimalParsed.value !== null && decimalParsed.value < 0) {
            if (!suppressNotifications) {
                toast.showError('Decimal places must be zero or greater.');
            }
            return false;
        }
        setTableSavingState((prev) => ({ ...prev, [tableId]: true }));
        try {
            const newField = await createField({
                tableId,
                name: trimmedName,
                description: sanitizeOptionalString(field.description),
                applicationUsage: sanitizeOptionalString(field.applicationUsage),
                businessDefinition: sanitizeOptionalString(field.businessDefinition),
                enterpriseAttribute: sanitizeOptionalString(field.enterpriseAttribute),
                fieldType: trimmedFieldType,
                fieldLength: lengthParsed.value,
                decimalPlaces: decimalParsed.value,
                systemRequired: field.systemRequired,
                businessProcessRequired: field.businessProcessRequired,
                suppressedField: field.suppressedField,
                active: field.active,
                legalRegulatoryImplications: sanitizeOptionalString(field.legalRegulatoryImplications),
                securityClassification: sanitizeOptionalString(field.securityClassification),
                dataValidation: sanitizeOptionalString(field.dataValidation),
                referenceTable: sanitizeOptionalString(field.referenceTable),
                groupingTab: sanitizeOptionalString(field.groupingTab)
            });
            const updated = await appendFieldsToDefinition(tableId, [
                { fieldId: newField.id, notes: sanitizeOptionalString(notes) }
            ]);
            if (updated && !suppressNotifications) {
                toast.showSuccess('Field created and added to the data definition.');
            }
            return updated;
        }
        catch (error) {
            toast.showError(getErrorMessage(error, 'Unable to create the field.'));
            return false;
        }
        finally {
            setTableSavingState((prev) => {
                const next = { ...prev };
                delete next[tableId];
                return next;
            });
        }
    }, [appendFieldsToDefinition, toast]);
    const handleBulkPasteResult = useCallback(({ tableName, succeeded, failed }) => {
        if (succeeded > 0) {
            toast.showSuccess(`${succeeded} field${succeeded === 1 ? '' : 's'} added to ${tableName}.`);
        }
        if (failed > 0) {
            toast.showError(`${failed} row${failed === 1 ? '' : 's'} could not be processed. Ensure Field Name and Field Type are provided.`);
        }
    }, [toast]);
    const handleOpenCreate = () => {
        setFormMode('create');
        setFormOpen(true);
    };
    const handleOpenEdit = () => {
        setFormMode('edit');
        setFormOpen(true);
    };
    const handleFormClose = () => {
        setFormOpen(false);
    };
    const handleFormSubmit = async (values) => {
        if (!dataObjectId || !systemId)
            return;
        try {
            if (formMode === 'create') {
                await createDataDefinition({
                    dataObjectId,
                    systemId,
                    description: values.description,
                    tables: values.tables
                });
            }
            else if (definition) {
                await updateDefinitionMutation({
                    id: definition.id,
                    input: {
                        description: values.description ?? null,
                        tables: values.tables
                    }
                });
            }
            setFormOpen(false);
        }
        catch (error) {
            // handled by toast notifications
        }
    };
    const handleDelete = async () => {
        if (!definition)
            return;
        try {
            await deleteDataDefinition(definition.id);
            setConfirmOpen(false);
        }
        catch (error) {
            // handled by toast notifications
        }
    };
    const canCreateDefinition = Boolean(selectedDataObject && selectedSystemId && systemOptions.length > 0);
    const noSystemsAssigned = Boolean(selectedDataObject && systemOptions.length === 0);
    const dataObjectsErrorMessage = dataObjectsError
        ? getErrorMessage(dataObjectsErrorDetails, 'Unable to load data objects.')
        : null;
    const tablesErrorMessage = tablesError
        ? getErrorMessage(tablesErrorDetails, 'Unable to load tables.')
        : null;
    const fieldsErrorMessage = fieldsError
        ? getErrorMessage(fieldsErrorDetails, 'Unable to load fields.')
        : null;
    const processAreasErrorMessage = processAreasError
        ? getErrorMessage(processAreasErrorDetails, 'Unable to load process areas.')
        : null;
    const theme = useTheme();
    return (_jsxs(Box, { children: [_jsxs(Box, { sx: {
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
                    borderBottom: `3px solid ${theme.palette.primary.main}`,
                    borderRadius: '12px',
                    p: 3,
                    mb: 3,
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
                }, children: [_jsx(Typography, { variant: "h4", gutterBottom: true, sx: { color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }, children: "Data Definitions" }), _jsx(Typography, { variant: "body2", sx: { color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }, children: "Select a data object and system to review or author its data definition, including tables and fields." })] }), _jsxs(Paper, { elevation: 3, sx: {
                    p: 3,
                    mb: 3,
                    backgroundColor: alpha(theme.palette.warning.main, 0.04),
                    borderColor: alpha(theme.palette.warning.main, 0.25),
                    borderWidth: 2,
                    borderStyle: 'solid',
                    borderRadius: 2,
                    boxShadow: `0 2px 8px ${alpha(theme.palette.warning.main, 0.1)}`
                }, children: [_jsx(Typography, { variant: "subtitle1", sx: {
                            fontWeight: 700,
                            color: theme.palette.primary.dark,
                            mb: 2.5,
                            letterSpacing: 0.5,
                            fontSize: '1.1rem'
                        }, children: "Filter by Process Area, Data Object & System" }), _jsxs(Stack, { spacing: 2, children: [_jsx(Autocomplete, { options: processAreas, value: processAreas.find((pa) => pa.id === selectedProcessAreaId) ?? null, loading: processAreasLoading, onChange: (_, value) => {
                                    setSelectedProcessAreaId(value?.id ?? null);
                                }, getOptionLabel: (option) => option.name, isOptionEqualToValue: (option, value) => option.id === value?.id, renderInput: (params) => (_jsx(TextField, { ...params, label: "Process Area", placeholder: processAreasLoading ? 'Loading…' : 'Select process area', sx: {
                                        '& .MuiOutlinedInput-root': {
                                            backgroundColor: 'white',
                                            borderColor: alpha(theme.palette.primary.main, 0.3),
                                            '&:hover': {
                                                borderColor: alpha(theme.palette.primary.main, 0.5),
                                                backgroundColor: alpha(theme.palette.primary.main, 0.01)
                                            },
                                            '&.Mui-focused': {
                                                borderColor: theme.palette.primary.main,
                                                boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.1)}`
                                            }
                                        },
                                        '& .MuiOutlinedInput-input': {
                                            fontSize: '0.95rem'
                                        },
                                        '& .MuiInputLabel-root': {
                                            color: theme.palette.primary.dark,
                                            fontWeight: 600,
                                            fontSize: '1.1rem',
                                            '&.MuiInputLabel-shrink': {
                                                fontSize: '0.85rem'
                                            }
                                        }
                                    } })) }), _jsx(Autocomplete, { options: filteredDataObjects, value: selectedDataObject, loading: dataObjectsLoading, onChange: (_, value) => {
                                    setSelectedDataObjectId(value?.id ?? null);
                                }, getOptionLabel: (option) => option.name, isOptionEqualToValue: (option, value) => option.id === value?.id, renderInput: (params) => (_jsx(TextField, { ...params, label: "Data Object", placeholder: dataObjectsLoading ? 'Loading…' : 'Select data object', sx: {
                                        '& .MuiOutlinedInput-root': {
                                            backgroundColor: 'white',
                                            borderColor: alpha(theme.palette.primary.main, 0.3),
                                            '&:hover': {
                                                borderColor: alpha(theme.palette.primary.main, 0.5),
                                                backgroundColor: alpha(theme.palette.primary.main, 0.01)
                                            },
                                            '&.Mui-focused': {
                                                borderColor: theme.palette.primary.main,
                                                boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.1)}`
                                            }
                                        },
                                        '& .MuiOutlinedInput-input': {
                                            fontSize: '0.95rem'
                                        },
                                        '& .MuiInputLabel-root': {
                                            color: theme.palette.primary.dark,
                                            fontWeight: 600,
                                            fontSize: '1.1rem',
                                            '&.MuiInputLabel-shrink': {
                                                fontSize: '0.85rem'
                                            }
                                        }
                                    } })) }), _jsx(Autocomplete, { options: systemOptions, value: systemOptions.find((system) => system.id === selectedSystemId) ?? null, loading: dataObjectsLoading, onChange: (_, value) => setSelectedSystemId(value?.id ?? null), getOptionLabel: (option) => option.name, isOptionEqualToValue: (option, value) => option.id === value?.id, renderInput: (params) => (_jsx(TextField, { ...params, label: "System", placeholder: systemOptions.length ? 'Select system' : 'No systems assigned', disabled: !selectedDataObject, sx: {
                                        '& .MuiOutlinedInput-root': {
                                            backgroundColor: 'white',
                                            borderColor: alpha(theme.palette.primary.main, 0.3),
                                            '&:hover': {
                                                borderColor: alpha(theme.palette.primary.main, 0.5),
                                                backgroundColor: alpha(theme.palette.primary.main, 0.01)
                                            },
                                            '&.Mui-focused': {
                                                borderColor: theme.palette.primary.main,
                                                boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.1)}`
                                            }
                                        },
                                        '& .MuiOutlinedInput-input': {
                                            fontSize: '0.95rem'
                                        },
                                        '& .MuiInputLabel-root': {
                                            color: theme.palette.primary.dark,
                                            fontWeight: 600,
                                            fontSize: '1.1rem',
                                            '&.MuiInputLabel-shrink': {
                                                fontSize: '0.85rem'
                                            }
                                        }
                                    } })) })] })] }), _jsxs(Stack, { spacing: 2, sx: { mb: 3 }, children: [processAreasErrorMessage && _jsx(Alert, { severity: "error", children: processAreasErrorMessage }), dataObjectsErrorMessage && _jsx(Alert, { severity: "error", children: dataObjectsErrorMessage }), tablesErrorMessage && _jsx(Alert, { severity: "error", children: tablesErrorMessage }), fieldsErrorMessage && _jsx(Alert, { severity: "error", children: fieldsErrorMessage }), definitionErrorMessage && _jsx(Alert, { severity: "error", children: definitionErrorMessage }), noSystemsAssigned && (_jsx(Alert, { severity: "info", children: "Assign the selected data object to at least one system before creating a data definition." }))] }), _jsx(Paper, { elevation: 1, sx: { p: 3 }, children: !selectedDataObject || !selectedSystemId ? (_jsx(Typography, { variant: "body2", color: "text.secondary", children: "Select a data object and system to view its data definition." })) : definitionLoading && !definition ? (_jsxs(Stack, { alignItems: "center", spacing: 1, py: 4, children: [_jsx(CircularProgress, { size: 24 }), _jsx(Typography, { variant: "body2", color: "text.secondary", children: "Loading data definition\u2026" })] })) : definition ? (_jsxs(Stack, { spacing: 3, children: [_jsxs(Paper, { elevation: 0, sx: {
                                backgroundColor: alpha(theme.palette.primary.main, 0.06),
                                borderRadius: 2,
                                overflow: 'hidden'
                            }, children: [_jsx(Box, { sx: {
                                        background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.1)} 0%, ${alpha(theme.palette.info.main, 0.06)} 100%)`,
                                        borderLeft: `4px solid ${theme.palette.info.main}`,
                                        p: 2.5
                                    }, children: _jsxs(Stack, { direction: "row", justifyContent: "space-between", alignItems: "center", children: [_jsx(Typography, { variant: "h6", component: "div", sx: {
                                                    color: theme.palette.primary.dark,
                                                    fontWeight: 700,
                                                    fontSize: '1.1rem',
                                                    letterSpacing: 0.3
                                                }, children: "Current Definition" }), canManage && (_jsxs(Stack, { direction: "row", spacing: 1, children: [_jsx(Button, { variant: "outlined", onClick: handleOpenEdit, disabled: busy || metadataLoading, children: "Edit" }), _jsx(Button, { variant: "outlined", color: "error", onClick: () => setConfirmOpen(true), disabled: busy, children: "Delete" })] }))] }) }), _jsx(DataDefinitionDetails, { definition: definition, canEdit: canInlineEdit, inlineSavingState: inlineSavingFields, onInlineFieldSubmit: handleInlineFieldSubmit, onEditField: handleOpenFieldEdit, onAddExistingFieldInline: handleAddExistingFieldInline, onCreateFieldInline: handleCreateFieldInline, availableFieldsByTable: availableFieldsByTable, tableSavingState: tableSavingState, fieldActionsDisabled: fieldActionsDisabled, relationshipBusy: relationshipBusy, onCreateRelationship: handleCreateRelationship, onUpdateRelationship: handleUpdateRelationship, onDeleteRelationship: handleDeleteRelationship, onBulkPasteResult: handleBulkPasteResult })] }), definitionFetching && (_jsx(Typography, { variant: "caption", color: "text.secondary", children: "Refreshing\u2026" }))] })) : (_jsxs(Stack, { spacing: 2, children: [_jsx(Alert, { severity: "info", children: "No data definition exists for this data object and system yet." }), canManage && (_jsx(Button, { variant: "contained", onClick: handleOpenCreate, disabled: !canCreateDefinition || busy || metadataLoading, children: "Create Data Definition" }))] })) }), canManage && selectedSystemId && (_jsx(DataDefinitionForm, { open: formOpen, mode: formMode, loading: formMode === 'create' ? creating : updating, onClose: handleFormClose, onSubmit: handleFormSubmit, initialDefinition: formMode === 'edit' ? definition : null, tables: tablesForSystem, systemId: selectedSystemId, dataObjectId: dataObjectId, onMetadataRefresh: refreshMetadata })), fieldEditDialog && (_jsx(CreateFieldDialog, { open: true, mode: "edit", tableName: fieldEditDialog.tableName, loading: fieldEditLoading, initialValues: {
                    name: fieldEditDialog.field.name,
                    description: fieldEditDialog.field.description ?? null,
                    applicationUsage: fieldEditDialog.field.applicationUsage ?? null,
                    businessDefinition: fieldEditDialog.field.businessDefinition ?? null,
                    enterpriseAttribute: fieldEditDialog.field.enterpriseAttribute ?? null,
                    fieldType: fieldEditDialog.field.fieldType,
                    fieldLength: fieldEditDialog.field.fieldLength != null
                        ? fieldEditDialog.field.fieldLength.toString()
                        : null,
                    decimalPlaces: fieldEditDialog.field.decimalPlaces != null
                        ? fieldEditDialog.field.decimalPlaces.toString()
                        : null,
                    systemRequired: fieldEditDialog.field.systemRequired,
                    businessProcessRequired: fieldEditDialog.field.businessProcessRequired,
                    suppressedField: fieldEditDialog.field.suppressedField,
                    active: fieldEditDialog.field.active,
                    legalRegulatoryImplications: fieldEditDialog.field.legalRegulatoryImplications ?? null,
                    securityClassification: fieldEditDialog.field.securityClassification ?? null,
                    dataValidation: fieldEditDialog.field.dataValidation ?? null,
                    referenceTable: fieldEditDialog.field.referenceTable ?? null,
                    groupingTab: fieldEditDialog.field.groupingTab ?? null
                }, onClose: handleFieldDialogClose, onSubmit: handleFieldDialogSubmit })), _jsx(ConfirmDialog, { open: confirmOpen, title: "Delete Data Definition", description: "Are you sure you want to delete this data definition? This action cannot be undone.", confirmLabel: "Delete", onClose: () => setConfirmOpen(false), onConfirm: handleDelete })] }));
};
export default DataDefinitionsPage;
