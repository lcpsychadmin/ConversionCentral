import { ChangeEvent, ClipboardEvent, MouseEvent, useCallback, useEffect, useMemo, useState } from 'react';
import {
	Autocomplete,
	Box,
	Button,
	Chip,
	Checkbox,
	CircularProgress,
	Collapse,
	Dialog,
	DialogActions,
	DialogContent,
	DialogTitle,
	FormControlLabel,
	Grid,
	IconButton,
	InputAdornment,
	Link,
	ListItemIcon,
	ListItemText,
	Menu,
	MenuItem,
	Paper,
	Stack,
	TextField,
	Typography
} from '@mui/material';
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
import { DataGrid, GridColDef, GridRowClassNameParams, useGridApiContext } from '@mui/x-data-grid';

import DataDefinitionRelationshipBuilder from './DataDefinitionRelationshipBuilder';
import {
	DataDefinition,
	DataDefinitionField,
	DataDefinitionRelationshipInput,
	DataDefinitionRelationshipUpdateInput,
	DataDefinitionTable,
	Field
} from '../../types/data';

type FieldDraft = {
	name: string;
	description: string;
	applicationUsage: string;
	businessDefinition: string;
	enterpriseAttribute: string;
	fieldType: string;
	fieldLength: string;
	decimalPlaces: string;
	legalRegulatoryImplications: string;
	securityClassification: string;
	dataValidation: string;
	referenceTable: string;
	groupingTab: string;
	systemRequired: boolean;
	businessProcessRequired: boolean;
	suppressedField: boolean;
	active: boolean;
};

type BooleanDraftKey = 'systemRequired' | 'businessProcessRequired' | 'suppressedField' | 'active';
type StringDraftKey = Exclude<keyof FieldDraft, BooleanDraftKey>;
type NumberColumnKey = Extract<StringDraftKey, 'fieldLength' | 'decimalPlaces'>;
type TextColumnKey = Exclude<StringDraftKey, NumberColumnKey>;

type FieldColumn =
	| { key: TextColumnKey; label: string; kind: 'text'; multiline?: boolean; minWidth?: number }
	| { key: NumberColumnKey; label: string; kind: 'number'; minWidth?: number }
	| { key: BooleanDraftKey; label: string; kind: 'boolean' };

const FIELD_COLUMNS: FieldColumn[] = [
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

const BOOLEAN_FIELD_KEYS: BooleanDraftKey[] = [
	'systemRequired',
	'businessProcessRequired',
	'suppressedField',
	'active'
];

const buildDraft = (field: Field): FieldDraft => ({
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

const mapDraftsFromDefinition = (definition: DataDefinition) => {
	const drafts: Record<string, FieldDraft> = {};
	definition.tables.forEach((table) => {
		table.fields.forEach((definitionField) => {
			drafts[definitionField.field.id] = buildDraft(definitionField.field);
		});
	});
	return drafts;
};

const createEmptyDraft = (): FieldDraft => ({
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

type InlineExistingState = {
	type: 'add-existing';
	fieldId: string | null;
	notes: string;
	error?: string;
};

type InlineCreateState = {
	type: 'create-new';
	draft: FieldDraft;
	notes: string;
	errors: Partial<Record<StringDraftKey | 'notes', string>>;
};

type InlineRowState = InlineExistingState | InlineCreateState;

type FieldRow = FieldDraft & {
	id: string;
	notes: string;
	__definitionField?: DataDefinitionField;
	__isPlaceholder?: boolean;
};

const resolveRowClassName = (params: GridRowClassNameParams<FieldRow>) => {
	if (params.row.__isPlaceholder) {
		return 'placeholder-row';
	}
	return '';
};

const renderNumber = (value: number | string | null | undefined) => {
	if (value === null || value === undefined || value === '') {
		return '—';
	}
	return value;
};

const renderText = (value: string | null | undefined) => {
	if (!value) {
		return '—';
	}
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : '—';
};

const toCsvString = (value: string | number | boolean | null | undefined) => {
	if (value === null || value === undefined) {
		return '""';
	}
	if (typeof value === 'boolean') {
		return value ? '"Yes"' : '"No"';
	}
	const stringValue = String(value).trim();
	return `"${stringValue.replace(/"/g, '""')}"`;
};

const parseBooleanCell = (value: string | undefined, defaultValue: boolean) => {
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

	const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
		const value = event.target.value;
		setFilterValue(value);
		apiRef.current.setQuickFilterValues(value.split(/\s+/).filter(Boolean));
	};

	return (
		<Stack
			direction={{ xs: 'column', sm: 'row' }}
			justifyContent={{ xs: 'flex-start', sm: 'flex-start' }}
			alignItems={{ xs: 'stretch', sm: 'center' }}
			sx={{
				px: 1.5,
				py: 1,
				gap: 1,
				backgroundColor: alpha(theme.palette.info.main, 0.05),
				borderBottom: `1px solid ${alpha(theme.palette.info.main, 0.15)}`
			}}
		>
			<TextField
				placeholder="Filter list..."
				value={filterValue}
				onChange={handleFilterChange}
				size="small"
				InputProps={{
					startAdornment: (
						<InputAdornment position="start">
							<SearchIcon sx={{ color: alpha(theme.palette.primary.main, 0.5) }} />
						</InputAdornment>
					)
				}}
				sx={{
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
				}}
			/>
		</Stack>
	);
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

type BulkPasteResult = {
	tableId: string;
	tableName: string;
	total: number;
	succeeded: number;
	failed: number;
};

type DataDefinitionDetailsProps = {
	definition: DataDefinition;
	canEdit?: boolean;
	inlineSavingState?: Record<string, boolean>;
	onInlineFieldSubmit?: (field: Field, changes: Partial<Record<StringDraftKey, string> & Record<BooleanDraftKey, boolean>>) => Promise<boolean>;
	onEditField?: (tableId: string, tableName: string, field: Field) => void;
	onAddExistingFieldInline?: (payload: {
		tableId: string;
		tableName: string;
		fieldId: string;
		notes: string;
	}) => Promise<boolean>;
	onCreateFieldInline?: (payload: {
		tableId: string;
		tableName: string;
		field: FieldDraft;
		notes: string;
		suppressNotifications?: boolean;
	}) => Promise<boolean>;
	availableFieldsByTable?: Record<string, Field[]>;
	tableSavingState?: Record<string, boolean>;
	fieldActionsDisabled?: boolean;
	onBulkPasteResult?: (result: BulkPasteResult) => void;
	onCreateRelationship?: (input: DataDefinitionRelationshipInput) => Promise<boolean>;
	onUpdateRelationship?: (relationshipId: string, input: DataDefinitionRelationshipUpdateInput) => Promise<boolean>;
	onDeleteRelationship?: (relationshipId: string) => Promise<boolean>;
	relationshipBusy?: boolean;
};

const DataDefinitionDetails = ({
	definition,
	canEdit = false,
	inlineSavingState,
	onInlineFieldSubmit,
	onEditField,
	onAddExistingFieldInline,
	onCreateFieldInline,
	availableFieldsByTable,
	tableSavingState,
	fieldActionsDisabled = false,
	onBulkPasteResult,
	onCreateRelationship,
	onUpdateRelationship,
	onDeleteRelationship,
	relationshipBusy = false
}: DataDefinitionDetailsProps) => {
	const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>(() => {
		const initial: Record<string, boolean> = {};
		definition.tables.forEach((table) => {
			initial[table.id] = false;
		});
		return initial;
	});

	const initialDrafts = useMemo(() => mapDraftsFromDefinition(definition), [definition]);
	const [drafts, setDrafts] = useState<Record<string, FieldDraft>>(initialDrafts);
	const [committed, setCommitted] = useState<Record<string, FieldDraft>>(initialDrafts);
	const [inlineRows, setInlineRows] = useState<Record<string, InlineRowState>>({});
	const [gridViewByTable, setGridViewByTable] = useState<Record<string, boolean>>({});
	const [filterValue, setFilterValue] = useState('');
	const [newMenu, setNewMenu] = useState<{ tableId: string; anchor: HTMLElement } | null>(null);
	const [activeDialog, setActiveDialog] = useState<{ tableId: string; type: InlineRowState['type'] } | null>(null);
	const [dragSource, setDragSource] = useState<{ tableId: string; fieldId: string } | null>(null);
	const [dropTargetTableId, setDropTargetTableId] = useState<string | null>(null);
	const [intendedRelationship, setIntendedRelationship] = useState<{ primaryFieldId: string; foreignFieldId: string } | null>(null);
	const theme = useTheme();

	const chipBaseSx = useMemo(
		() => ({
			height: 24,
			borderRadius: 12,
			fontWeight: 600,
			fontSize: 12,
			'& .MuiChip-label': {
				px: 1.2
			}
		}),
		[]
	);

	const renderBooleanChip = useCallback(
		(value: boolean | null | undefined) => {
			if (value === null || value === undefined) {
				return (
					<Chip
						label="—"
						size="small"
						sx={{
							...chipBaseSx,
							color: theme.palette.text.disabled,
							bgcolor: alpha(theme.palette.text.disabled, 0.12)
						}}
					/>
				);
			}
			const isTrue = Boolean(value);
			const paletteColor = isTrue ? theme.palette.success : theme.palette.warning;
			return (
				<Chip
					label={isTrue ? 'Yes' : 'No'}
					size="small"
					sx={{
						...chipBaseSx,
						bgcolor: alpha(paletteColor.main, isTrue ? 0.24 : 0.18),
						color: paletteColor.dark ?? paletteColor.main,
						textTransform: 'uppercase'
					}}
				/>
			);
		},
		[chipBaseSx, theme]
	);

	const resolveFieldTypePalette = useCallback(
		(fieldType: string) => {
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
		},
		[theme]
	);

	const renderFieldTypeChip = useCallback(
		(value: string | null | undefined) => {
			if (!value) {
				return '—';
			}
			const paletteColor = resolveFieldTypePalette(value);
			return (
				<Chip
					label={value}
					size="small"
					sx={{
						...chipBaseSx,
						textTransform: 'capitalize',
						bgcolor: alpha(paletteColor.main, 0.22),
						color: paletteColor.dark ?? paletteColor.main
					}}
				/>
			);
		},
		[chipBaseSx, resolveFieldTypePalette]
	);

	useEffect(() => {
		setDrafts(initialDrafts);
		setCommitted(initialDrafts);
	}, [initialDrafts]);

	useEffect(() => {
		setExpandedTables((prev) => {
			const next: Record<string, boolean> = {};
			definition.tables.forEach((table) => {
				next[table.id] = prev[table.id] ?? false;
			});
			return next;
		});
	}, [definition.tables]);

	useEffect(() => {
		setGridViewByTable((prev) => {
			const next: Record<string, boolean> = {};
			definition.tables.forEach((table) => {
				next[table.id] = prev[table.id] ?? false;
			});
			return next;
		});
	}, [definition.tables]);

	const sortedTables = useMemo(
		() =>
			definition.tables
				.slice()
				.sort((a, b) => {
					const left = a.loadOrder ?? Number.MAX_SAFE_INTEGER;
					const right = b.loadOrder ?? Number.MAX_SAFE_INTEGER;
					if (left === right) {
						return a.table.name.localeCompare(b.table.name);
					}
					return left - right;
				}),
		[definition.tables]
	);

	const handleToggle = useCallback((tableId: string) => {
		setExpandedTables((prev) => ({
			...prev,
			[tableId]: !(prev[tableId] ?? false)
		}));
	}, []);

	const getDraftForField = useCallback(
		(field: Field) => drafts[field.id] ?? buildDraft(field),
		[drafts]
	);

	const getCommittedForField = useCallback(
		(field: Field) => committed[field.id] ?? buildDraft(field),
		[committed]
	);

	const handleDraftChange = useCallback(
		(field: Field, key: StringDraftKey, value: string) => {
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

	const handleSubmitChange = useCallback(
		async (field: Field, key: StringDraftKey) => {
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
			} else {
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
		},
		[canEdit, getCommittedForField, getDraftForField, onInlineFieldSubmit]
	);

	const handleBooleanToggle = useCallback(
		async (field: Field, key: BooleanDraftKey, checked: boolean) => {
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
			} else {
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
		},
		[canEdit, getCommittedForField, onInlineFieldSubmit]
	);

	const clearInlineRow = useCallback((definitionTableId: string) => {
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

	const updateExistingRow = useCallback((definitionTableId: string, updates: Partial<InlineExistingState>) => {
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

	const updateCreateRow = useCallback(
		(definitionTableId: string, updater: (state: InlineCreateState) => InlineCreateState) => {
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
		},
		[]
	);

	const handleStartAddExisting = useCallback((table: DataDefinitionTable, selectableFields: Field[]) => {
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

	const handleStartCreateNew = useCallback((table: DataDefinitionTable) => {
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

	const handleExistingFieldSelect = useCallback(
		(definitionTableId: string, field: Field | null) => {
			updateExistingRow(definitionTableId, {
				fieldId: field?.id ?? null,
				error: undefined
			});
		},
		[updateExistingRow]
	);

	const handleInlineNotesChange = useCallback((definitionTableId: string, value: string) => {
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

	const handleCreateDraftTextChange = useCallback(
		(definitionTableId: string, key: StringDraftKey, value: string) => {
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
		},
		[updateCreateRow]
	);

	const handleCreateDraftBooleanChange = useCallback(
		(definitionTableId: string, key: BooleanDraftKey, checked: boolean) => {
			updateCreateRow(definitionTableId, (state) => ({
				...state,
				draft: {
					...state.draft,
					[key]: checked
				}
			}));
		},
		[updateCreateRow]
	);

	const handleInlineSave = useCallback(
		async (
			table: DataDefinitionTable,
			tableName: string,
			inlineState: InlineRowState,
			tableSaving: boolean
		) => {
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

			const errors: Partial<Record<StringDraftKey | 'notes', string>> = {};
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
		},
		[
			clearInlineRow,
			fieldActionsDisabled,
			onAddExistingFieldInline,
			onCreateFieldInline,
			updateCreateRow,
			updateExistingRow
		]
	);

	const handlePaste = useCallback(
		async (event: ClipboardEvent<HTMLDivElement>, table: DataDefinitionTable, tableName: string) => {
			if (!canEdit || !onCreateFieldInline) {
				return;
			}

			const target = event.target as HTMLElement | null;
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
				const [
					name,
					fieldType,
					fieldLength,
					decimalPlaces,
					systemRequired,
					businessProcessRequired,
					suppressedField,
					active,
					description,
					applicationUsage,
					businessDefinition,
					enterpriseAttribute,
					legalRegulatoryImplications,
					securityClassification,
					dataValidation,
					referenceTable,
					groupingTab,
					notes
				] = cells;

				const draft: FieldDraft = {
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
				} else {
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
		},
		[canEdit, fieldActionsDisabled, inlineRows, onBulkPasteResult, onCreateFieldInline, tableSavingState]
	);

	const exportTableToCsv = useCallback((table: DataDefinitionTable, tableName: string) => {
		const headers = [...FIELD_COLUMNS.map((column) => column.label), 'Notes'];
		const body = table.fields.map((definitionField) => {
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

	return (
		<Stack spacing={3} sx={{ p: 2 }}>
			{definition.description && (
				<Paper
					variant="outlined"
					sx={{
						p: 2,
						bgcolor: alpha(theme.palette.info.main, 0.04),
						borderColor: alpha(theme.palette.info.main, 0.25),
						borderWidth: 1.5
					}}
				>
					<Typography variant="subtitle2" color="info.dark" sx={{ fontWeight: 600, mb: 1 }}>
						Description
					</Typography>
					<Typography variant="body1">{definition.description}</Typography>
				</Paper>
			)}

			<DataDefinitionRelationshipBuilder
				tables={definition.tables}
				relationships={definition.relationships}
				canEdit={Boolean(canEdit)}
				onCreateRelationship={onCreateRelationship}
				onUpdateRelationship={onUpdateRelationship}
				onDeleteRelationship={onDeleteRelationship}
				busy={relationshipBusy}
				initialPrimaryFieldId={intendedRelationship?.primaryFieldId}
				initialForeignFieldId={intendedRelationship?.foreignFieldId}
			/>

			{sortedTables.map((table) => {
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
				const disableAdd =
					fieldActionsDisabled || tableSaving || Boolean(inlineState) || selectableFields.length === 0;
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

				const rows: FieldRow[] = table.fields.map((definitionField) => ({
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

				const baseColumns: GridColDef<FieldRow>[] = FIELD_COLUMNS.map((column) => {
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
						sortComparator:
							column.kind === 'number'
								? (value1, value2) => Number(value1 ?? 0) - Number(value2 ?? 0)
							: undefined,
						renderCell: (params) => {
							const { row, value } = params;
							if (row.__isPlaceholder) {
								if (isGridView && column.key === 'name') {
									return (
										<Stack direction="row" spacing={1} alignItems="center">
											<Box
												sx={{
												width: 10,
												height: 10,
												borderRadius: '50%',
												border: `2px dashed ${alpha(theme.palette.primary.main, 0.4)}`
											}}
											/>
											<Typography variant="body2" color="text.disabled" sx={{ fontStyle: 'italic' }}>
												Paste new rows here
											</Typography>
										</Stack>
									);
								}
								return null;
							}

							const definitionField = row.__definitionField;
							if (!definitionField) {
								if (column.key === 'fieldType') {
									return renderFieldTypeChip(value as string | null | undefined);
								}
								if (column.kind === 'boolean') {
									return renderBooleanChip(value as boolean | null | undefined);
								}
								if (column.kind === 'number') {
									return renderNumber(value as string | number | null | undefined);
								}
								if (isMultiline) {
									const text = typeof value === 'string' ? value.trim() : '';
									return (
										<Typography
											variant="body2"
											sx={{
												width: '100%',
												whiteSpace: 'pre-wrap',
												lineHeight: 1.4,
												color: text ? theme.palette.text.primary : theme.palette.text.disabled,
												fontStyle: text ? 'normal' : 'italic'
											}}
										>
											{text || '—'}
										</Typography>
									);
								}

								const textValue = renderText(value as string | null | undefined);
								return (
									<Typography
										variant="body2"
										sx={{
											width: '100%',
											whiteSpace: 'nowrap',
											overflow: 'hidden',
											textOverflow: 'ellipsis',
											color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
										}}
									>
										{textValue}
									</Typography>
								);
							}

							const saving = inlineSavingState?.[definitionField.field.id] ?? false;

							if (isGridView && canEdit) {
								if (column.kind === 'boolean') {
									return (
										<Checkbox
											size="small"
											checked={Boolean(row[column.key])}
											onChange={(_, checked) => {
												void handleBooleanToggle(definitionField.field, column.key, checked);
											}}
											disabled={fieldActionsDisabled || saving}
										/>
									);
								}

								const draftKey = column.key as StringDraftKey;
								const draftValue = row[draftKey];
								const inputType = column.kind === 'number' ? 'number' : 'text';

								if (isMultiline) {
									return (
										<GridTextarea
											value={draftValue ?? ''}
											disabled={fieldActionsDisabled || saving}
											onChange={(event) => handleDraftChange(definitionField.field, draftKey, event.target.value)}
											onBlur={() => {
												void handleSubmitChange(definitionField.field, draftKey);
											}}
											minRows={3}
											maxRows={8}
										/>
									);
								}

								return (
									<TextField
										variant="outlined"
										size="small"
										fullWidth
										value={draftValue}
										type={inputType}
										disabled={fieldActionsDisabled || saving}
										onChange={(event) => handleDraftChange(definitionField.field, draftKey, event.target.value)}
										onBlur={() => {
											void handleSubmitChange(definitionField.field, draftKey);
										}}
										sx={{
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
										}}
									/>
								);
							}

							if (!isGridView && column.key === 'name' && canEdit) {
								const displayValue = renderText(value as string | null | undefined);
								return (
									<Stack 
										direction="row" 
										spacing={1} 
										alignItems="center"
										draggable
										onDragStart={(e) => {
											setDragSource({ tableId: table.id, fieldId: definitionField.field.id });
											e.dataTransfer.effectAllowed = 'copy';
											e.dataTransfer.setData('application/json', JSON.stringify({
												tableId: table.id,
												fieldId: definitionField.field.id,
												fieldName: definitionField.field.name
											}));
										}}
										onDragEnd={() => {
											setDragSource(null);
										}}
										sx={{
											cursor: 'grab',
											'&:active': { cursor: 'grabbing' },
											opacity: dragSource?.fieldId === definitionField.field.id ? 0.7 : 1,
											transition: 'opacity 0.2s'
										}}
									>
										{saving && <CircularProgress size={16} />}
										<Link
											component="button"
											variant="body2"
											underline="hover"
											onClick={() => onEditField?.(table.tableId, tableName, definitionField.field)}
											sx={{ fontWeight: 600 }}
										>
											{displayValue}
										</Link>
									</Stack>
								);
							}

							if (column.kind === 'boolean') {
								return renderBooleanChip(value as boolean | null | undefined);
							}

							if (column.key === 'fieldType') {
								return renderFieldTypeChip(value as string | null | undefined);
							}

							if (column.kind === 'number') {
								return renderNumber(value as string | number | null | undefined);
							}

							const textValue = renderText(value as string | null | undefined);
							return (
								<Typography
									variant="body2"
									sx={{
										width: '100%',
										whiteSpace: 'nowrap',
										overflow: 'hidden',
										textOverflow: 'ellipsis',
										color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
									}}
								>
									{textValue}
								</Typography>
							);
						}
					};
				});

				const notesColumn: GridColDef<FieldRow> = {
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
						const textValue = renderText(params.value as string | null | undefined);
						return (
							<Typography
								variant="body2"
								sx={{
									width: '100%',
									whiteSpace: 'pre-wrap',
									lineHeight: 1.4,
									color: textValue === '—' ? theme.palette.text.disabled : theme.palette.text.primary
								}}
							>
								{textValue}
							</Typography>
						);
					}
				};

				const columns: GridColDef<FieldRow>[] = [
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
							return (
								<Box
									sx={{
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
									}}
								/>
							);
						}
					},
					...baseColumns,
					notesColumn
				];

				return (
					<Paper 
						key={table.id} 
						variant="outlined" 
						sx={{ 
							p: 2, 
							bgcolor: theme.palette.common.white, 
							borderColor: dropTargetTableId === table.id ? theme.palette.info.main : alpha(theme.palette.info.main, 0.25), 
							borderLeft: `4px solid ${theme.palette.info.main}`, 
							boxShadow: `0 4px 12px ${alpha(theme.palette.common.black, 0.05)}`,
							transition: theme.transitions.create('border-color', { duration: 200 })
						}}
						onDragOver={(e) => {
							e.preventDefault();
							e.dataTransfer.dropEffect = 'copy';
							setDropTargetTableId(table.id);
						}}
						onDragLeave={(e) => {
							// Only clear if leaving the Paper itself
							if (e.target === e.currentTarget) {
								setDropTargetTableId(null);
							}
						}}
						onDrop={(e) => {
							e.preventDefault();
							e.stopPropagation();
							setDropTargetTableId(null);
							
							try {
								const data = JSON.parse(e.dataTransfer.getData('application/json'));
								const { tableId: sourceTableId, fieldId: sourceFieldId } = data;
								
								// Prevent dropping on the same table
								if (sourceTableId !== table.id && dragSource) {
									setDragSource(null);
									// Set the intended relationship with source and first field of target table
									const targetField = table.fields[0];
									if (targetField) {
										setIntendedRelationship({
											primaryFieldId: sourceFieldId,
											foreignFieldId: targetField.fieldId
										});
									}
								}
							} catch (error) {
								console.error('Error processing drop:', error);
							}
						}}
					>
						<Stack spacing={2}>
							<Stack
								direction={{ xs: 'column', md: 'row' }}
								justifyContent="space-between"
								alignItems={{ xs: 'flex-start', md: 'center' }}
								spacing={1.5}
								sx={{
									pb: 1.5,
									borderBottom: `2px solid ${alpha(theme.palette.warning.main, 0.15)}`
								}}
							>
								<Stack spacing={0.5}>
									<Typography
										variant="h6"
										sx={{
											color: theme.palette.primary.main,
											fontWeight: 700,
											letterSpacing: 0.3
										}}
									>
										{tableName}
									</Typography>
									<Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
										{table.table.schemaName
											? `${table.table.schemaName}.${table.table.physicalName}`
											: table.table.physicalName}
									</Typography>
								</Stack>
								<Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
									{typeof table.loadOrder === 'number' && (
										<Chip label={`Load Order: ${table.loadOrder}`} size="small" color="primary" variant="outlined" />
									)}
									{table.table.tableType && <Chip label={table.table.tableType} size="small" />}
									{table.table.status && (
										<Chip
											label={table.table.status}
											size="small"
											color={table.table.status === 'active' ? 'success' : 'default'}
										/>
									)}
									<IconButton
										onClick={() => handleToggle(table.id)}
										aria-label={isExpanded ? 'Collapse fields' : 'Expand fields'}
										size="small"
									>
										{isExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
									</IconButton>
								</Stack>
							</Stack>

							{table.description && (
								<Typography variant="body2" color="text.secondary">
									{table.description}
								</Typography>
							)}

							<Stack
								direction={{ xs: 'column', sm: 'row' }}
								spacing={1}
								justifyContent="flex-end"
								alignItems={{ xs: 'stretch', sm: 'center' }}
							>
								<Button
									variant="contained"
									size="small"
									startIcon={<AddCircleOutlineIcon />}
									onClick={(event: MouseEvent<HTMLButtonElement>) =>
										setNewMenu({ tableId: table.id, anchor: event.currentTarget })
									}
									disabled={!canEdit || fieldActionsDisabled || tableSaving}
								>
									New
								</Button>
								<Button
									variant={isGridView ? 'contained' : 'outlined'}
									size="small"
									startIcon={<EditNoteIcon />}
									onClick={() =>
										setGridViewByTable((prev) => ({
											...prev,
											[table.id]: !isGridView
										}))
									}
									disabled={disableGridToggle}
								>
									{isGridView ? 'Exit grid view' : 'Edit in grid'}
								</Button>
								<Button
									variant="outlined"
									size="small"
									startIcon={<FileDownloadOutlinedIcon />}
									onClick={() => exportTableToCsv(table, tableName)}
								>
									Export to Excel
								</Button>
							</Stack>

							<Menu anchorEl={menuAnchor} open={isNewMenuOpen} onClose={() => setNewMenu(null)} keepMounted>
								<MenuItem
									onClick={() => {
										handleStartCreateNew(table);
										setNewMenu(null);
									}}
									disabled={disableCreate || !canEdit}
								>
									<ListItemIcon>
										<PostAddOutlinedIcon fontSize="small" />
									</ListItemIcon>
									<ListItemText primary="Create field" secondary={disableCreate ? createTooltip : undefined} />
								</MenuItem>
								<MenuItem
									onClick={() => {
										handleStartAddExisting(table, selectableFields);
										setNewMenu(null);
									}}
									disabled={disableAdd || !canEdit}
								>
									<ListItemIcon>
										<LibraryAddCheckOutlinedIcon fontSize="small" />
									</ListItemIcon>
									<ListItemText primary="Add existing field" secondary={disableAdd ? addTooltip : undefined} />
								</MenuItem>
							</Menu>

							<Collapse in={isExpanded} timeout="auto" unmountOnExit>
								<Stack spacing={2}>
									<Box
										onPaste={(event) => {
											if (isGridView) {
												handlePaste(event, table, tableName);
											}
										}}
										tabIndex={isGridView ? 0 : undefined}
										sx={{ 
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
										}}
									>
										<DataGrid
											autoHeight
											rows={rows}
											columns={columns}
											disableRowSelectionOnClick
											disableColumnSelector
											disableDensitySelector
											hideFooter
											density="comfortable"
											sortingOrder={['asc', 'desc']}
											getRowClassName={resolveRowClassName}
											columnHeaderHeight={isGridView ? 46 : 52}
											rowHeight={isGridView ? 68 : 52}
											showColumnVerticalBorder
											showCellVerticalBorder
											slots={{ toolbar: QuickFilterToolbar }}
											sx={{
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
											}}
										/>
									</Box>

									{isGridView && (
										<Stack spacing={0.5}>
											<Typography variant="caption" color="text.secondary" sx={{ fontWeight: 500 }}>
												💡 Tip: Click the grid above, then use <strong>Ctrl+V</strong> to paste from Excel or paste new rows in the blank row at the bottom.
											</Typography>
										</Stack>
									)}
								</Stack>
							</Collapse>

							<Dialog
								open={isDialogOpen}
								onClose={(_, reason) => {
									if (tableSaving) {
										return;
									}
									if (reason === 'backdropClick' && fieldActionsDisabled) {
										return;
									}
									clearInlineRow(table.id);
								}}
								fullWidth
								maxWidth={inlineType === 'create-new' ? 'md' : 'sm'}
								aria-labelledby={dialogTitleId}
								disableEscapeKeyDown={tableSaving}
							>
								{inlineType === 'add-existing' && inlineState?.type === 'add-existing' && (
									<>
										<DialogTitle id={dialogTitleId}>Add existing field</DialogTitle>
										<DialogContent dividers>
											<Stack spacing={2}>
												<Autocomplete
													options={selectableFields}
													value={
														inlineState.fieldId
															? selectableFields.find((field) => field.id === inlineState.fieldId) ?? null
															: null
													}
													onChange={(_, value) => handleExistingFieldSelect(table.id, value)}
													getOptionLabel={(option) => `${option.name} (${option.fieldType})`}
													isOptionEqualToValue={(option, value) => option.id === value.id}
													renderInput={(params) => (
														<TextField
															{...params}
															label="Field"
															error={Boolean(inlineState.error)}
															helperText={inlineState.error}
															size="small"
														/>
													)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
												/>
												<TextField
													label="Notes"
													value={inlineState.notes}
													onChange={(event) => handleInlineNotesChange(table.id, event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={3}
												/>
											</Stack>
										</DialogContent>
										<DialogActions sx={{ px: 3, py: 2, gap: 1 }}>
											{tableSaving && <CircularProgress size={18} />}
											<Button onClick={() => clearInlineRow(table.id)} disabled={tableSaving}>
												Cancel
											</Button>
											<Button
												variant="contained"
												onClick={() => handleInlineSave(table, tableName, inlineState, tableSaving)}
												disabled={fieldActionsDisabled || tableSaving || !inlineState.fieldId}
											>
												Add field
											</Button>
										</DialogActions>
									</>
								)}
								{inlineType === 'create-new' && inlineState?.type === 'create-new' && (
									<>
										<DialogTitle id={dialogTitleId}>Create new field</DialogTitle>
										<DialogContent dividers>
											<Stack spacing={2}>
												<TextField
													label="Name"
													value={inlineState.draft.name}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'name', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													error={Boolean(inlineState.errors.name)}
													helperText={inlineState.errors.name}
												/>
												<TextField
													label="Description"
													value={inlineState.draft.description}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'description', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={3}
												/>
												<TextField
													label="Application Usage"
													value={inlineState.draft.applicationUsage}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'applicationUsage', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={2}
												/>
												<TextField
													label="Business Definition"
													value={inlineState.draft.businessDefinition}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'businessDefinition', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={2}
												/>
												<TextField
													label="Enterprise Attribute"
													value={inlineState.draft.enterpriseAttribute}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'enterpriseAttribute', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
												/>
												<TextField
													label="Field Type"
													value={inlineState.draft.fieldType}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'fieldType', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													error={Boolean(inlineState.errors.fieldType)}
													helperText={inlineState.errors.fieldType}
												/>
												<Grid container spacing={2}>
													<Grid item xs={6}>
														<TextField
															label="Length"
															value={inlineState.draft.fieldLength}
															onChange={(event) => handleCreateDraftTextChange(table.id, 'fieldLength', event.target.value)}
															disabled={fieldActionsDisabled || tableSaving}
															fullWidth
															size="small"
															type="number"
														/>
													</Grid>
													<Grid item xs={6}>
														<TextField
															label="Decimal Places"
															value={inlineState.draft.decimalPlaces}
															onChange={(event) => handleCreateDraftTextChange(table.id, 'decimalPlaces', event.target.value)}
															disabled={fieldActionsDisabled || tableSaving}
															fullWidth
															size="small"
															type="number"
														/>
													</Grid>
												</Grid>
												<Stack spacing={1}>
													<Typography variant="subtitle2" color="text.secondary">
														Legal / Regulatory Implications
													</Typography>
													<TextField
														value={inlineState.draft.legalRegulatoryImplications}
														onChange={(event) => handleCreateDraftTextChange(table.id, 'legalRegulatoryImplications', event.target.value)}
														disabled={fieldActionsDisabled || tableSaving}
														fullWidth
														size="small"
														multiline
														minRows={2}
													/>
												</Stack>
												<Stack spacing={1}>
													<Typography variant="subtitle2" color="text.secondary">
														Security Classification
													</Typography>
													<TextField
														value={inlineState.draft.securityClassification}
														onChange={(event) => handleCreateDraftTextChange(table.id, 'securityClassification', event.target.value)}
														disabled={fieldActionsDisabled || tableSaving}
														fullWidth
														size="small"
													/>
												</Stack>
												<TextField
													label="Data Validation"
													value={inlineState.draft.dataValidation}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'dataValidation', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={2}
												/>
												<TextField
													label="Reference Table"
													value={inlineState.draft.referenceTable}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'referenceTable', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
												/>
												<TextField
													label="Grouping Tab"
													value={inlineState.draft.groupingTab}
													onChange={(event) => handleCreateDraftTextChange(table.id, 'groupingTab', event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
												/>
												<TextField
													label="Notes"
													value={inlineState.notes}
													onChange={(event) => handleInlineNotesChange(table.id, event.target.value)}
													disabled={fieldActionsDisabled || tableSaving}
													fullWidth
													size="small"
													multiline
													minRows={3}
												/>
												<FormControlLabel
													control={
														<Checkbox
															checked={inlineState.draft.systemRequired}
															onChange={(event) => handleCreateDraftBooleanChange(table.id, 'systemRequired', event.target.checked)}
															disabled={fieldActionsDisabled || tableSaving}
														/>
													}
													label="System Required"
												/>
												<FormControlLabel
													control={
														<Checkbox
															checked={inlineState.draft.businessProcessRequired}
															onChange={(event) => handleCreateDraftBooleanChange(table.id, 'businessProcessRequired', event.target.checked)}
															disabled={fieldActionsDisabled || tableSaving}
														/>
													}
													label="Business Process Required"
												/>
												<FormControlLabel
													control={
														<Checkbox
															checked={inlineState.draft.suppressedField}
															onChange={(event) => handleCreateDraftBooleanChange(table.id, 'suppressedField', event.target.checked)}
															disabled={fieldActionsDisabled || tableSaving}
														/>
													}
													label="Suppressed"
												/>
												<FormControlLabel
													control={
														<Checkbox
															checked={inlineState.draft.active}
															onChange={(event) => handleCreateDraftBooleanChange(table.id, 'active', event.target.checked)}
															disabled={fieldActionsDisabled || tableSaving}
														/>
													}
													label="Active"
												/>
											</Stack>
										</DialogContent>
										<DialogActions sx={{ px: 3, py: 2, gap: 1 }}>
											{tableSaving && <CircularProgress size={18} />}
											<Button onClick={() => clearInlineRow(table.id)} disabled={tableSaving}>
												Cancel
											</Button>
											<Button
												variant="contained"
												onClick={() => handleInlineSave(table, tableName, inlineState, tableSaving)}
												disabled={fieldActionsDisabled || tableSaving || !inlineState.draft.name.trim() || !inlineState.draft.fieldType.trim()}
											>
												Create field
											</Button>
										</DialogActions>
									</>
								)}
							</Dialog>
						</Stack>
					</Paper>
				);
			})}
		</Stack>
	);
};

export default DataDefinitionDetails;
