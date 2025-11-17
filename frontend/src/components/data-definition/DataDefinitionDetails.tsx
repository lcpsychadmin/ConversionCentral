import { ClipboardEvent, MouseEvent, createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import clsx from 'clsx';
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
	Switch,
	TextField,
	Typography
} from '@mui/material';
import TextareaAutosize from '@mui/material/TextareaAutosize';
import { alpha, styled, useTheme } from '@mui/material/styles';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditNoteIcon from '@mui/icons-material/EditNote';
import FileDownloadOutlinedIcon from '@mui/icons-material/FileDownloadOutlined';
import LibraryAddCheckOutlinedIcon from '@mui/icons-material/LibraryAddCheckOutlined';
import PostAddOutlinedIcon from '@mui/icons-material/PostAddOutlined';
import SearchIcon from '@mui/icons-material/Search';
import PreviewIcon from '@mui/icons-material/Preview';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import {
	DndContext,
	DragEndEvent,
	KeyboardSensor,
	PointerSensor,
	closestCenter,
	useSensor,
	useSensors
} from '@dnd-kit/core';
import type { DraggableAttributes } from '@dnd-kit/core';
import {
	SortableContext,
	arrayMove,
	sortableKeyboardCoordinates,
	useSortable,
	verticalListSortingStrategy
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
	DataGrid,
	GridColDef,
	GridRenderCellParams,
	GridRow,
	GridRowClassNameParams,
	GridRowProps,
	useGridApiContext
} from '@mui/x-data-grid';

import DataDefinitionRelationshipBuilder from './DataDefinitionRelationshipBuilder';
import ConnectionDataPreviewDialog from '../system-connection/ConnectionDataPreviewDialog';
import {
	DataDefinition,
	DataDefinitionField,
	DataDefinitionRelationshipInput,
	DataDefinitionRelationshipUpdateInput,
	DataDefinitionTable,
	Field,
	ConnectionTablePreview,
	DatabricksDataType,
	LegalRequirement,
	SecurityClassification
} from '../../types/data';
import { fetchTablePreview } from '../../services/tableService';
import { useToast } from '../../hooks/useToast';
import { getPanelSurface } from '../../theme/surfaceStyles';

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
	legalRequirementId: string;
	securityClassificationId: string;
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
	{ key: 'fieldType', label: 'Field Type', kind: 'text', minWidth: 160 },
	{ key: 'fieldLength', label: 'Length', kind: 'number', minWidth: 100 },
	{ key: 'decimalPlaces', label: 'Decimal Places', kind: 'number', minWidth: 120 },
	{ key: 'systemRequired', label: 'System Required', kind: 'boolean' },
	{ key: 'businessProcessRequired', label: 'Business Process Required', kind: 'boolean' },
	{ key: 'enterpriseAttribute', label: 'Enterprise Attribute', kind: 'text', minWidth: 160 },
	{ key: 'suppressedField', label: 'Suppressed', kind: 'boolean' },
	{ key: 'active', label: 'Active', kind: 'boolean' },
	{
		key: 'legalRegulatoryImplications',
		label: 'Legal / Regulatory Implications',
		kind: 'text',
		multiline: true,
		minWidth: 240
	},
	{ key: 'legalRequirementId', label: 'Legal Requirement', kind: 'text', minWidth: 200 },
	{ key: 'securityClassificationId', label: 'Security Classification', kind: 'text', minWidth: 200 },
	{ key: 'dataValidation', label: 'Data Validation', kind: 'text', multiline: true, minWidth: 220 },
	{ key: 'referenceTable', label: 'Reference Table', kind: 'text', minWidth: 180 },
	{ key: 'groupingTab', label: 'Grouping Tab', kind: 'text', minWidth: 160 }
];

const normalizeEnterpriseAttribute = (value?: string | null): string => {
	if (!value) {
		return '';
	}

	const normalized = value.trim().toLowerCase();

	if (['yes', 'y', 'true'].includes(normalized)) {
		return 'Yes';
	}

	if (['no', 'n', 'false'].includes(normalized)) {
		return 'No';
	}

	return value.trim();
};

const enterpriseAttributeToBoolean = (value?: string | null): boolean | null => {
	const normalized = normalizeEnterpriseAttribute(value);
	if (normalized === 'Yes') {
		return true;
	}
	if (normalized === 'No') {
		return false;
	}
	return null;
};

const cycleEnterpriseAttributeValue = (value?: string | null): string => {
	const normalized = normalizeEnterpriseAttribute(value);
	if (normalized === 'Yes') {
		return 'No';
	}
	if (normalized === 'No') {
		return '';
	}
	return 'Yes';
};

const AUDIT_FIELD_NAMES = new Set(
	['Project', 'Release', 'Created By', 'Created Date', 'Modified By', 'Modified Date'].map((name) =>
		name.toLowerCase()
	)
);

const buildDraft = (field: Field): FieldDraft => ({
	name: field.name,
	description: field.description ?? '',
	applicationUsage: field.applicationUsage ?? '',
	businessDefinition: field.businessDefinition ?? '',
	enterpriseAttribute: normalizeEnterpriseAttribute(field.enterpriseAttribute),
	fieldType: field.fieldType,
	fieldLength: field.fieldLength?.toString() ?? '',
	decimalPlaces: field.decimalPlaces?.toString() ?? '',
	legalRegulatoryImplications: field.legalRegulatoryImplications ?? '',
	legalRequirementId: field.legalRequirementId ?? '',
	securityClassificationId: field.securityClassificationId ?? '',
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
			if (definitionField.field) {
				drafts[definitionField.field.id] = buildDraft(definitionField.field);
			}
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
	legalRequirementId: '',
	securityClassificationId: '',
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
	isUnique: boolean;
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
	const isDarkMode = theme.palette.mode === 'dark';
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
					backgroundColor: alpha(theme.palette.primary.main, isDarkMode ? 0.22 : 0.08),
					borderBottom: `1px solid ${alpha(theme.palette.primary.main, isDarkMode ? 0.35 : 0.18)}`,
					backdropFilter: isDarkMode ? 'blur(18px)' : undefined
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
						backgroundColor: isDarkMode
							? alpha(theme.palette.background.paper, 0.82)
							: alpha(theme.palette.common.white, 0.95),
						boxShadow: isDarkMode
							? `0 10px 24px ${alpha(theme.palette.common.black, 0.5)}`
							: `0 6px 16px ${alpha(theme.palette.primary.main, 0.12)}`,
						transition: theme.transitions.create(['box-shadow', 'border-color'], {
							duration: theme.transitions.duration.shorter
						}),
						'&:hover': {
							boxShadow: isDarkMode
								? `0 14px 32px ${alpha(theme.palette.common.black, 0.55)}`
								: `0 8px 20px ${alpha(theme.palette.primary.main, 0.16)}`
						},
						'&.Mui-focused': {
							boxShadow: isDarkMode
								? `0 16px 36px ${alpha(theme.palette.common.black, 0.6)}`
								: `0 10px 28px ${alpha(theme.palette.primary.main, 0.22)}`
						},
						'& fieldset': {
							border: `1.5px solid ${alpha(theme.palette.primary.main, isDarkMode ? 0.6 : 0.35)}`
						},
						'&:hover fieldset': {
							borderColor: alpha(theme.palette.primary.main, isDarkMode ? 0.75 : 0.5)
						},
						'&.Mui-focused fieldset': {
							borderColor: theme.palette.primary.main
						}
					},
					'& .MuiInputBase-input': {
						fontSize: 13,
						padding: theme.spacing(0.5, 1.5),
						color: theme.palette.text.primary
					}
				}}
			/>
		</Stack>
	);
};

const GridTextarea = styled(TextareaAutosize)(({ theme }) => {
	const isDarkMode = theme.palette.mode === 'dark';
	const baseBackground = isDarkMode
		? alpha(theme.palette.background.paper, 0.82)
		: alpha(theme.palette.common.white, 0.96);
	const focusBackground = isDarkMode
		? alpha(theme.palette.background.paper, 0.94)
		: alpha(theme.palette.common.white, 0.98);
	const disabledBackground = isDarkMode
		? alpha(theme.palette.background.paper, 0.45)
		: alpha(theme.palette.action.disabledBackground, 0.35);

	return {
		border: `1px solid ${alpha(theme.palette.divider, isDarkMode ? 0.55 : 0.45)}`,
		backgroundColor: baseBackground,
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
			borderColor: alpha(theme.palette.primary.main, isDarkMode ? 0.75 : 0.7)
		},
		'&:focus': {
			outline: 'none',
			borderColor: theme.palette.primary.main,
			boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, isDarkMode ? 0.35 : 0.2)}`,
			backgroundColor: focusBackground
		},
		'&:disabled': {
			backgroundColor: disabledBackground,
			color: theme.palette.text.disabled,
			cursor: 'not-allowed'
		}
	};
});

type RowDragContextValue = {
	attributes: DraggableAttributes;
	listeners: ReturnType<typeof useSortable>['listeners'];
	setActivatorNodeRef: (element: HTMLElement | null) => void;
	isDragging: boolean;
	disabled: boolean;
};

const RowDragContext = createContext<RowDragContextValue | null>(null);

type DraggableRowProps = GridRowProps & {
	disableDrag?: boolean;
};

const DraggableRow = ({ disableDrag = false, className, style, ...rest }: DraggableRowProps) => {
	const sortable = useSortable({
		id: String(rest.rowId),
		disabled: disableDrag
	});

	const transformStyle = !disableDrag && sortable.transform
		? CSS.Transform.toString({ ...sortable.transform, scaleX: 1, scaleY: 1 })
		: undefined;

	const combinedStyle = disableDrag
		? style
		: {
			...style,
			transform: transformStyle ?? style?.transform,
			transition: sortable.transition ?? style?.transition
		};

	return (
		<RowDragContext.Provider
			value={{
				attributes: sortable.attributes,
				listeners: sortable.listeners,
				setActivatorNodeRef: sortable.setActivatorNodeRef,
				isDragging: sortable.isDragging,
				disabled: disableDrag
			}}
		>
			<GridRow
				{...rest}
				ref={sortable.setNodeRef}
				className={clsx(className, sortable.isDragging ? 'dragging-row' : undefined)}
				style={combinedStyle}
			/>
		</RowDragContext.Provider>
	);
};

type RowActionsCellProps = GridRenderCellParams<FieldRow> & {
	canEdit: boolean;
	disableActions: boolean;
	onDelete: (definitionFieldId: string) => void;
};

const RowActionsCell = ({ row, canEdit, disableActions, onDelete }: RowActionsCellProps) => {
	if (row.__isPlaceholder) {
		return null;
	}

	const handleDeleteClick = (event: MouseEvent<HTMLButtonElement>) => {
		event.stopPropagation();
		onDelete(row.id);
	};

	return (
		<IconButton
			size="small"
			aria-label="Remove field"
			onClick={handleDeleteClick}
			disabled={!canEdit || disableActions}
		>
			<DeleteOutlineIcon fontSize="small" />
		</IconButton>
	);
};

type DragHandleCellProps = GridRenderCellParams<FieldRow> & {
	canEdit: boolean;
	disableActions: boolean;
	dndEnabled: boolean;
};

const DragHandleCell = ({ row, canEdit, disableActions, dndEnabled }: DragHandleCellProps) => {
	const theme = useTheme();
	const dragContext = useContext(RowDragContext);

	if (row.__isPlaceholder) {
		return (
			<Box
				sx={{
					display: 'flex',
					alignItems: 'center',
					justifyContent: 'center',
					width: '100%'
				}}
			>
				<Box
					sx={{
						width: 12,
						height: 12,
						borderRadius: '50%',
						border: `2px dashed ${alpha(theme.palette.primary.main, 0.4)}`
					}}
				/>
			</Box>
		);
	}

	const draggingDisabled = !dndEnabled || disableActions || !canEdit || !dragContext || dragContext.disabled;
	const activatorRef = draggingDisabled || !dragContext ? undefined : dragContext.setActivatorNodeRef;
	const activatorListeners = draggingDisabled || !dragContext ? undefined : dragContext.listeners;
	const activatorAttributes = draggingDisabled || !dragContext ? undefined : dragContext.attributes;
	const isActive = Boolean(row.active);

	return (
		<IconButton
			size="small"
			aria-label="Reorder field"
			disabled={draggingDisabled}
			sx={{
				cursor: draggingDisabled ? 'default' : 'grab',
				color: isActive ? theme.palette.primary.main : theme.palette.text.secondary,
				'&:hover': {
					color: draggingDisabled ? undefined : theme.palette.primary.dark
				},
				'&.Mui-disabled': {
					color: alpha(theme.palette.text.disabled, 0.8)
				}
			}}
			ref={activatorRef}
			{...(activatorAttributes ?? {})}
			{...(activatorListeners ?? {})}
		>
			<DragIndicatorIcon fontSize="small" />
		</IconButton>
	);
};

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
	onEditField?: (
	  definitionTableId: string,
	  tableId: string,
	  tableName: string,
	  definitionField: DataDefinitionField
	) => void;
	onUpdateFieldUnique?: (definitionTableId: string, definitionFieldId: string, isUnique: boolean) => Promise<boolean>;
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
	onReorderFields?: (definitionTableId: string, orderedDefinitionFieldIds: string[]) => Promise<boolean>;
	onDeleteField?: (definitionTableId: string, definitionFieldId: string) => Promise<boolean>;
	relationshipBusy?: boolean;
	dataTypes?: DatabricksDataType[];
	legalRequirements?: LegalRequirement[];
	securityClassifications?: SecurityClassification[];
};

const DataDefinitionDetails = ({
	definition,
	canEdit = false,
	inlineSavingState,
	onInlineFieldSubmit,
	onEditField,
	onUpdateFieldUnique,
	onAddExistingFieldInline,
	onCreateFieldInline,
	availableFieldsByTable,
	tableSavingState,
	fieldActionsDisabled = false,
	onBulkPasteResult,
	onCreateRelationship,
	onUpdateRelationship,
	onDeleteRelationship,
	onReorderFields,
	onDeleteField,
	relationshipBusy = false,
	dataTypes = [],
	legalRequirements = [],
	securityClassifications = []
}: DataDefinitionDetailsProps) => {
	const toast = useToast();
	const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>(() => {
		const initial: Record<string, boolean> = {};
		definition.tables.forEach((table) => {
			initial[table.id] = false;
		});
		return initial;
	});

	const tablesById = useMemo(() => {
		const map = new Map<string, DataDefinitionTable>();
		definition.tables.forEach((table) => {
			map.set(table.id, table);
		});
		return map;
	}, [definition.tables]);

	const dataTypeByName = useMemo(() => {
		const map = new Map<string, DatabricksDataType>();
		dataTypes.forEach((type) => {
			const original = type.name;
			map.set(original, type);
			map.set(original.toLowerCase(), type);
			map.set(original.toUpperCase(), type);
		});
		return map;
	}, [dataTypes]);

	const legalRequirementNameById = useMemo(() => {
		const map = new Map<string, string>();
		legalRequirements.forEach((item) => {
			map.set(item.id, item.name);
		});
		return map;
	}, [legalRequirements]);

	const securityClassificationNameById = useMemo(() => {
		const map = new Map<string, string>();
		securityClassifications.forEach((item) => {
			map.set(item.id, item.name);
		});
		return map;
	}, [securityClassifications]);

	const supportsDecimalPlaces = useCallback(
		(name: string) => {
			if (!name) {
				return true;
			}
			const trimmed = name.trim();
			const type =
				dataTypeByName.get(trimmed) ??
				dataTypeByName.get(trimmed.toLowerCase()) ??
				dataTypeByName.get(trimmed.toUpperCase());
			return type ? type.supportsDecimalPlaces : true;
		},
		[dataTypeByName]
	);

	const initialDrafts = useMemo(() => mapDraftsFromDefinition(definition), [definition]);
	const [drafts, setDrafts] = useState<Record<string, FieldDraft>>(initialDrafts);
	const [committed, setCommitted] = useState<Record<string, FieldDraft>>(initialDrafts);
	const [inlineRows, setInlineRows] = useState<Record<string, InlineRowState>>({});
	const [gridViewByTable, setGridViewByTable] = useState<Record<string, boolean>>({});
	const [newMenu, setNewMenu] = useState<{ tableId: string; anchor: HTMLElement } | null>(null);
	const [activeDialog, setActiveDialog] = useState<{ tableId: string; type: InlineRowState['type'] } | null>(null);
	const [dragSource, setDragSource] = useState<{ tableId: string; fieldId: string } | null>(null);
	const [dropTargetTableId, setDropTargetTableId] = useState<string | null>(null);
	const [fieldOrderByTable, setFieldOrderByTable] = useState<Record<string, string[]>>({});
	const [pendingUniqueByField, setPendingUniqueByField] = useState<Record<string, boolean>>({});
	const [uniqueSavingByField, setUniqueSavingByField] = useState<Record<string, boolean>>({});
	const auditFieldIdsByTable = useMemo(() => {
		const map: Record<string, string[]> = {};
		definition.tables.forEach((table) => {
			if (!table.isConstruction) {
				return;
			}
			const ids = table.fields
				.filter((definitionField) => {
					const name = definitionField.field?.name?.trim().toLowerCase();
					return Boolean(name && AUDIT_FIELD_NAMES.has(name));
				})
				.map((definitionField) => definitionField.id);
			if (ids.length) {
				map[table.id] = ids;
			}
		});
		return map;
	}, [definition.tables]);
	const [intendedRelationship, setIntendedRelationship] = useState<{ primaryFieldId: string; foreignFieldId: string } | null>(null);
	const [previewOpen, setPreviewOpen] = useState(false);
	const [previewTableId, setPreviewTableId] = useState<string | null>(null);
	const [previewTableName, setPreviewTableName] = useState<string>('');
	const [previewSchemaName, setPreviewSchemaName] = useState<string | null>(null);
	const [previewData, setPreviewData] = useState<ConnectionTablePreview | null>(null);
	const [previewLoading, setPreviewLoading] = useState(false);
	const [previewError, setPreviewError] = useState<string | null>(null);
	const theme = useTheme();
	const isDarkMode = theme.palette.mode === 'dark';
	const tableCardBaseStyles = useMemo(() => {
		const surface = getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' });
		return {
			background: surface.background,
			boxShadow: surface.boxShadow
		} as const;
	}, [isDarkMode, theme]);
	// relationshipCardBaseStyles was previously an alias but is not used; remove to satisfy linter
	const tableBorderColor = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.45 : 0.28),
		[isDarkMode, theme]
	);
	const tableBorderHighlight = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.85 : 0.55),
		[isDarkMode, theme]
	);
	const tableAccentBorder = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.95 : 0.75),
		[isDarkMode, theme]
	);
	const headerDividerColor = useMemo(
		() => alpha(theme.palette.divider, isDarkMode ? 0.5 : 0.18),
		[isDarkMode, theme]
	);
	const tableTitleColor = useMemo(
		() => (isDarkMode ? alpha(theme.palette.primary.light, 0.9) : theme.palette.primary.dark),
		[isDarkMode, theme]
	);
	const tableSubtitleColor = useMemo(
		() => alpha(isDarkMode ? theme.palette.common.white : theme.palette.text.secondary, isDarkMode ? 0.7 : 0.75),
		[isDarkMode, theme]
	);
	const calloutBackground = useMemo(
		() => (isDarkMode ? alpha(theme.palette.primary.main, 0.18) : alpha(theme.palette.primary.light, 0.08)),
		[isDarkMode, theme]
	);
	const calloutBorder = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.5 : 0.35),
		[isDarkMode, theme]
	);
	const gridBackground = useMemo(
		() => (isDarkMode ? alpha(theme.palette.background.paper, 0.88) : alpha(theme.palette.common.white, 0.98)),
		[isDarkMode, theme]
	);
	const gridShadow = useMemo(
		() => (isDarkMode ? `0 16px 32px ${alpha(theme.palette.common.black, 0.45)}` : `0 6px 18px ${alpha(theme.palette.common.black, 0.08)}`),
		[isDarkMode, theme]
	);
	const gridBorderColor = useMemo(
		() => alpha(theme.palette.divider, isDarkMode ? 0.5 : 0.22),
		[isDarkMode, theme]
	);
	const gridHeaderGradient = useMemo(
		() =>
			isDarkMode
				? `linear-gradient(180deg, ${alpha(theme.palette.primary.main, 0.88)} 0%, ${alpha(theme.palette.primary.dark, 0.82)} 100%)`
				: `linear-gradient(180deg, ${theme.palette.primary.main} 0%, ${alpha(theme.palette.primary.dark, 0.8)} 100%)`,
		[isDarkMode, theme]
	);
	const gridHeaderShadow = useMemo(
		() => (isDarkMode ? `inset 0 -2px 4px ${alpha(theme.palette.common.black, 0.4)}` : `inset 0 -2px 4px ${alpha(theme.palette.primary.dark, 0.35)}`),
		[isDarkMode, theme]
	);
	const gridHeaderTextColor = useMemo(
		() => (isDarkMode ? alpha(theme.palette.common.white, 0.95) : theme.palette.primary.contrastText),
		[isDarkMode, theme]
	);
	const gridEvenRowBackground = useMemo(
		() => alpha(theme.palette.action.hover, isDarkMode ? 0.25 : 0.08),
		[isDarkMode, theme]
	);
	const gridHoverBackground = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.2 : 0.05),
		[isDarkMode, theme]
	);
	const gridSelectedBackground = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.3 : 0.1),
		[isDarkMode, theme]
	);
	const gridEditableBackground = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.18 : 0.06),
		[isDarkMode, theme]
	);
	const gridEditingBackground = useMemo(
		() => (isDarkMode ? alpha(theme.palette.background.paper, 0.92) : alpha(theme.palette.common.white, 0.98)),
		[isDarkMode, theme]
	);
	const placeholderRowBackground = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.24 : 0.1),
		[isDarkMode, theme]
	);
	const placeholderRowHover = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.32 : 0.14),
		[isDarkMode, theme]
	);
	const placeholderRowFocus = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.48 : 0.6),
		[isDarkMode, theme]
	);
	const placeholderTextColor = useMemo(
		() => (isDarkMode ? alpha(theme.palette.common.white, 0.78) : theme.palette.text.secondary),
		[isDarkMode, theme]
	);
	const gridFocusOutline = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.4 : 0.2),
		[isDarkMode, theme]
	);
	const gridFocusBackground = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.14 : 0.02),
		[isDarkMode, theme]
	);
	const inlineInputBackground = useMemo(
		() => (isDarkMode ? alpha(theme.palette.background.paper, 0.85) : alpha(theme.palette.common.white, 0.97)),
		[isDarkMode, theme]
	);
	const inlineInputBorder = useMemo(
		() => alpha(theme.palette.divider, isDarkMode ? 0.55 : 0.45),
		[isDarkMode, theme]
	);
	const inlineInputHoverBorder = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.75 : 0.65),
		[isDarkMode, theme]
	);
	const inlineInputFocusShadow = useMemo(
		() => alpha(theme.palette.primary.main, isDarkMode ? 0.4 : 0.2),
		[isDarkMode, theme]
	);
	const reorderSensors = useSensors(
		useSensor(PointerSensor, { activationConstraint: { distance: 6 } }),
		useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
	);

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

	useEffect(() => {
		setPendingUniqueByField({});
		setUniqueSavingByField({});
	}, [definition.tables]);

	useEffect(() => {
		setFieldOrderByTable((prev) => {
			let changed = false;
			const next = { ...prev };
			definition.tables.forEach((table) => {
				const orderedIds = table.fields
					.filter((definitionField) => definitionField.field != null)
					.map((definitionField) => definitionField.id);
				const existing = prev[table.id];
				const needsUpdate =
					!existing ||
					existing.length !== orderedIds.length ||
					existing.some((value, index) => value !== orderedIds[index]);
				if (needsUpdate) {
					next[table.id] = orderedIds;
					changed = true;
				}
			});
			return changed ? next : prev;
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
						return (a.table?.name ?? '').localeCompare(b.table?.name ?? '');
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
				let nextDraft = {
					...previous,
					[key]: value
				};

				if (key === 'enterpriseAttribute') {
					nextDraft = {
						...nextDraft,
						enterpriseAttribute: normalizeEnterpriseAttribute(value)
					};
				}

				if (key === 'fieldType') {
					nextDraft = {
						...nextDraft,
						fieldType: value
					};
					if (!supportsDecimalPlaces(value)) {
						nextDraft = {
							...nextDraft,
							decimalPlaces: ''
						};
					}
				}

				return {
					...prev,
					[field.id]: nextDraft
				};
			});
		},
		[supportsDecimalPlaces]
	);

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
			const payload: Partial<Record<StringDraftKey, string>> = {
				[key]: currentDraft[key]
			};
			if (
				key === 'fieldType' &&
				currentDraft.decimalPlaces !== currentCommitted.decimalPlaces
			) {
				payload.decimalPlaces = currentDraft.decimalPlaces;
			}
			const success = await onInlineFieldSubmit(field, payload);
			if (success) {
				setCommitted((prev) => {
					const previous = prev[field.id] ?? buildDraft(field);
					return {
						...prev,
						[field.id]: {
							...previous,
							...Object.fromEntries(
								Object.keys(payload).map((changedKey) => [
									changedKey,
									currentDraft[changedKey as StringDraftKey]
								])
							)
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
							...Object.fromEntries(
								Object.keys(payload).map((changedKey) => [
									changedKey,
									currentCommitted[changedKey as StringDraftKey]
								])
							)
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

		const handleUniqueToggle = useCallback(
			async (definitionTableId: string, definitionField: DataDefinitionField, checked: boolean) => {
				if (!canEdit || !onUpdateFieldUnique) {
					return;
				}

				setPendingUniqueByField((prev) => ({ ...prev, [definitionField.id]: checked }));
				setUniqueSavingByField((prev) => ({ ...prev, [definitionField.id]: true }));

				try {
					const success = await onUpdateFieldUnique(definitionTableId, definitionField.id, checked);
					if (!success) {
						setPendingUniqueByField((prev) => {
							const next = { ...prev };
							if (definitionField.isUnique === undefined || definitionField.isUnique === null) {
								delete next[definitionField.id];
							} else {
								next[definitionField.id] = Boolean(definitionField.isUnique);
							}
							return next;
						});
					}
				} finally {
					setUniqueSavingByField((prev) => {
						const next = { ...prev };
						delete next[definitionField.id];
						return next;
					});
				}
			},
			[canEdit, onUpdateFieldUnique]
		);

	const persistReorder = useCallback(
		async (definitionTableId: string, nextOrder: string[], previousOrder: string[]) => {
			if (!onReorderFields) {
				return;
			}
			const success = await onReorderFields(definitionTableId, nextOrder);
			if (!success) {
				setFieldOrderByTable((prev) => ({ ...prev, [definitionTableId]: previousOrder }));
			}
		},
		[onReorderFields]
	);

	const persistDeletion = useCallback(
		async (definitionTableId: string, definitionFieldId: string, previousOrder: string[]) => {
			if (!onDeleteField) {
				return;
			}
			const success = await onDeleteField(definitionTableId, definitionFieldId);
			if (!success) {
				setFieldOrderByTable((prev) => ({ ...prev, [definitionTableId]: previousOrder }));
			}
		},
		[onDeleteField]
	);

	const handleDeleteField = useCallback(
		(definitionTableId: string, definitionFieldId: string) => {
			if (!onDeleteField) {
				return;
			}
			const auditIds = auditFieldIdsByTable[definitionTableId] ?? [];
			if (auditIds.includes(definitionFieldId)) {
				toast.showInfo('Audit fields cannot be removed from constructed tables.');
				return;
			}
			let previousOrder: string[] | null = null;
			setFieldOrderByTable((prev) => {
				const currentOrder = prev[definitionTableId];
				if (!currentOrder || !currentOrder.includes(definitionFieldId)) {
					return prev;
				}
				previousOrder = currentOrder.slice();
				const nextOrder = currentOrder.filter((id) => id !== definitionFieldId);
				return { ...prev, [definitionTableId]: nextOrder };
			});
			if (previousOrder) {
				void persistDeletion(definitionTableId, definitionFieldId, previousOrder);
			}
		},
		[persistDeletion, onDeleteField, auditFieldIdsByTable, toast]
	);

	const handleFieldDragEnd = useCallback(
		(definitionTableId: string, event: DragEndEvent) => {
			if (!onReorderFields) {
				return;
			}
			const { active, over } = event;
			if (!over || active.id === over.id) {
				return;
			}
			const auditIds = auditFieldIdsByTable[definitionTableId] ?? [];
			let previousOrder: string[] | null = null;
			let nextOrder: string[] | null = null;
			setFieldOrderByTable((prev) => {
				const currentOrder = prev[definitionTableId];
				if (!currentOrder) {
					return prev;
				}
				const activeId = String(active.id);
				const overId = String(over.id);
				const nonAuditOrder = currentOrder.filter((id) => !auditIds.includes(id));
				const fromIndex = nonAuditOrder.indexOf(activeId);
				const toIndex = nonAuditOrder.indexOf(overId);
				if (fromIndex === -1 || toIndex === -1) {
					return prev;
				}
				const reorderedNonAudit = arrayMove(nonAuditOrder.slice(), fromIndex, toIndex);
				const nextFullOrder: string[] = [];
				let nonAuditCursor = 0;
				for (let i = 0; i < currentOrder.length; i += 1) {
					const fieldId = currentOrder[i];
					if (auditIds.includes(fieldId)) {
						nextFullOrder.push(fieldId);
					} else {
						nextFullOrder.push(reorderedNonAudit[nonAuditCursor]);
						nonAuditCursor += 1;
					}
				}
				const changed = nextFullOrder.some((id, index) => id !== currentOrder[index]);
				if (!changed) {
					return prev;
				}
				previousOrder = currentOrder.slice();
				nextOrder = nextFullOrder;
				return { ...prev, [definitionTableId]: nextFullOrder };
			});
			if (previousOrder && nextOrder) {
				void persistReorder(definitionTableId, nextOrder, previousOrder);
			}
		},
		[persistReorder, onReorderFields, auditFieldIdsByTable]
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
			updateCreateRow(definitionTableId, (state) => {
				let nextDraft = {
					...state.draft,
					[key]: value
				};

				if (key === 'enterpriseAttribute') {
					nextDraft = {
						...nextDraft,
						enterpriseAttribute: normalizeEnterpriseAttribute(value)
					};
				}

				if (key === 'fieldType') {
					nextDraft = {
						...nextDraft,
						fieldType: value
					};
					if (!supportsDecimalPlaces(value)) {
						nextDraft = {
							...nextDraft,
							decimalPlaces: ''
						};
					}
				}

				return {
					...state,
					draft: nextDraft,
					errors: {
						...state.errors,
						[key]: undefined,
						...(key === 'fieldType' ? { decimalPlaces: undefined } : {})
					}
				};
			});
		},
		[supportsDecimalPlaces, updateCreateRow]
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

	const handleCreateDraftEnterpriseToggle = useCallback(
		(definitionTableId: string, checked: boolean) => {
			updateCreateRow(definitionTableId, (state) => ({
				...state,
				draft: {
					...state.draft,
					enterpriseAttribute: checked ? 'Yes' : 'No'
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
					legalRequirementIdCell,
					securityClassificationIdCell,
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
					legalRequirementId: legalRequirementIdCell ?? '',
					securityClassificationId: securityClassificationIdCell ?? '',
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
		const headers = [...FIELD_COLUMNS.map((column) => column.label), 'Unique', 'Notes'];
		const body = table.fields
			.filter((definitionField) => definitionField.field != null)
			.map((definitionField) => {
				const field = definitionField.field!;
				const values = FIELD_COLUMNS.map((column) => {
					switch (column.key) {
						case 'legalRequirementId': {
							const id = field.legalRequirementId ?? '';
							const label = id ? legalRequirementNameById.get(id) ?? id : '';
							return toCsvString(label);
						}
						case 'securityClassificationId': {
							const id = field.securityClassificationId ?? '';
							const label = id ? securityClassificationNameById.get(id) ?? id : '';
							return toCsvString(label);
						}
						case 'enterpriseAttribute':
							return toCsvString(normalizeEnterpriseAttribute(field.enterpriseAttribute));
						default:
							switch (column.kind) {
								case 'boolean':
									return toCsvString(field[column.key]);
								case 'number':
									return toCsvString(field[column.key] ?? '');
								default:
									return toCsvString(field[column.key] ?? '');
							}
					}
				});
				values.push(toCsvString(definitionField.isUnique ? 'Yes' : 'No'));
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
	}, [legalRequirementNameById, securityClassificationNameById]);

	const handleOpenPreview = useCallback((table: DataDefinitionTable) => {
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
			} catch (error) {
				setPreviewError(error instanceof Error ? error.message : 'Failed to load preview');
			} finally {
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
		if (!previewTableId) return;

		setPreviewLoading(true);
		setPreviewError(null);
		try {
			const data = await fetchTablePreview(previewTableId, 100);
			setPreviewData(data);
		} catch (error) {
			setPreviewError(error instanceof Error ? error.message : 'Failed to load preview');
		} finally {
			setPreviewLoading(false);
		}
	}, [previewTableId]);

	return (
		<Stack spacing={3} sx={{ p: 2 }}>
			{definition.description && (
				<Paper
					variant="outlined"
					sx={{
						p: 2,
						borderRadius: 2,
						background: calloutBackground,
						border: `1px solid ${calloutBorder}`
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
				onInitialRelationshipConsumed={() => setIntendedRelationship(null)}
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

				const auditIds = auditFieldIdsByTable[table.id] ?? [];
				const baseRows: FieldRow[] = table.fields
					.filter((definitionField) => definitionField.field != null)
					.filter((definitionField) => {
						if (!table.isConstruction) {
							return true;
						}
						return !auditIds.includes(definitionField.id);
					})
					.map((definitionField) => ({
						id: definitionField.id,
						notes: definitionField.notes ?? '',
						isUnique:
							definitionField.id in pendingUniqueByField
								? pendingUniqueByField[definitionField.id]
								: Boolean(definitionField.isUnique),
						__definitionField: definitionField,
						...getDraftForField(definitionField.field!)
					}));
				if (isGridView) {
					baseRows.push({
						id: `__placeholder__-${table.id}`,
						notes: '',
						isUnique: false,
						__isPlaceholder: true,
						...createEmptyDraft()
					});
				}

				const nonPlaceholderRows = baseRows.filter((row) => !row.__isPlaceholder);
				const storedOrder = fieldOrderByTable[table.id];
				const orderedRows = isGridView
					? baseRows
					: (storedOrder ?? nonPlaceholderRows.map((row) => row.id))
						.map((rowId) => nonPlaceholderRows.find((row) => row.id === rowId))
						.filter((row): row is FieldRow => Boolean(row));
				const visibleRowIds = orderedRows.filter((row) => !row.__isPlaceholder).map((row) => row.id);
				const dndEnabled = Boolean(
					canEdit &&
					onReorderFields &&
					!isGridView &&
					!fieldActionsDisabled &&
					!tableSaving &&
					visibleRowIds.length > 1
				);
				const actionsDisabled = fieldActionsDisabled || tableSaving || Boolean(inlineState) || isGridView;

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
								if (column.key === 'enterpriseAttribute') {
									return renderBooleanChip(enterpriseAttributeToBoolean(value as string | null | undefined));
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
								const draftValue = (row[draftKey] ?? '') as string;
								const disabled = fieldActionsDisabled || saving;
								const textFieldSx = {
									'& .MuiOutlinedInput-root': {
										borderRadius: 0.75,
										backgroundColor: inlineInputBackground,
										'& fieldset': {
											borderColor: inlineInputBorder
										},
										'&:hover fieldset': {
											borderColor: inlineInputHoverBorder
										},
										'&.Mui-focused fieldset': {
											borderColor: theme.palette.primary.main
										},
										'&.Mui-focused': {
											boxShadow: `0 0 0 2px ${inlineInputFocusShadow}`
										}
									},
									'& .MuiOutlinedInput-input': {
										fontSize: 13.5,
										lineHeight: 1.35,
										padding: theme.spacing(0.55, 1)
									}
								};

								const commitSelectChange = async (nextValue: string, targetKey: StringDraftKey) => {
									handleDraftChange(definitionField.field, targetKey, nextValue);
									await Promise.resolve();
									await handleSubmitChange(definitionField.field, targetKey);
								};

								if (column.key === 'fieldType') {
									return (
										<TextField
											select
											variant="outlined"
											size="small"
											fullWidth
											value={draftValue}
											disabled={disabled}
											onChange={(event) => {
												void commitSelectChange(event.target.value, draftKey);
											}}
											SelectProps={{ displayEmpty: true }}
											sx={textFieldSx}
										>
											<MenuItem value="">
												<em>Select a data type</em>
											</MenuItem>
											{dataTypes.map((type) => (
												<MenuItem key={type.name} value={type.name}>
													{type.name}
												</MenuItem>
											))}
										</TextField>
									);
								}

								if (column.key === 'enterpriseAttribute') {
									const normalizedValue = normalizeEnterpriseAttribute(draftValue);
									const checked = normalizedValue === 'Yes';
									const indeterminate = normalizedValue !== 'Yes' && normalizedValue !== 'No';
									return (
										<Checkbox
											size="small"
											checked={checked}
											indeterminate={indeterminate}
											onChange={(event) => {
												event.preventDefault();
												event.stopPropagation();
												if (disabled) {
													return;
												}
												const nextValue = cycleEnterpriseAttributeValue(draftValue);
												void commitSelectChange(nextValue, draftKey);
											}}
											disabled={disabled}
										/>
									);
								}

								if (column.key === 'legalRequirementId') {
									return (
										<TextField
											select
											variant="outlined"
											size="small"
											fullWidth
											value={draftValue}
											disabled={disabled}
											onChange={(event) => {
												void commitSelectChange(event.target.value, draftKey);
											}}
											SelectProps={{ displayEmpty: true }}
											sx={textFieldSx}
										>
											<MenuItem value="">
												<em>None</em>
											</MenuItem>
											{legalRequirements.map((item) => (
												<MenuItem key={item.id} value={item.id}>
													{item.name}
												</MenuItem>
											))}
										</TextField>
									);
								}

								if (column.key === 'securityClassificationId') {
									return (
										<TextField
											select
											variant="outlined"
											size="small"
											fullWidth
											value={draftValue}
											disabled={disabled}
											onChange={(event) => {
												void commitSelectChange(event.target.value, draftKey);
											}}
											SelectProps={{ displayEmpty: true }}
											sx={textFieldSx}
										>
											<MenuItem value="">
												<em>None</em>
											</MenuItem>
											{securityClassifications.map((item) => (
												<MenuItem key={item.id} value={item.id}>
													{item.name}
												</MenuItem>
											))}
										</TextField>
									);
								}

								if (column.key === 'decimalPlaces') {
									return (
										<TextField
											variant="outlined"
											size="small"
											fullWidth
											value={draftValue}
											type="number"
											disabled={disabled}
											onChange={(event) => handleDraftChange(definitionField.field, draftKey, event.target.value)}
											onBlur={() => {
												void handleSubmitChange(definitionField.field, draftKey);
											}}
											sx={textFieldSx}
										/>
									);
								}

								const inputType = column.kind === 'number' ? 'number' : 'text';

								if (isMultiline) {
									return (
										<GridTextarea
											value={draftValue}
											disabled={disabled}
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
										disabled={disabled}
										onChange={(event) => handleDraftChange(definitionField.field, draftKey, event.target.value)}
										onBlur={() => {
											void handleSubmitChange(definitionField.field, draftKey);
										}}
										sx={textFieldSx}
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
											setDragSource({ tableId: table.id, fieldId: definitionField.id });
											e.dataTransfer.effectAllowed = 'copy';
											e.dataTransfer.setData('application/json', JSON.stringify({
												tableId: table.id,
												definitionFieldId: definitionField.id,
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
											opacity: dragSource?.fieldId === definitionField.id ? 0.7 : 1,
											transition: 'opacity 0.2s'
										}}
									>
										{saving && <CircularProgress size={16} />}
										<Link
											component="button"
											variant="body2"
											underline="hover"
											onClick={() =>
												onEditField?.(table.id, table.tableId, tableName, definitionField)
											}
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

							let transformedValue = value as string | null | undefined;
							if (column.key === 'legalRequirementId') {
								const id = value as string | null | undefined;
								transformedValue = id ? legalRequirementNameById.get(id) ?? null : null;
							}
							if (column.key === 'securityClassificationId') {
								const id = value as string | null | undefined;
								transformedValue = id ? securityClassificationNameById.get(id) ?? null : null;
							}
							const textValue = renderText(transformedValue);
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

				const uniqueColumn: GridColDef<FieldRow> = {
					field: 'isUnique',
					headerName: 'Unique',
					minWidth: 120,
					flex: 0.6,
					sortable: true,
					filterable: true,
					type: 'boolean',
					align: 'center',
					headerAlign: 'center',
					valueGetter: (params) => params.row.isUnique,
					renderCell: (params) => {
						if (params.row.__isPlaceholder) {
							return null;
						}
						const definitionField = params.row.__definitionField;
						const pendingValue = definitionField ? pendingUniqueByField[definitionField.id] : undefined;
						const displayValue = pendingValue ?? Boolean(params.row.isUnique);
						if (isGridView && canEdit && definitionField) {
							const saving = uniqueSavingByField[definitionField.id] ?? false;
							return (
								<Checkbox
									size="small"
									checked={displayValue}
									onChange={(_, checked) => {
										void handleUniqueToggle(table.id, definitionField, checked);
									}}
									disabled={fieldActionsDisabled || tableSaving || saving}
								/>
							);
						}
						return renderBooleanChip(displayValue);
					}
				};

				const columns: GridColDef<FieldRow>[] = [
					{
						field: '__dragHandle',
						headerName: '',
						width: 44,
						minWidth: 44,
						sortable: false,
						filterable: false,
						disableColumnMenu: true,
						headerAlign: 'center',
						align: 'center',
						resizable: false,
						renderCell: (params) => (
							<DragHandleCell
								{...params}
								canEdit={canEdit}
								disableActions={actionsDisabled}
								dndEnabled={dndEnabled}
							/>
						)
					},
					...baseColumns
				];

				const activeColumnIndex = columns.findIndex((column) => column.field === 'active');
				const uniqueInsertIndex = activeColumnIndex >= 0 ? activeColumnIndex + 1 : columns.length;
				columns.splice(uniqueInsertIndex, 0, uniqueColumn);
				columns.push(
					notesColumn,
					{
						field: '__actions',
						headerName: '',
						width: 56,
						minWidth: 52,
						sortable: false,
						filterable: false,
						disableColumnMenu: true,
						headerAlign: 'center',
						align: 'center',
						renderCell: (params) => (
							<RowActionsCell
								{...params}
								canEdit={canEdit}
								disableActions={actionsDisabled}
								onDelete={(definitionFieldId) => handleDeleteField(table.id, definitionFieldId)}
							/>
						)
					}
				);

				const dataGridElement = (
						<DataGrid
							autoHeight
							rows={orderedRows}
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
							slots={dndEnabled ? { toolbar: QuickFilterToolbar, row: DraggableRow } : { toolbar: QuickFilterToolbar }}
							slotProps={dndEnabled ? { row: { disableDrag: false } } : undefined}
							sx={{
								'--DataGrid-rowBorderColor': gridBorderColor,
								'--DataGrid-columnSeparatorColor': gridBorderColor,
								border: `1px solid ${gridBorderColor}`,
								borderRadius: isGridView ? 1.5 : 2,
								background: gridBackground,
								boxShadow: gridShadow,
								'& .MuiDataGrid-columnHeaders': {
									background: gridHeaderGradient,
									borderBottom: `1px solid ${alpha(theme.palette.primary.dark, isDarkMode ? 0.55 : 0.4)}`,
									boxShadow: gridHeaderShadow,
									textTransform: 'uppercase',
									letterSpacing: 0.35,
									color: gridHeaderTextColor
								},
								'& .MuiDataGrid-columnHeader, & .MuiDataGrid-cell': {
									outline: 'none'
								},
								'& .MuiDataGrid-columnHeaderTitle': {
									fontWeight: 700,
									fontSize: 12.5,
									color: gridHeaderTextColor,
									textShadow: isDarkMode
										? `0 1px 2px ${alpha(theme.palette.common.black, 0.55)}`
										: `0 1px 2px ${alpha(theme.palette.primary.dark, 0.4)}`
								},
								'& .MuiDataGrid-columnHeader .MuiSvgIcon-root': {
									color: alpha(gridHeaderTextColor, 0.9)
								},
								'& .MuiDataGrid-cell': {
									display: 'flex',
									alignItems: 'center',
									borderBottom: `1px solid ${alpha(theme.palette.divider, isDarkMode ? 0.32 : 0.18)}`,
									borderRight: `1px solid ${alpha(theme.palette.divider, isDarkMode ? 0.28 : 0.16)}`,
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
								backgroundColor: gridEvenRowBackground
							},
							'& .MuiDataGrid-row:hover .MuiDataGrid-cell': {
								backgroundColor: gridHoverBackground
							},
							'& .MuiDataGrid-row.Mui-selected .MuiDataGrid-cell': {
								backgroundColor: gridSelectedBackground
							},
							'& .MuiDataGrid-cell:focus-within': {
								outline: `2px solid ${gridFocusOutline}`,
								outlineOffset: -2,
								backgroundColor: gridFocusBackground
							},
							'& .MuiDataGrid-cell.MuiDataGrid-cell--editable': {
								backgroundColor: gridEditableBackground
							},
							'& .MuiDataGrid-cell.MuiDataGrid-cell--editing': {
								backgroundColor: gridEditingBackground,
								boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.primary.main, isDarkMode ? 0.5 : 0.45)}`
							},
							'& .MuiDataGrid-virtualScroller': {
								backgroundColor: 'transparent'
							},
							'& .placeholder-row .MuiDataGrid-cell': {
								fontStyle: 'italic',
								color: placeholderTextColor,
								backgroundColor: placeholderRowBackground,
								transition: theme.transitions.create(['background-color', 'box-shadow', 'color'], {
									duration: theme.transitions.duration.shortest
								})
							},
							'& .placeholder-row .MuiDataGrid-cell:focus-within': {
								outline: 'none',
								backgroundColor: placeholderRowFocus,
								boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.primary.main, isDarkMode ? 0.55 : 0.6)}, 0 0 8px ${alpha(theme.palette.primary.main, isDarkMode ? 0.3 : 0.28)}`,
								color: theme.palette.primary.contrastText,
								fontWeight: 500
							},
							'& .placeholder-row:hover .MuiDataGrid-cell': {
								backgroundColor: placeholderRowHover,
								color: theme.palette.text.secondary
							}
							}}
						/>
					);

					const gridContent = dndEnabled ? (
						<DndContext
							sensors={reorderSensors}
							collisionDetection={closestCenter}
							onDragEnd={(event) => handleFieldDragEnd(table.id, event)}
						>
							<SortableContext items={visibleRowIds} strategy={verticalListSortingStrategy}>
								{dataGridElement}
							</SortableContext>
						</DndContext>
					) : (
						dataGridElement
					);

				return (
					<Paper
						key={table.id}
						variant="outlined"
						sx={{
							p: 2,
							borderRadius: 3,
							...tableCardBaseStyles,
							border: `1px solid ${dropTargetTableId === table.id ? tableBorderHighlight : tableBorderColor}`,
							borderLeft: `4px solid ${dropTargetTableId === table.id ? tableBorderHighlight : tableAccentBorder}`,
							transition: theme.transitions.create(['border-color', 'box-shadow', 'transform'], {
								duration: 220,
								easing: theme.transitions.easing.easeOut
							}),
							boxShadow:
								dropTargetTableId === table.id
									? `0 12px 30px ${alpha(theme.palette.primary.main, isDarkMode ? 0.28 : 0.16)}`
									: tableCardBaseStyles.boxShadow,
							transform: dropTargetTableId === table.id ? 'translateY(-2px)' : 'none'
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
								const { tableId: sourceTableId, definitionFieldId, fieldId: legacyFieldId } = data;
								const sourceFieldCandidateId: string | undefined = definitionFieldId ?? legacyFieldId;
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
								const sourceField = sourceTable?.fields.find(
									(field) =>
										(field.id === sourceFieldCandidateId || field.fieldId === sourceFieldCandidateId) &&
										Boolean(field.field)
								);
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
									borderBottom: `1px solid ${headerDividerColor}`
								}}
							>
								<Stack spacing={0.5}>
									<Typography
										variant="h6"
										sx={{
											color: tableTitleColor,
											fontWeight: 700,
											letterSpacing: 0.3
										}}
									>
										{tableName}
									</Typography>
									<Typography variant="body2" sx={{ fontStyle: 'italic', color: tableSubtitleColor }}>
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
									startIcon={<PreviewIcon />}
									onClick={() => handleOpenPreview(table)}
									disabled={tableSaving}
								>
									Preview
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
											'&:focus, &:focus-visible': {
												boxShadow: `0 0 0 3px ${gridFocusOutline}, inset 0 0 0 2px ${alpha(theme.palette.primary.main, isDarkMode ? 0.75 : 0.9)}`,
												backgroundColor: gridFocusBackground
											},
											'&:hover': {
												boxShadow: `0 0 0 1px ${alpha(theme.palette.primary.main, isDarkMode ? 0.35 : 0.22)}`
											}
										}}
									>
										{gridContent}
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
													isOptionEqualToValue={(option, value) => option.id === value?.id}
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
								{inlineType === 'create-new' && inlineState?.type === 'create-new' && (() => {
									return (
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
														select
														label="Field Type"
														value={inlineState.draft.fieldType}
														onChange={(event) => handleCreateDraftTextChange(table.id, 'fieldType', event.target.value)}
														disabled={fieldActionsDisabled || tableSaving}
														fullWidth
														size="small"
														error={Boolean(inlineState.errors.fieldType)}
														helperText={inlineState.errors.fieldType}
														SelectProps={{ displayEmpty: true }}
														InputLabelProps={{ shrink: true }}
													>
														<MenuItem value="">
															<em>Select a data type</em>
														</MenuItem>
														{dataTypes.map((type) => (
															<MenuItem key={type.name} value={type.name}>
																{type.name}
															</MenuItem>
														))}
													</TextField>
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
																InputLabelProps={{ shrink: true }}
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
																error={Boolean(inlineState.errors.decimalPlaces)}
																helperText={inlineState.errors.decimalPlaces}
																InputLabelProps={{ shrink: true }}
															/>
														</Grid>
													</Grid>
													<Box
														sx={{
															display: 'grid',
															gridTemplateColumns: {
																xs: 'repeat(1, minmax(0, 1fr))',
																sm: 'repeat(2, minmax(0, 220px))',
																md: 'repeat(3, minmax(0, 220px))'
															},
															columnGap: 2,
															rowGap: 2,
															alignItems: 'center'
														}}
													>
														<FormControlLabel
															control={
																<Switch
																	checked={normalizeEnterpriseAttribute(inlineState.draft.enterpriseAttribute) === 'Yes'}
																	onChange={(_, checked) => handleCreateDraftEnterpriseToggle(table.id, checked)}
																	disabled={fieldActionsDisabled || tableSaving}
																/>
															}
															label="Enterprise Attribute"
														/>
														<FormControlLabel
															control={
																<Switch
																	checked={inlineState.draft.systemRequired}
																	onChange={(_, checked) =>
																		handleCreateDraftBooleanChange(table.id, 'systemRequired', checked)}
																	disabled={fieldActionsDisabled || tableSaving}
																/>
															}
															label="System Required"
														/>
														<FormControlLabel
															control={
																<Switch
																	checked={inlineState.draft.businessProcessRequired}
																	onChange={(_, checked) =>
																		handleCreateDraftBooleanChange(table.id, 'businessProcessRequired', checked)}
																	disabled={fieldActionsDisabled || tableSaving}
																/>
															}
															label="Business Process Required"
														/>
														<FormControlLabel
															control={
																<Switch
																	checked={inlineState.draft.suppressedField}
																	onChange={(_, checked) =>
																		handleCreateDraftBooleanChange(table.id, 'suppressedField', checked)}
																	disabled={fieldActionsDisabled || tableSaving}
																/>
															}
															label="Suppressed Field"
														/>
														<FormControlLabel
															control={
																<Switch
																	checked={inlineState.draft.active}
																	onChange={(_, checked) =>
																		handleCreateDraftBooleanChange(table.id, 'active', checked)}
																	disabled={fieldActionsDisabled || tableSaving}
																/>
															}
															label="Active"
														/>
													</Box>
													<TextField
														select
														label="Legal Requirement"
														value={inlineState.draft.legalRequirementId}
														onChange={(event) => handleCreateDraftTextChange(table.id, 'legalRequirementId', event.target.value)}
														disabled={fieldActionsDisabled || tableSaving}
														fullWidth
														size="small"
														SelectProps={{ displayEmpty: true }}
														InputLabelProps={{ shrink: true }}
													>
														<MenuItem value="">
															<em>None</em>
														</MenuItem>
														{legalRequirements.map((item) => (
															<MenuItem key={item.id} value={item.id}>
																{item.name}
															</MenuItem>
														))}
													</TextField>
													<TextField
														select
														label="Security Classification"
														value={inlineState.draft.securityClassificationId}
														onChange={(event) => handleCreateDraftTextChange(table.id, 'securityClassificationId', event.target.value)}
														disabled={fieldActionsDisabled || tableSaving}
														fullWidth
														size="small"
														SelectProps={{ displayEmpty: true }}
														InputLabelProps={{ shrink: true }}
													>
														<MenuItem value="">
															<em>None</em>
														</MenuItem>
														{securityClassifications.map((item) => (
															<MenuItem key={item.id} value={item.id}>
																{item.name}
															</MenuItem>
														))}
													</TextField>
													<Stack spacing={1}>
														<Typography variant="subtitle2" color="text.secondary">
															Legal / Regulatory Notes
														</Typography>
														<TextField
															value={inlineState.draft.legalRegulatoryImplications}
															onChange={(event) =>
																handleCreateDraftTextChange(
																	table.id,
																	'legalRegulatoryImplications',
																	event.target.value
																)
															}
															disabled={fieldActionsDisabled || tableSaving}
															fullWidth
															size="small"
															multiline
															minRows={2}
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
													disabled={
														fieldActionsDisabled ||
														tableSaving ||
														!inlineState.draft.name.trim() ||
														!inlineState.draft.fieldType.trim()
													}
												>
													Create field
												</Button>
											</DialogActions>
										</>
									);
								})()}
							</Dialog>
						</Stack>
					</Paper>
				);
			})}
			<ConnectionDataPreviewDialog
				open={previewOpen}
				schemaName={previewSchemaName}
				tableName={previewTableName}
				loading={previewLoading}
				error={previewError}
				preview={previewData}
				onClose={handleClosePreview}
				onRefresh={handleRefreshPreview}
			/>
		</Stack>
	);
};

export default DataDefinitionDetails;
