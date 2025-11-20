import {
	ChangeEvent,
	KeyboardEvent,
	MouseEvent,
	forwardRef,
	useCallback,
	useEffect,
	useImperativeHandle,
	useMemo,
	useRef,
	useState
} from 'react';
import { alpha, useTheme } from '@mui/material/styles';
import {
	Autocomplete,
	Box,
	Button,
	Chip,
	CircularProgress,
	Collapse,
	Dialog,
	DialogActions,
	DialogContent,
	DialogTitle,
	Divider,
	FormControl,
	FormHelperText,
	IconButton,
	InputLabel,
	MenuItem,
	Paper,
	Select,
	SelectChangeEvent,
	Stack,
	TextField,
	ToggleButton,
	ToggleButtonGroup,
	Typography
} from '@mui/material';
import AddLinkIcon from '@mui/icons-material/AddLink';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import ReactFlow, {
	Background,
	Controls,
	Edge,
	MarkerType,
	MiniMap,
	Node,
	ReactFlowInstance,
	ReactFlowProvider,
	useEdgesState,
	useNodesState
} from 'reactflow';
import 'reactflow/dist/style.css';

import ReportTableNode, { ReportTableNodeData, ReportTableNodeGroup } from '@components/reporting/ReportTableNode';
import { useToast } from '@hooks/useToast';
import type {
	DataDefinitionField,
	DataDefinitionJoinType,
	DataDefinitionRelationship,
	DataDefinitionRelationshipInput,
	DataDefinitionRelationshipUpdateInput,
	DataDefinitionTable
} from '../../types/data';
import type {
	CanvasGraphAccess,
	CanvasGraphColumn,
	CanvasGraphColumnTest,
	CanvasGraphColumnTests,
	CanvasGraphEdgeType,
	CanvasGraphJoin,
	CanvasGraphJoinType,
	CanvasGraphLayer,
	CanvasGraphMaterialization,
	CanvasGraphNodeConfig,
	CanvasGraphNodeOrigin,
	CanvasGraphNode,
	CanvasGraphResourceType,
	CanvasGraphEdge,
	CanvasGraph
} from '../../types/canvasGraph';
import { getPanelSurface } from '../../theme/surfaceStyles';

const nodeTypes = { dataDefinitionTable: ReportTableNode } as const;

type BuilderNodeGroup = ReportTableNodeGroup;

const builderNodeGroupOrder: BuilderNodeGroup[] = ['source', 'transform', 'output'];
const outputTypeKeywords = ['output', 'published', 'publish', 'report', 'reporting', 'consumer', 'presentation', 'mart', 'semantic'];
const transformTypeKeywords = ['transform', 'transformation', 'model', 'intermediate', 'staging', 'stage', 'constructed', 'feature', 'prep', 'enrichment', 'processing'];

const matchesTableTypeKeyword = (tableType: string, keywords: readonly string[]) =>
	keywords.some((keyword) => tableType.includes(keyword));

const deriveBuilderNodeGroup = (table: DataDefinitionTable): BuilderNodeGroup => {
	const tableType = table.table.tableType?.trim().toLowerCase() ?? '';
	if (tableType) {
		if (matchesTableTypeKeyword(tableType, outputTypeKeywords)) {
			return 'output';
		}
		if (matchesTableTypeKeyword(tableType, transformTypeKeywords)) {
			return 'transform';
		}
	}
	if (table.isConstruction) {
		return 'transform';
	}
	return 'source';
};

const defaultNodeWidth = 260;
const defaultNodeHeight = 320;
const horizontalGap = 360;
const verticalGap = 360;

const joinTypeLabels: Record<DataDefinitionJoinType, string> = {
	inner: 'Inner Join',
	left: 'Left Join',
	right: 'Right Join'
};

const resourceTypeOptions: CanvasGraphResourceType[] = ['source', 'model', 'seed', 'snapshot', 'exposure', 'metric'];
const layerOptions: CanvasGraphLayer[] = ['source', 'staging', 'intermediate', 'mart', 'analytics', 'exposure'];
const materializationOptions: CanvasGraphMaterialization[] = ['view', 'table', 'incremental', 'ephemeral', 'seed', 'snapshot'];
const accessOptions: CanvasGraphAccess[] = ['public', 'protected', 'private'];
const edgeTypeOptions: CanvasGraphEdgeType[] = ['ref', 'source', 'exposure', 'metric'];

type FieldOption = {
	id: string;
	tableId: string;
	tableLabel: string;
	tableSubtitle: string;
	fieldLabel: string;
	detail: string | null;
	definitionField: DataDefinitionField;
	table: DataDefinitionTable;
};

type FieldDragPayload = {
	tableId: string;
	fieldId: string;
};

type RelationshipEdgeData = {
	relationshipId: string;
	primaryFieldId: string;
	foreignFieldId: string;
	joinType: DataDefinitionJoinType;
	notes?: string | null;
	canvas?: BuilderEdgeCanvasState;
};

type JoinDialogMode = 'create' | 'edit';

type JoinDialogState = {
	mode: JoinDialogMode;
	relationshipId?: string;
	primaryFieldId: string | null;
	primaryTableId: string | null;
	foreignFieldId: string | null;
	foreignTableId: string | null;
	joinType: DataDefinitionJoinType;
	notes: string;
};

type BuilderNodeCanvasState = {
	resourceType: CanvasGraphResourceType;
	layer?: CanvasGraphLayer;
	description?: string | null;
	origin: CanvasGraphNodeOrigin;
	columns: CanvasGraphColumn[];
	config: CanvasGraphNodeConfig;
	meta?: Record<string, unknown>;
};

type BuilderEdgeCanvasState = {
	type: CanvasGraphEdgeType;
	description?: string | null;
	join?: CanvasGraphJoin;
	meta?: Record<string, unknown>;
};

type NodeCanvasFormState = {
	resourceType: CanvasGraphResourceType;
	layer?: CanvasGraphLayer;
	materialization?: CanvasGraphMaterialization;
	access?: CanvasGraphAccess;
	owner: string;
	description: string;
	tags: string[];
	tagsInput: string;
	preHooks: string;
	postHooks: string;
	incrementalStrategy: string;
	incrementalUniqueKey: string;
	incrementalPartitionBy: string;
	incrementalClusterBy: string;
};

type NodeCanvasDialogState = {
	table: DataDefinitionTable;
	columns: CanvasGraphColumn[];
	state: BuilderNodeCanvasState;
	form: NodeCanvasFormState;
};

type JoinCanvasFormState = {
	edgeType: CanvasGraphEdgeType;
	edgeDescription: string;
	joinCondition: string;
};

type PendingEdgeCanvasUpdate = {
	primaryFieldId: string;
	foreignFieldId: string;
	joinType: DataDefinitionJoinType;
	form: JoinCanvasFormState;
};

type DataDefinitionRelationshipBuilderProps = {
	tables: DataDefinitionTable[];
	relationships: DataDefinitionRelationship[];
	canEdit?: boolean;
	onCreateRelationship?: (input: DataDefinitionRelationshipInput) => Promise<boolean>;
	onUpdateRelationship?: (
		relationshipId: string,
		input: DataDefinitionRelationshipUpdateInput
	) => Promise<boolean>;
	onDeleteRelationship?: (relationshipId: string) => Promise<boolean>;
	busy?: boolean;
	initialPrimaryFieldId?: string;
	initialForeignFieldId?: string;
	onInitialRelationshipConsumed?: () => void;
};

export interface DataDefinitionRelationshipBuilderHandle {
	exportCanvasGraph: () => CanvasGraph;
}

const buildTableLabel = (table: DataDefinitionTable) => {
	const alias = table.alias?.trim();
	if (alias) {
		return alias;
	}
	return table.table.name || table.table.physicalName || 'Untitled Table';
};

const buildTableSubtitle = (table: DataDefinitionTable) => {
	if (table.table.schemaName) {
		return `${table.table.schemaName}.${table.table.physicalName}`;
	}
	return table.table.physicalName;
};

const qualifyField = (field: FieldOption | undefined | null) => {
	if (!field) {
		return 'Unknown field';
	}
	return `${field.tableLabel}.${field.fieldLabel}`;
};

const stableStringify = (value: unknown): string => {
	const seen = new WeakSet();
	const serialize = (input: unknown): unknown => {
		if (input === undefined || input === null) {
			return input;
		}
		if (typeof input !== 'object') {
			return input;
		}
		if (seen.has(input as object)) {
			return undefined;
		}
		seen.add(input as object);
		if (Array.isArray(input)) {
			return input.map((entry) => serialize(entry));
		}
		const entries = Object.entries(input as Record<string, unknown>).sort(([firstKey], [secondKey]) =>
			firstKey.localeCompare(secondKey)
		);
		const normalized: Record<string, unknown> = {};
		entries.forEach(([key, entry]) => {
			normalized[key] = serialize(entry);
		});
		return normalized;
	};
	return JSON.stringify(serialize(value));
};

const areNodeCanvasStatesEqual = (
	previous: BuilderNodeCanvasState | undefined,
	next: BuilderNodeCanvasState | undefined
) => stableStringify(previous) === stableStringify(next);

const areEdgeCanvasStatesEqual = (
	previous: BuilderEdgeCanvasState | undefined,
	next: BuilderEdgeCanvasState | undefined
) => stableStringify(previous) === stableStringify(next);

const pruneColumnTests = (
	tests: CanvasGraphColumnTests | undefined,
	columnIds: Set<string>
): CanvasGraphColumnTests | undefined => {
	if (!tests) {
		return undefined;
	}
	const next: CanvasGraphColumnTests = {};
	Object.entries(tests).forEach(([columnId, columnTests]) => {
		if (columnIds.has(columnId) && columnTests.length > 0) {
			next[columnId] = columnTests;
		}
	});
	return Object.keys(next).length > 0 ? next : undefined;
};

const buildCanvasColumns = (
	table: DataDefinitionTable,
	existingColumns?: CanvasGraphColumn[]
): CanvasGraphColumn[] => {
	const existingById = new Map<string, CanvasGraphColumn>();
	existingColumns?.forEach((column) => existingById.set(column.id, column));
	return table.fields
		.filter((definitionField) => Boolean(definitionField.field))
		.map((definitionField) => {
			const existing = existingById.get(definitionField.id);
			return {
				id: definitionField.id,
				name: definitionField.field.name,
				dataType: definitionField.field.fieldType,
				description: definitionField.field.description ?? definitionField.notes ?? undefined,
				tests: existing?.tests ?? []
			};
		});
};

const createDefaultNodeCanvasState = (
	table: DataDefinitionTable,
	columns: CanvasGraphColumn[]
): BuilderNodeCanvasState => {
	const defaultDescription = table.description ?? table.table.description ?? null;
	const defaultResource: CanvasGraphResourceType = table.isConstruction ? 'model' : 'source';
	const defaultLayer: CanvasGraphLayer | undefined = table.isConstruction ? 'intermediate' : 'source';
	const defaultMaterialization: CanvasGraphMaterialization | undefined = table.isConstruction ? 'view' : undefined;
	return {
		resourceType: defaultResource,
		layer: defaultLayer,
		description: defaultDescription,
		origin: {
			dataDefinitionTableId: table.id,
			systemId: table.table.systemId,
			schemaName: table.table.schemaName ?? undefined,
			tableName: table.table.physicalName,
			isConstructed: table.isConstruction
		},
		columns,
		config: {
			materialization: defaultMaterialization,
			access: 'protected',
			tags: [],
			owner: null,
			description: defaultDescription,
			columnTests: undefined
		}
	};
};

const createNodeCanvasForm = (state: BuilderNodeCanvasState): NodeCanvasFormState => ({
	resourceType: state.resourceType,
	layer: state.layer,
	materialization: state.config.materialization,
	access: state.config.access,
	owner: state.config.owner ?? '',
	description: state.description ?? '',
	tags: [...(state.config.tags ?? [])],
	tagsInput: '',
	preHooks: (state.config.hooks?.pre ?? []).join('\n'),
	postHooks: (state.config.hooks?.post ?? []).join('\n'),
	incrementalStrategy: state.config.incremental?.strategy ?? '',
	incrementalUniqueKey: Array.isArray(state.config.incremental?.uniqueKey)
		? state.config.incremental?.uniqueKey.join(', ')
		: state.config.incremental?.uniqueKey ?? '',
	incrementalPartitionBy: Array.isArray(state.config.incremental?.partitionBy)
		? state.config.incremental?.partitionBy.join(', ')
		: state.config.incremental?.partitionBy ?? '',
	incrementalClusterBy: (state.config.incremental?.clusterBy ?? []).join(', ')
});

const parseMultilineInput = (value: string): string[] =>
	value
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter((line) => line.length > 0);

const parseCommaSeparatedInput = (value: string): string[] =>
	value
		.split(',')
		.map((entry) => entry.trim())
		.filter((entry) => entry.length > 0);

const normalizeUniqueKey = (value: string): string | string[] | undefined => {
	const entries = parseCommaSeparatedInput(value);
	if (entries.length === 0) {
		return undefined;
	}
	return entries.length === 1 ? entries[0] : entries;
};

const assembleNodeCanvasState = (
	previous: BuilderNodeCanvasState,
	columns: CanvasGraphColumn[],
	form: NodeCanvasFormState
): BuilderNodeCanvasState => {
	const columnIds = new Set(columns.map((column) => column.id));
	const normalizedTags = Array.from(new Set(form.tags.map((tag) => tag.trim()).filter(Boolean)));
	const trimmedDescription = form.description.trim();
	const trimmedOwner = form.owner.trim();
	const preHooks = parseMultilineInput(form.preHooks);
	const postHooks = parseMultilineInput(form.postHooks);
	const hooks = preHooks.length || postHooks.length ? { pre: preHooks.length ? preHooks : undefined, post: postHooks.length ? postHooks : undefined } : undefined;
	const incrementalStrategy = form.incrementalStrategy.trim();
	const incrementalUniqueKey = normalizeUniqueKey(form.incrementalUniqueKey);
	const incrementalPartitionByValues = parseCommaSeparatedInput(form.incrementalPartitionBy);
	const incrementalPartitionBy = incrementalPartitionByValues.length === 0 ? undefined : incrementalPartitionByValues.length === 1 ? incrementalPartitionByValues[0] : incrementalPartitionByValues;
	const incrementalClusterByValues = parseCommaSeparatedInput(form.incrementalClusterBy);
	const incremental = (() => {
		if (form.materialization !== 'incremental') {
			return undefined;
		}
		if (!incrementalStrategy && !incrementalUniqueKey && !incrementalPartitionBy && incrementalClusterByValues.length === 0) {
			return undefined;
		}
		return {
			strategy: incrementalStrategy || undefined,
			uniqueKey: incrementalUniqueKey,
			partitionBy: incrementalPartitionBy,
			clusterBy: incrementalClusterByValues.length > 0 ? incrementalClusterByValues : undefined
		};
	})();
	return {
		...previous,
		resourceType: form.resourceType,
		layer: form.layer,
		description: trimmedDescription || null,
		columns,
		config: {
			...previous.config,
			materialization: form.materialization,
			access: form.access,
			owner: trimmedOwner ? trimmedOwner : null,
			tags: normalizedTags,
			description: trimmedDescription || null,
			hooks,
			incremental,
			columnTests: pruneColumnTests(previous.config.columnTests, columnIds)
		}
	};
};

const buildJoinConditionPreview = (primary: FieldOption | null, foreign: FieldOption | null) => {
	if (!primary || !foreign) {
		return '';
	}
	return `${qualifyField(foreign)} = ${qualifyField(primary)}`;
};

const createJoinCanvasForm = (
	existing: BuilderEdgeCanvasState | undefined,
	relationship: DataDefinitionRelationship | undefined,
	primaryField: FieldOption | null,
	foreignField: FieldOption | null
): JoinCanvasFormState => ({
	edgeType:
		existing?.type ??
		primaryField?.table.isConstruction
			? 'ref'
			: 'source',
	edgeDescription: existing?.description ?? relationship?.notes ?? '',
	joinCondition: existing?.join?.condition ?? buildJoinConditionPreview(primaryField, foreignField)
});

const assembleEdgeCanvasState = (options: {
	relationship: DataDefinitionRelationship;
	primaryField: FieldOption;
	foreignField: FieldOption;
	previous?: BuilderEdgeCanvasState;
	form?: JoinCanvasFormState;
}): BuilderEdgeCanvasState => {
	const { relationship, primaryField, foreignField, previous, form } = options;
	const resolvedForm = form ?? createJoinCanvasForm(previous, relationship, primaryField, foreignField);
	const normalizedDescription = resolvedForm.edgeDescription.trim();
	const normalizedCondition = resolvedForm.joinCondition.trim() || buildJoinConditionPreview(primaryField, foreignField);
	return {
		type: resolvedForm.edgeType,
		description: normalizedDescription || null,
		join: {
			type: relationship.joinType as CanvasGraphJoinType,
			condition: normalizedCondition || undefined,
			sourceColumns: [
				{
					column: foreignField.definitionField.field.name,
					fieldId: foreignField.id
				}
			],
			targetColumns: [
				{
					column: primaryField.definitionField.field.name,
					fieldId: primaryField.id
				}
			]
		},
		meta: previous?.meta
	};
};

const cloneColumnTestsMap = (tests: CanvasGraphColumnTests | undefined): CanvasGraphColumnTests | undefined => {
	if (!tests) {
		return undefined;
	}
	const next: CanvasGraphColumnTests = {};
	Object.entries(tests).forEach(([columnId, entries]) => {
		if (!entries || entries.length === 0) {
			return;
		}
		const clonedEntries: CanvasGraphColumnTest[] = entries
			.filter((entry) => Boolean(entry))
			.map((entry) => ({
				...entry,
				arguments: entry.arguments ? { ...entry.arguments } : undefined
			}));
		if (clonedEntries.length > 0) {
			next[columnId] = clonedEntries;
		}
	});
	return Object.keys(next).length > 0 ? next : undefined;
};

const cloneNodeConfig = (config: CanvasGraphNodeConfig | undefined): CanvasGraphNodeConfig | undefined => {
	if (!config) {
		return undefined;
	}
	const next: CanvasGraphNodeConfig = {
		materialization: config.materialization,
		access: config.access
	};
	if (config.tags && config.tags.length > 0) {
		next.tags = [...config.tags];
	}
	if (Object.prototype.hasOwnProperty.call(config, 'owner')) {
		next.owner = config.owner ?? null;
	}
	if (Object.prototype.hasOwnProperty.call(config, 'path')) {
		next.path = config.path ?? null;
	}
	if (Object.prototype.hasOwnProperty.call(config, 'sql')) {
		next.sql = config.sql ?? null;
	}
	if (Object.prototype.hasOwnProperty.call(config, 'description')) {
		next.description = config.description ?? null;
	}
	if (config.meta) {
		next.meta = { ...config.meta };
	}
	if (config.hooks) {
		const hooks: CanvasGraphNodeConfig['hooks'] = {};
		if (config.hooks.pre && config.hooks.pre.length > 0) {
			hooks.pre = [...config.hooks.pre];
		}
		if (config.hooks.post && config.hooks.post.length > 0) {
			hooks.post = [...config.hooks.post];
		}
		if (Object.keys(hooks).length > 0) {
			next.hooks = hooks;
		}
	}
	if (config.incremental) {
		const incremental: CanvasGraphNodeConfig['incremental'] = {};
		if (config.incremental.strategy) {
			incremental.strategy = config.incremental.strategy;
		}
		if (config.incremental.uniqueKey !== undefined) {
			incremental.uniqueKey = Array.isArray(config.incremental.uniqueKey)
				? [...config.incremental.uniqueKey]
				: config.incremental.uniqueKey;
		}
		if (config.incremental.partitionBy !== undefined) {
			incremental.partitionBy = Array.isArray(config.incremental.partitionBy)
				? [...config.incremental.partitionBy]
				: config.incremental.partitionBy;
		}
		if (config.incremental.clusterBy && config.incremental.clusterBy.length > 0) {
			incremental.clusterBy = [...config.incremental.clusterBy];
		}
		if (Object.keys(incremental).length > 0) {
			next.incremental = incremental;
		}
	}
	const columnTests = cloneColumnTestsMap(config.columnTests);
	if (columnTests) {
		next.columnTests = columnTests;
	}
	if (config.tests && config.tests.length > 0) {
		next.tests = config.tests.map((entry) => ({
			...entry,
			arguments: entry.arguments ? { ...entry.arguments } : undefined
		}));
	}
	if (config.exposures && config.exposures.length > 0) {
		next.exposures = config.exposures.map((entry) => ({
			...entry
		}));
	}
	if (config.metrics && config.metrics.length > 0) {
		next.metrics = config.metrics.map((entry) => ({
			...entry,
			meta: entry.meta ? { ...entry.meta } : undefined
		}));
	}
	return next;
};

const cloneColumns = (columns: CanvasGraphColumn[]): CanvasGraphColumn[] =>
	columns.map((column) => ({
		...column,
		tests: column.tests?.map((entry) => ({
			...entry,
			arguments: entry.arguments ? { ...entry.arguments } : undefined
		})),
		meta: column.meta ? { ...column.meta } : undefined
	}));

const cloneJoin = (join: CanvasGraphJoin | undefined): CanvasGraphJoin | undefined => {
	if (!join) {
		return undefined;
	}
	return {
		...join,
		sourceColumns: join.sourceColumns?.map((column) => ({ ...column })),
		targetColumns: join.targetColumns?.map((column) => ({ ...column })),
		filters: join.filters?.map((filter) => ({ ...filter }))
	};
};

const BuilderContent = forwardRef<
	DataDefinitionRelationshipBuilderHandle,
	DataDefinitionRelationshipBuilderProps
>((props, ref) => {
	const {
		tables,
		relationships,
		canEdit = false,
		onCreateRelationship,
		onUpdateRelationship,
		onDeleteRelationship,
		busy = false,
		initialPrimaryFieldId,
		initialForeignFieldId,
		onInitialRelationshipConsumed
	} = props;
	const theme = useTheme();
	const isDarkMode = theme.palette.mode === 'dark';
	const relationshipCardSurface = useMemo(() => getPanelSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }), [isDarkMode, theme]);
	const relationshipBorderColor = useMemo(() => alpha(theme.palette.primary.main, isDarkMode ? 0.45 : 0.28), [isDarkMode, theme]);
	const relationshipAccentColor = useMemo(() => alpha(theme.palette.primary.main, isDarkMode ? 0.95 : 0.75), [isDarkMode, theme]);
	const headerDividerColor = useMemo(() => alpha(theme.palette.divider, isDarkMode ? 0.5 : 0.18), [isDarkMode, theme]);
	const relationshipTitleColor = useMemo(() => (isDarkMode ? alpha(theme.palette.primary.light, 0.9) : theme.palette.primary.dark), [isDarkMode, theme]);
	const relationshipCalloutBackground = useMemo(() => (isDarkMode ? alpha(theme.palette.primary.main, 0.18) : alpha(theme.palette.primary.light, 0.08)), [isDarkMode, theme]);
	const relationshipCalloutBorder = useMemo(() => alpha(theme.palette.primary.main, isDarkMode ? 0.5 : 0.35), [isDarkMode, theme]);
	const toast = useToast();
	const [nodes, setNodes, onNodesChange] = useNodesState<ReportTableNodeData>([]);
	const [edges, setEdges, onEdgesChange] = useEdgesState<RelationshipEdgeData>([]);
	const [joinDialog, setJoinDialog] = useState<JoinDialogState | null>(null);
	const [dialogSubmitting, setDialogSubmitting] = useState(false);
	const [expanded, setExpanded] = useState(false);
	const draggedFieldRef = useRef<FieldDragPayload | null>(null);
	const reactFlowInstanceRef = useRef<ReactFlowInstance<ReportTableNodeData> | null>(null);
	const initialRelationshipHandledRef = useRef(false);
	const [nodeCanvasSettings, setNodeCanvasSettings] = useState<Map<string, BuilderNodeCanvasState>>(new Map());
	const [edgeCanvasSettings, setEdgeCanvasSettings] = useState<Map<string, BuilderEdgeCanvasState>>(new Map());
	const [nodeCanvasDialog, setNodeCanvasDialog] = useState<NodeCanvasDialogState | null>(null);
	const [nodeCanvasDialogError, setNodeCanvasDialogError] = useState<string | null>(null);
	const [joinCanvasForm, setJoinCanvasForm] = useState<JoinCanvasFormState | null>(null);
	const pendingEdgeCanvasUpdateRef = useRef<PendingEdgeCanvasUpdate | null>(null);

	const allowInteractions = canEdit && !busy && !dialogSubmitting;

	const tableGrouping = useMemo(() => {
		const groups: Record<BuilderNodeGroup, DataDefinitionTable[]> = {
			source: [],
			transform: [],
			output: []
		};
		const groupById = new Map<string, BuilderNodeGroup>();
		tables.forEach((table) => {
			const group = deriveBuilderNodeGroup(table);
			groupById.set(table.id, group);
			groups[group].push(table);
		});
		const orderedTables = builderNodeGroupOrder.flatMap((group) => groups[group]);
		return { groups, orderedTables, groupById };
	}, [tables]);

	const orderedTables = tableGrouping.orderedTables;
	const tableGroupById = tableGrouping.groupById;

	const fieldOptions = useMemo<FieldOption[]>(() => {
		const options: FieldOption[] = [];
		orderedTables.forEach((table) => {
			const tableLabel = buildTableLabel(table);
			const tableSubtitle = buildTableSubtitle(table);
			table.fields.forEach((definitionField) => {
				if (!definitionField.field) {
					return;
				}
				options.push({
					id: definitionField.id,
					tableId: table.id,
					tableLabel,
					tableSubtitle,
					fieldLabel: definitionField.field.name,
					detail: definitionField.field.fieldType ?? null,
					definitionField,
					table
				});
			});
		});
		options.sort((a, b) => {
			if (a.tableLabel === b.tableLabel) {
				return a.fieldLabel.localeCompare(b.fieldLabel);
			}
			return a.tableLabel.localeCompare(b.tableLabel);
		});
		return options;
	}, [orderedTables]);

	const fieldOptionsById = useMemo(() => {
		const map = new Map<string, FieldOption>();
		fieldOptions.forEach((option) => map.set(option.id, option));
		return map;
	}, [fieldOptions]);

	const exportCanvasGraph = useCallback((): CanvasGraph => {
		const instanceNodes = reactFlowInstanceRef.current?.getNodes?.() ?? nodes;
		const instanceEdges = reactFlowInstanceRef.current?.getEdges?.() ?? edges;
		const tableById = new Map<string, DataDefinitionTable>();
		tables.forEach((table) => tableById.set(table.id, table));
		const warnings: string[] = [];
		const nodePayloads: CanvasGraphNode[] = [];
		instanceNodes.forEach((node) => {
			const table = tableById.get(node.id);
			let canvasState = nodeCanvasSettings.get(node.id);
			if (!canvasState) {
				if (table) {
					const fallbackColumns = buildCanvasColumns(table);
					canvasState = createDefaultNodeCanvasState(table, fallbackColumns);
					warnings.push(`Missing Canvas settings for table ${node.id}; using defaults.`);
				} else {
					warnings.push(`Missing Canvas settings for table ${node.id}.`);
					return;
				}
			}
			const width = typeof node.width === 'number' ? node.width : undefined;
			const height = typeof node.height === 'number' ? node.height : undefined;
			const size = width !== undefined && height !== undefined ? { width, height } : undefined;
			const origin = canvasState.origin ? { ...canvasState.origin } : undefined;
			const config = cloneNodeConfig(canvasState.config);
			const columns = cloneColumns(canvasState.columns);
			const meta = canvasState.meta ? { ...canvasState.meta } : undefined;
			const description = Object.prototype.hasOwnProperty.call(canvasState, 'description')
				? canvasState.description ?? null
				: undefined;
			const label = table ? buildTableLabel(table) : node.data?.label ?? node.id;
			const name = table?.table.physicalName ?? table?.table.name ?? (typeof label === 'string' ? label : node.id);
			const payload: CanvasGraphNode = {
				id: node.id,
				name,
				label: typeof label === 'string' ? label : node.id,
				resourceType: canvasState.resourceType,
				position: {
					x: node.position.x,
					y: node.position.y
				},
				size,
				layer: canvasState.layer,
				description,
				origin,
				config,
				columns,
				meta
			};
			nodePayloads.push(payload);
		});
		nodePayloads.sort((first, second) => {
			if (first.position.y !== second.position.y) {
				return first.position.y - second.position.y;
			}
			if (first.position.x !== second.position.x) {
				return first.position.x - second.position.x;
			}
			return first.id.localeCompare(second.id);
		});
		const relationshipById = new Map<string, DataDefinitionRelationship>();
		relationships.forEach((relationship) => relationshipById.set(relationship.id, relationship));
		const edgePayloads: CanvasGraphEdge[] = instanceEdges.map((edge) => {
			const relationship = relationshipById.get(edge.id);
			const baseState = edgeCanvasSettings.get(edge.id);
			let canvasState = baseState;
			if (!canvasState && relationship) {
				const primaryField = fieldOptionsById.get(relationship.primaryFieldId) ?? null;
				const foreignField = fieldOptionsById.get(relationship.foreignFieldId) ?? null;
				if (primaryField && foreignField) {
					canvasState = assembleEdgeCanvasState({
						relationship,
						primaryField,
						foreignField
					});
				} else {
					warnings.push(`Missing field metadata for relationship ${edge.id}.`);
				}
			} else if (!canvasState && !relationship) {
				warnings.push(`Relationship metadata missing for edge ${edge.id}.`);
			}
			const payload: CanvasGraphEdge = {
				id: edge.id,
				source: edge.source,
				target: edge.target,
				type: canvasState?.type ?? 'ref',
				description: canvasState ? canvasState.description ?? null : undefined,
				relationshipId: relationship?.id ?? edge.data?.relationshipId,
				join: cloneJoin(canvasState?.join),
				meta: canvasState?.meta ? { ...canvasState.meta } : undefined
			};
			return payload;
		});
		edgePayloads.sort((first, second) => first.id.localeCompare(second.id));
		const graph: CanvasGraph = {
			nodes: nodePayloads,
			edges: edgePayloads
		};
		if (warnings.length > 0) {
			graph.warnings = warnings;
		}
		return graph;
	}, [
		edgeCanvasSettings,
		edges,
		fieldOptionsById,
		nodeCanvasSettings,
		nodes,
		reactFlowInstanceRef,
		relationships,
		tables
	]);

	useImperativeHandle(ref, () => ({
		exportCanvasGraph
	}), [exportCanvasGraph]);

	useEffect(() => {
		setNodeCanvasSettings((previous) => {
			const next = new Map<string, BuilderNodeCanvasState>();
			tables.forEach((table) => {
				const existing = previous.get(table.id);
				const columns = buildCanvasColumns(table, existing?.columns);
				const resolvedDescription = existing?.description ?? table.description ?? table.table.description ?? null;
				const columnIds = new Set(columns.map((column) => column.id));
				const candidate: BuilderNodeCanvasState = existing
					? {
						...existing,
						description: resolvedDescription,
						origin: {
							dataDefinitionTableId: table.id,
							systemId: table.table.systemId,
							schemaName: table.table.schemaName ?? undefined,
							tableName: table.table.physicalName,
							isConstructed: table.isConstruction
						},
						columns,
						config: {
							...existing.config,
							description: existing.config.description ?? resolvedDescription,
							columnTests: pruneColumnTests(existing.config.columnTests, columnIds)
						}
					}
					: createDefaultNodeCanvasState(table, columns);

				const current = existing ?? null;
				if (current && areNodeCanvasStatesEqual(current, candidate)) {
					next.set(table.id, current);
					return;
				}
				next.set(table.id, candidate);
			});
			return next;
		});
	}, [tables]);

	const activeFieldIds = useMemo(() => {
		const ids = new Set<string>();
		if (joinDialog?.primaryFieldId) {
			ids.add(joinDialog.primaryFieldId);
		}
		if (joinDialog?.foreignFieldId) {
			ids.add(joinDialog.foreignFieldId);
		}
		return ids;
	}, [joinDialog]);

	useEffect(() => {
		setEdgeCanvasSettings((previous) => {
			const next = new Map<string, BuilderEdgeCanvasState>();
			let matchedPending = false;
			relationships.forEach((relationship) => {
				const primaryField = fieldOptionsById.get(relationship.primaryFieldId);
				const foreignField = fieldOptionsById.get(relationship.foreignFieldId);
				if (!primaryField || !foreignField) {
					return;
				}
				const existing = previous.get(relationship.id);
				const pending = pendingEdgeCanvasUpdateRef.current;
				const shouldApplyPending = Boolean(
					pending &&
					pending.primaryFieldId === relationship.primaryFieldId &&
					pending.foreignFieldId === relationship.foreignFieldId &&
					pending.joinType === relationship.joinType
				);
				const candidate = assembleEdgeCanvasState({
					relationship,
					primaryField,
					foreignField,
					previous: existing,
					form: shouldApplyPending ? pendingEdgeCanvasUpdateRef.current?.form : undefined
				});
				if (shouldApplyPending) {
					matchedPending = true;
				}
				if (existing && areEdgeCanvasStatesEqual(existing, candidate)) {
					next.set(relationship.id, existing);
					return;
				}
				next.set(relationship.id, candidate);
			});
			if (matchedPending) {
				pendingEdgeCanvasUpdateRef.current = null;
			}
			return next;
		});
	}, [fieldOptionsById, relationships]);

	const joinColors = useMemo(
		() => ({
			inner: theme.palette.success.main,
			left: theme.palette.primary.main,
			right: theme.palette.secondary.main
		}),
	 [theme]
	);

	const openJoinDialog = useCallback(
		(
			mode: JoinDialogMode,
			payload: {
				primaryFieldId?: string | null;
				foreignFieldId?: string | null;
				relationship?: DataDefinitionRelationship;
				joinType?: DataDefinitionJoinType;
				notes?: string | null;
			}
		) => {
			if (!canEdit) {
				return;
			}

			setExpanded(true);

			const primaryField = payload.primaryFieldId
				? fieldOptionsById.get(payload.primaryFieldId) ?? null
				: payload.relationship?.primaryFieldId
					? fieldOptionsById.get(payload.relationship.primaryFieldId) ?? null
					: null;
			const foreignField = payload.foreignFieldId
				? fieldOptionsById.get(payload.foreignFieldId) ?? null
				: payload.relationship?.foreignFieldId
					? fieldOptionsById.get(payload.relationship.foreignFieldId) ?? null
					: null;
			const existingEdge = payload.relationship ? edgeCanvasSettings.get(payload.relationship.id) : undefined;
			setJoinCanvasForm(createJoinCanvasForm(existingEdge, payload.relationship, primaryField, foreignField));

			setJoinDialog({
				mode,
				relationshipId: payload.relationship?.id,
				primaryFieldId: primaryField?.id ?? payload.primaryFieldId ?? payload.relationship?.primaryFieldId ?? null,
				primaryTableId: primaryField?.tableId ?? payload.relationship?.primaryTableId ?? null,
				foreignFieldId: foreignField?.id ?? payload.foreignFieldId ?? payload.relationship?.foreignFieldId ?? null,
				foreignTableId: foreignField?.tableId ?? payload.relationship?.foreignTableId ?? null,
				joinType: payload.joinType ?? payload.relationship?.joinType ?? 'inner',
				notes: payload.notes ?? payload.relationship?.notes ?? ''
			});
		},
		[canEdit, edgeCanvasSettings, fieldOptionsById]
	);

	const handleFieldDragStart = useCallback(
		(payload: FieldDragPayload) => {
			if (!allowInteractions) {
				return;
			}
			draggedFieldRef.current = payload;
		},
		[allowInteractions]
	);

	const handleFieldDragEnd = useCallback(() => {
		draggedFieldRef.current = null;
	}, []);

	const handleFieldJoin = useCallback(
		(
			source: { tableId: string; fieldId: string } | null,
			target: { tableId: string; fieldId: string }
		) => {
			if (!allowInteractions) {
				return;
			}

			const effectiveSource = source ?? draggedFieldRef.current;
			if (!effectiveSource) {
				return;
			}

			const primaryField = fieldOptionsById.get(target.fieldId);
			const foreignField = fieldOptionsById.get(effectiveSource.fieldId);

			if (!primaryField || !foreignField) {
				toast.showError('Unable to determine both fields for the relationship.');
				return;
			}

			if (primaryField.id === foreignField.id) {
				toast.showError('Please select two different fields to define a relationship.');
				return;
			}

			draggedFieldRef.current = null;
			openJoinDialog('create', {
				primaryFieldId: primaryField.id,
				foreignFieldId: foreignField.id
			});
		},
		[allowInteractions, fieldOptionsById, openJoinDialog, toast]
	);

	const handleNodeCanvasEdit = useCallback(
		(tableId: string) => {
			if (!allowInteractions) {
				return;
			}
			const table = tables.find((item) => item.id === tableId);
			if (!table) {
				return;
			}
			const existing = nodeCanvasSettings.get(tableId);
			const columns = buildCanvasColumns(table, existing?.columns);
			const baseState = existing ? { ...existing, columns } : createDefaultNodeCanvasState(table, columns);
			if (!existing || !areNodeCanvasStatesEqual(existing, baseState)) {
				setNodeCanvasSettings((current) => {
					const next = new Map(current);
					next.set(tableId, baseState);
					return next;
				});
			}
			setNodeCanvasDialog({
				table,
				columns,
				state: baseState,
				form: createNodeCanvasForm(baseState)
			});
			nodeCanvasDialogError && setNodeCanvasDialogError(null);
		},
		[allowInteractions, nodeCanvasDialogError, nodeCanvasSettings, tables]
	);

	useEffect(() => {
		const tableCount = orderedTables.length;
		const columnCount = Math.max(1, Math.ceil(Math.sqrt(Math.max(tableCount, 1))));

		const nextNodes: Node<ReportTableNodeData>[] = orderedTables.map((table, index) => {
			const column = index % columnCount;
			const row = Math.floor(index / columnCount);
			const position = {
				x: column * horizontalGap,
				y: row * verticalGap
			};
			const selectedFieldIds = table.fields
				.filter((definitionField) => activeFieldIds.has(definitionField.id))
				.map((definitionField) => definitionField.id);
			const canvas = nodeCanvasSettings.get(table.id) ?? null;
			const group = tableGroupById.get(table.id) ?? 'source';

			return {
				id: table.id,
				type: 'dataDefinitionTable',
				position,
				data: {
					tableId: table.id,
					label: buildTableLabel(table),
					subtitle: buildTableSubtitle(table),
					group,
					fields: table.fields
						.filter((definitionField) => Boolean(definitionField.field))
						.map((definitionField) => ({
							id: definitionField.id,
							name: definitionField.field.name,
							type: definitionField.field.fieldType ?? null,
							description: definitionField.field.description ?? definitionField.notes ?? null
						})),
					canvas: canvas ?? undefined,
					allowSelection: false,
					onFieldDragStart: allowInteractions ? handleFieldDragStart : undefined,
					onFieldDragEnd: allowInteractions ? handleFieldDragEnd : undefined,
					onFieldJoin: allowInteractions ? handleFieldJoin : undefined,
					onRemoveTable: undefined,
					onEditCanvas: allowInteractions ? handleNodeCanvasEdit : undefined,
					selectedFieldIds
				} satisfies ReportTableNodeData,
				width: defaultNodeWidth,
				height: defaultNodeHeight
			} satisfies Node<ReportTableNodeData>;
		});

		setNodes(nextNodes);
		requestAnimationFrame(() => {
			if (reactFlowInstanceRef.current && nextNodes.length > 0) {
				reactFlowInstanceRef.current.fitView({ padding: 0.45, duration: 300, includeHiddenNodes: true });
			}
		});
	}, [
		activeFieldIds,
		allowInteractions,
		handleFieldDragEnd,
		handleFieldDragStart,
		handleFieldJoin,
		handleNodeCanvasEdit,
		nodeCanvasSettings,
		orderedTables,
		reactFlowInstanceRef,
		setNodes,
		tableGroupById
	]);

	useEffect(() => {
		const nextEdges: Edge<RelationshipEdgeData>[] = [];
		relationships.forEach((relationship) => {
			const primaryField = fieldOptionsById.get(relationship.primaryFieldId);
			const foreignField = fieldOptionsById.get(relationship.foreignFieldId);

			if (!primaryField || !foreignField) {
				return;
			}

			const color = joinColors[relationship.joinType];
			const canvas = edgeCanvasSettings.get(relationship.id);

			nextEdges.push({
				id: relationship.id,
				source: foreignField.tableId,
				sourceHandle: `source:${foreignField.id}`,
				target: primaryField.tableId,
				targetHandle: `target:${primaryField.id}`,
				type: 'smoothstep',
				style: {
					stroke: color,
					strokeWidth: 1.8
				},
				markerEnd: {
					type: MarkerType.ArrowClosed,
					color,
					width: 18,
					height: 18
				},
				data: {
					relationshipId: relationship.id,
					primaryFieldId: relationship.primaryFieldId,
					foreignFieldId: relationship.foreignFieldId,
					joinType: relationship.joinType,
					notes: relationship.notes ?? null,
					canvas: canvas
				}
			});
		});

		setEdges(nextEdges);
	}, [edgeCanvasSettings, fieldOptionsById, joinColors, relationships, setEdges]);

	useEffect(() => {
		if (!allowInteractions) {
			return;
		}
		if (initialRelationshipHandledRef.current) {
			return;
		}
		if (!initialPrimaryFieldId || !initialForeignFieldId) {
			return;
		}
		const primary = fieldOptionsById.get(initialPrimaryFieldId);
		const foreign = fieldOptionsById.get(initialForeignFieldId);
		if (!primary || !foreign) {
			return;
		}

		initialRelationshipHandledRef.current = true;
		openJoinDialog('create', {
			primaryFieldId: primary.id,
			foreignFieldId: foreign.id
		});
		onInitialRelationshipConsumed?.();
	}, [
		allowInteractions,
		fieldOptionsById,
		initialForeignFieldId,
		initialPrimaryFieldId,
		onInitialRelationshipConsumed,
		openJoinDialog
	]);

	useEffect(() => {
		const instance = reactFlowInstanceRef.current;
		if (!instance) {
			return;
		}
		instance.fitView({ padding: 0.4, duration: 400 });
	}, [nodes.length, relationships.length]);

	const handleFlowInit = useCallback((instance: ReactFlowInstance<ReportTableNodeData>) => {
		reactFlowInstanceRef.current = instance;
		instance.fitView({ padding: 0.45, duration: 400 });
	}, []);

	const handleEdgeClick = useCallback(
		(event: MouseEvent, edge: Edge<RelationshipEdgeData>) => {
			event.preventDefault();
			event.stopPropagation();
			if (!allowInteractions) {
				return;
			}
			const relationship = relationships.find((item) => item.id === edge.data?.relationshipId);
			if (!relationship) {
				return;
			}
			openJoinDialog('edit', {
				relationship,
				primaryFieldId: relationship.primaryFieldId,
				foreignFieldId: relationship.foreignFieldId,
				joinType: relationship.joinType,
				notes: relationship.notes ?? ''
			});
		},
		[allowInteractions, openJoinDialog, relationships]
	);

	const handleDialogClose = () => {
		if (dialogSubmitting) {
			return;
		}
		setJoinDialog(null);
		setJoinCanvasForm(null);
	};

	const buildDefaultJoinCanvasForm = useCallback((): JoinCanvasFormState => {
		const relationship = joinDialog?.relationshipId
			? relationships.find((item) => item.id === joinDialog.relationshipId)
			: undefined;
		const primary = joinDialog?.primaryFieldId ? fieldOptionsById.get(joinDialog.primaryFieldId) ?? null : null;
		const foreign = joinDialog?.foreignFieldId ? fieldOptionsById.get(joinDialog.foreignFieldId) ?? null : null;
		return createJoinCanvasForm(
			joinDialog?.relationshipId ? edgeCanvasSettings.get(joinDialog.relationshipId) : undefined,
			relationship,
			primary,
			foreign
		);
	}, [edgeCanvasSettings, fieldOptionsById, joinDialog, relationships]);

	const updateJoinCanvasForm = useCallback(
		(updater: (form: JoinCanvasFormState) => JoinCanvasFormState) => {
			setJoinCanvasForm((previous) => updater(previous ?? buildDefaultJoinCanvasForm()));
		},
		[buildDefaultJoinCanvasForm]
	);

	const handleNodeCanvasClose = () => {
		setNodeCanvasDialog(null);
		setNodeCanvasDialogError(null);
	};

	const updateNodeCanvasForm = useCallback(
		(updater: (form: NodeCanvasFormState) => NodeCanvasFormState) => {
			setNodeCanvasDialog((previous) => {
				if (!previous) {
					return previous;
				}
				return {
					...previous,
					form: updater(previous.form)
				};
			});
		},
		[]
	);

	const handleNodeCanvasResourceTypeChange = (event: SelectChangeEvent<CanvasGraphResourceType>) => {
		const value = event.target.value as CanvasGraphResourceType;
		nodeCanvasDialogError && setNodeCanvasDialogError(null);
		updateNodeCanvasForm((form) => ({ ...form, resourceType: value }));
	};

	const handleNodeCanvasLayerChange = (event: SelectChangeEvent<string>) => {
		const value = (event.target.value as CanvasGraphLayer | '') || undefined;
		updateNodeCanvasForm((form) => ({ ...form, layer: value }));
	};

	const handleNodeCanvasMaterializationChange = (event: SelectChangeEvent<string>) => {
		const value = (event.target.value as CanvasGraphMaterialization | '') || undefined;
		updateNodeCanvasForm((form) => ({ ...form, materialization: value }));
	};

	const handleNodeCanvasAccessChange = (event: SelectChangeEvent<CanvasGraphAccess>) => {
		const value = event.target.value as CanvasGraphAccess;
		updateNodeCanvasForm((form) => ({ ...form, access: value }));
	};

	const handleNodeCanvasOwnerChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, owner: value }));
	};

	const handleNodeCanvasDescriptionChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, description: value }));
	};

	const handleNodeCanvasPreHooksChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, preHooks: value }));
	};

	const handleNodeCanvasPostHooksChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, postHooks: value }));
	};

	const handleNodeCanvasIncrementalStrategyChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, incrementalStrategy: value }));
	};

	const handleNodeCanvasIncrementalUniqueKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, incrementalUniqueKey: value }));
	};

	const handleNodeCanvasIncrementalPartitionByChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, incrementalPartitionBy: value }));
	};

	const handleNodeCanvasIncrementalClusterByChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, incrementalClusterBy: value }));
	};

	const handleNodeCanvasTagsInputChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateNodeCanvasForm((form) => ({ ...form, tagsInput: value }));
	};

	const addNodeCanvasTag = useCallback(
		(tag: string) => {
			const candidate = tag.trim();
			if (!candidate) {
				return;
			}
			updateNodeCanvasForm((form) => {
				if (form.tags.includes(candidate)) {
					return { ...form, tagsInput: '' };
				}
				return { ...form, tags: [...form.tags, candidate], tagsInput: '' };
			});
		},
		[updateNodeCanvasForm]
	);

	const handleNodeCanvasTagsKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
		if (!nodeCanvasDialog) {
			return;
		}
		if (event.key === 'Enter' || event.key === 'Tab' || event.key === ',') {
			event.preventDefault();
			const currentValue = nodeCanvasDialog.form.tagsInput;
			addNodeCanvasTag(currentValue);
		}
	};

	const handleNodeCanvasRemoveTag = (tag: string) => {
		updateNodeCanvasForm((form) => ({
			...form,
			tags: form.tags.filter((existing) => existing !== tag)
		}));
	};

	const handleNodeCanvasSubmit = () => {
		if (!nodeCanvasDialog) {
			return;
		}
		if (!nodeCanvasDialog.form.resourceType) {
			setNodeCanvasDialogError('Please select a resource type for this table.');
			return;
		}
		const nextState = assembleNodeCanvasState(nodeCanvasDialog.state, nodeCanvasDialog.columns, nodeCanvasDialog.form);
		setNodeCanvasSettings((current) => {
			const next = new Map(current);
			next.set(nodeCanvasDialog.table.id, nextState);
			return next;
		});
		handleNodeCanvasClose();
	};

	const handleNodeCanvasTagsBlur = () => {
		if (!nodeCanvasDialog?.form.tagsInput.trim()) {
			return;
		}
		addNodeCanvasTag(nodeCanvasDialog.form.tagsInput);
	};

	const handleJoinTypeChange = (_event: MouseEvent<HTMLElement>, value: DataDefinitionJoinType | null) => {
		if (!value) {
			return;
		}
		setJoinDialog((previous) => (previous ? { ...previous, joinType: value } : previous));
	};

	const handleJoinCanvasEdgeTypeChange = (event: SelectChangeEvent<CanvasGraphEdgeType>) => {
		const value = event.target.value as CanvasGraphEdgeType;
		updateJoinCanvasForm((form) => ({ ...form, edgeType: value }));
	};

	const handleJoinCanvasConditionChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateJoinCanvasForm((form) => ({ ...form, joinCondition: value }));
	};

	const handleJoinCanvasDescriptionChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		updateJoinCanvasForm((form) => ({ ...form, edgeDescription: value }));
	};

	const handleNotesChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		setJoinDialog((previous) => (previous ? { ...previous, notes: value } : previous));
	};

	const handleFieldSelection = (key: 'primary' | 'foreign', option: FieldOption | null) => {
		const previousPrimaryField = joinDialog?.primaryFieldId
			? fieldOptionsById.get(joinDialog.primaryFieldId) ?? null
			: null;
		const previousForeignField = joinDialog?.foreignFieldId
			? fieldOptionsById.get(joinDialog.foreignFieldId) ?? null
			: null;
		const previousPreview = buildJoinConditionPreview(previousPrimaryField, previousForeignField);
		setJoinDialog((previous) => {
			if (!previous) {
				return previous;
			}
			if (!option) {
				return {
					...previous,
					[key === 'primary' ? 'primaryFieldId' : 'foreignFieldId']: null,
					[key === 'primary' ? 'primaryTableId' : 'foreignTableId']: null
				};
			}
			return {
				...previous,
				[key === 'primary' ? 'primaryFieldId' : 'foreignFieldId']: option.id,
				[key === 'primary' ? 'primaryTableId' : 'foreignTableId']: option.tableId
			};
		});
		const nextPrimaryField = key === 'primary' ? option : previousPrimaryField;
		const nextForeignField = key === 'foreign' ? option : previousForeignField;
		const nextPreview = buildJoinConditionPreview(nextPrimaryField, nextForeignField);
		updateJoinCanvasForm((form) => {
			if (!form.joinCondition || form.joinCondition === previousPreview) {
				return { ...form, joinCondition: nextPreview };
			}
			return form;
		});
	};

	const handleDialogSubmit = useCallback(async () => {
		if (!joinDialog) {
			return;
		}

		const primaryFieldId = joinDialog.primaryFieldId;
		const foreignFieldId = joinDialog.foreignFieldId;

		if (!primaryFieldId || !foreignFieldId) {
			toast.showError('Please choose both the primary and foreign fields.');
			return;
		}

		if (primaryFieldId === foreignFieldId) {
			toast.showError('A relationship requires two distinct fields.');
			return;
		}

		const primaryFieldOption = fieldOptionsById.get(primaryFieldId);
		const foreignFieldOption = fieldOptionsById.get(foreignFieldId);
		if (!primaryFieldOption || !foreignFieldOption) {
			toast.showError('Selected fields are no longer available.');
			return;
		}

		if (joinDialog.mode === 'create' && !onCreateRelationship) {
			toast.showError('Relationship creation is not available.');
			return;
		}

		if (joinDialog.mode === 'edit' && (!onUpdateRelationship || !joinDialog.relationshipId)) {
			toast.showError('Relationship updates are not available.');
			return;
		}

		setDialogSubmitting(true);
		try {
			const payload = {
				primaryFieldId,
				foreignFieldId,
				joinType: joinDialog.joinType,
				notes: joinDialog.notes.trim() ? joinDialog.notes.trim() : undefined
			};

			let success = false;
			if (joinDialog.mode === 'create' && onCreateRelationship) {
				success = await onCreateRelationship(payload);
			} else if (joinDialog.mode === 'edit' && onUpdateRelationship && joinDialog.relationshipId) {
				success = await onUpdateRelationship(joinDialog.relationshipId, payload);
			}

			if (success) {
				toast.showSuccess(joinDialog.mode === 'create' ? 'Relationship created.' : 'Relationship updated.');
				const edgeForm = joinCanvasForm ?? createJoinCanvasForm(
					joinDialog.relationshipId ? edgeCanvasSettings.get(joinDialog.relationshipId) : undefined,
					joinDialog.relationshipId ? relationships.find((item) => item.id === joinDialog.relationshipId) : undefined,
					primaryFieldOption,
					foreignFieldOption
				);
				if (joinDialog.mode === 'edit' && joinDialog.relationshipId) {
					const relationshipRecord = relationships.find((item) => item.id === joinDialog.relationshipId);
					if (relationshipRecord) {
						const updatedRelationship: DataDefinitionRelationship = {
							...relationshipRecord,
							primaryFieldId,
							foreignFieldId,
							joinType: joinDialog.joinType
						};
						const nextState = assembleEdgeCanvasState({
							relationship: updatedRelationship,
							primaryField: primaryFieldOption,
							foreignField: foreignFieldOption,
							previous: edgeCanvasSettings.get(joinDialog.relationshipId),
							form: edgeForm
						});
						setEdgeCanvasSettings((current) => {
							const next = new Map(current);
							next.set(joinDialog.relationshipId!, nextState);
							return next;
						});
						pendingEdgeCanvasUpdateRef.current = null;
					} else {
						pendingEdgeCanvasUpdateRef.current = {
							primaryFieldId,
							foreignFieldId,
							joinType: joinDialog.joinType,
							form: edgeForm
						};
					}
				} else {
					pendingEdgeCanvasUpdateRef.current = {
						primaryFieldId,
						foreignFieldId,
						joinType: joinDialog.joinType,
						form: edgeForm
					};
				}
				setJoinDialog(null);
				setJoinCanvasForm(null);
			} else {
				toast.showError('The relationship could not be saved.');
			}
		} catch (error) {
			console.error(error);
			toast.showError('Failed to save the relationship.');
		} finally {
			setDialogSubmitting(false);
		}
	}, [
		edgeCanvasSettings,
		fieldOptionsById,
		joinCanvasForm,
		joinDialog,
		onCreateRelationship,
		onUpdateRelationship,
		relationships,
		toast
	]);

	const handleDeleteRelationship = useCallback(async () => {
		if (!joinDialog?.relationshipId) {
			return;
		}
		if (!onDeleteRelationship) {
			toast.showError('Relationship deletion is not available.');
			return;
		}
		setDialogSubmitting(true);
		try {
			const success = await onDeleteRelationship(joinDialog.relationshipId);
			if (success) {
				toast.showSuccess('Relationship deleted.');
				setJoinDialog(null);
				setJoinCanvasForm(null);
				setEdgeCanvasSettings((current) => {
					const next = new Map(current);
					next.delete(joinDialog.relationshipId!);
					return next;
				});
				pendingEdgeCanvasUpdateRef.current = null;
			} else {
				toast.showError('The relationship could not be deleted.');
			}
		} catch (error) {
			console.error(error);
			toast.showError('Failed to delete the relationship.');
		} finally {
			setDialogSubmitting(false);
		}
	}, [joinDialog, onDeleteRelationship, toast]);

	const primaryField = joinDialog?.primaryFieldId
		? fieldOptionsById.get(joinDialog.primaryFieldId) ?? null
		: null;
	const foreignField = joinDialog?.foreignFieldId
		? fieldOptionsById.get(joinDialog.foreignFieldId) ?? null
		: null;

	const isIncrementalMaterialization = nodeCanvasDialog?.form.materialization === 'incremental';

	return (
		<Paper
			variant="outlined"
			sx={{
				p: 2,
				borderRadius: 3,
				background: relationshipCardSurface.background,
				boxShadow: relationshipCardSurface.boxShadow,
				border: `1px solid ${relationshipBorderColor}`,
				borderLeft: `4px solid ${relationshipAccentColor}`,
				transition: theme.transitions.create(['border-color', 'box-shadow'], {
					duration: 220,
					easing: theme.transitions.easing.easeOut
				})
			}}
		>
			<Stack spacing={2}>
				<Stack
					direction="row"
					justifyContent="space-between"
					alignItems="center"
					spacing={1.5}
					sx={{
						pb: 1.5,
						borderBottom: `1px solid ${headerDividerColor}`
					}}
				>
					<Typography
						variant="h6"
						sx={{
							color: relationshipTitleColor,
							fontWeight: 700,
							letterSpacing: 0.3
						}}
					>
						Relationships
					</Typography>
					<IconButton
						size="small"
						onClick={() => setExpanded((previous) => !previous)}
						aria-label={expanded ? 'Collapse relationships' : 'Expand relationships'}
					>
						{expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
					</IconButton>
				</Stack>
				<Collapse in={expanded} timeout="auto" unmountOnExit>
					<Stack spacing={2}>
						<Box
							sx={{
								px: 2,
								py: 1.5,
								borderRadius: 2,
								border: `1px dashed ${relationshipCalloutBorder}`,
								backgroundColor: relationshipCalloutBackground
							}}
						>
							<Typography variant="body2" color="text.secondary">
								Drag a field onto another to propose a join or select an existing connector to make edits.
							</Typography>
						</Box>
						<Box
							sx={{
								borderRadius: 2,
								overflow: 'hidden',
								position: 'relative',
								minHeight: 320,
								height: { xs: '48vh', md: '56vh', lg: '60vh' }
							}}
						>
						<ReactFlow
																	nodes={nodes}
																	edges={edges}
																	nodeTypes={nodeTypes}
																	onNodesChange={onNodesChange}
																	onEdgesChange={onEdgesChange}
																	onInit={handleFlowInit}
																	onEdgeClick={handleEdgeClick}
																	fitView
																	panOnScroll={false}
																	selectionOnDrag={false}
																	nodesDraggable
																nodesConnectable={false}
																edgesFocusable={allowInteractions}
																	zoomOnScroll
																	minZoom={0.25}
																	maxZoom={1.5}
																	proOptions={{ hideAttribution: true }}
																	style={{ width: '100%', height: '100%', background: alpha(theme.palette.background.default, 0.86) }}
																>
					<Background color={alpha(theme.palette.divider, 0.6)} gap={32} />
					<Controls showInteractive={false} position="bottom-right" />
					<MiniMap
						nodeColor={() => alpha(theme.palette.primary.main, 0.8)}
						nodeStrokeWidth={3}
						style={{ background: alpha(theme.palette.background.paper, 0.9), borderRadius: 8 }}
						zoomable
						pannable
					/>
				</ReactFlow>
					</Box>
				</Stack>
			</Collapse>

			<Dialog open={Boolean(joinDialog)} onClose={handleDialogClose} fullWidth maxWidth="sm">
				<DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
					{joinDialog?.mode === 'edit' ? <EditOutlinedIcon fontSize="small" /> : <AddLinkIcon fontSize="small" />}
					{joinDialog?.mode === 'edit' ? 'Edit Relationship' : 'Create Relationship'}
				</DialogTitle>
				<DialogContent dividers>
					<Stack spacing={2}>
						<Stack spacing={1}>
							<Typography variant="subtitle2">Primary field</Typography>
							<Autocomplete
								options={fieldOptions}
								value={primaryField}
								onChange={(_event, option) => handleFieldSelection('primary', option)}
								groupBy={(option) => option.tableLabel}
								getOptionLabel={(option) => `${option.tableLabel}.${option.fieldLabel}`}
								disableClearable={false}
								renderInput={(params) => <TextField {...params} label="Primary field" placeholder="Select primary field" />}
								renderOption={(props, option) => (
									<li {...props} key={option.id}>
										<Stack spacing={0.25}>
											<Typography variant="body2" sx={{ fontWeight: 600 }}>
												{option.fieldLabel}
											</Typography>
											<Typography variant="caption" color="text.secondary">
												{option.tableSubtitle}
											</Typography>
										</Stack>
									</li>
								)}
								isOptionEqualToValue={(option, value) => option.id === value.id}
							/>
						</Stack>
						<Stack spacing={1}>
							<Typography variant="subtitle2">Foreign field</Typography>
							<Autocomplete
								options={fieldOptions}
								value={foreignField}
								onChange={(_event, option) => handleFieldSelection('foreign', option)}
								groupBy={(option) => option.tableLabel}
								getOptionLabel={(option) => `${option.tableLabel}.${option.fieldLabel}`}
								disableClearable={false}
								renderInput={(params) => <TextField {...params} label="Foreign field" placeholder="Select foreign field" />}
								renderOption={(props, option) => (
									<li {...props} key={option.id}>
										<Stack spacing={0.25}>
											<Typography variant="body2" sx={{ fontWeight: 600 }}>
												{option.fieldLabel}
											</Typography>
											<Typography variant="caption" color="text.secondary">
												{option.tableSubtitle}
											</Typography>
										</Stack>
									</li>
								)}
								isOptionEqualToValue={(option, value) => option.id === value.id}
							/>
						</Stack>
						<Stack spacing={1}>
							<Typography variant="subtitle2">Join type</Typography>
							<ToggleButtonGroup
								value={joinDialog?.joinType ?? 'inner'}
								exclusive
								onChange={handleJoinTypeChange}
							>
								{(Object.keys(joinTypeLabels) as Array<DataDefinitionJoinType>).map((joinType) => (
									<ToggleButton key={joinType} value={joinType} sx={{ textTransform: 'none', px: 1.5 }}>
										{joinTypeLabels[joinType]}
									</ToggleButton>
								))}
							</ToggleButtonGroup>
						</Stack>
						<TextField
							label="Notes"
							multiline
							minRows={3}
							value={joinDialog?.notes ?? ''}
							onChange={handleNotesChange}
							placeholder="Optional context for this relationship"
						/>
						<Divider sx={{ my: 1 }} />
						<Stack spacing={1.5}>
							<Typography variant="subtitle2">dbt Canvas settings</Typography>
							<FormControl fullWidth size="small">
								<InputLabel id="join-edge-type-label">Dependency type</InputLabel>
								<Select
									labelId="join-edge-type-label"
									label="Dependency type"
									value={joinCanvasForm?.edgeType ?? edgeTypeOptions[0]}
									onChange={handleJoinCanvasEdgeTypeChange}
								>
									{edgeTypeOptions.map((option) => (
										<MenuItem key={option} value={option}>
											{option}
										</MenuItem>
									))}
								</Select>
							</FormControl>
							<TextField
								label="Join condition"
								placeholder="foreign_table.column = primary_table.column"
								value={joinCanvasForm?.joinCondition ?? ''}
								onChange={handleJoinCanvasConditionChange}
								size="small"
								multiline
								minRows={2}
							/>
							<TextField
								label="Dependency notes"
								placeholder="Optional description for dbt Canvas"
								value={joinCanvasForm?.edgeDescription ?? ''}
								onChange={handleJoinCanvasDescriptionChange}
								size="small"
								multiline
								minRows={2}
							/>
						</Stack>
						{primaryField && foreignField ? (
							<Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
								<Typography variant="caption" color="text.secondary">
									Preview
								</Typography>
								<Chip
									label={`${qualifyField(primaryField)} ${joinTypeLabels[joinDialog?.joinType ?? 'inner']} ${qualifyField(foreignField)}`}
									variant="outlined"
									sx={{ fontSize: 12, maxWidth: '100%' }}
								/>
							</Stack>
						) : null}
					</Stack>
				</DialogContent>
				<DialogActions sx={{ px: 3, py: 2 }}>
					<Button onClick={handleDialogClose} disabled={dialogSubmitting}>
						Cancel
					</Button>
					{joinDialog?.mode === 'edit' ? (
						<Button
							color="error"
							startIcon={
								dialogSubmitting ? <CircularProgress size={16} color="inherit" /> : <DeleteOutlineIcon />
							}
							onClick={handleDeleteRelationship}
							disabled={dialogSubmitting}
						>
							Delete
						</Button>
					) : null}
					<Button
						variant="contained"
						onClick={handleDialogSubmit}
						disabled={dialogSubmitting}
						startIcon={
							dialogSubmitting ? (
								<CircularProgress size={16} color="inherit" />
							) : joinDialog?.mode === 'edit' ? (
								<EditOutlinedIcon />
							) : (
								<AddLinkIcon />
							)
						}
					>
						{joinDialog?.mode === 'edit' ? 'Save changes' : 'Create relationship'}
					</Button>
				</DialogActions>
			</Dialog>
				<Dialog open={Boolean(nodeCanvasDialog)} onClose={handleNodeCanvasClose} fullWidth maxWidth="sm">
					<DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
						<SettingsOutlinedIcon fontSize="small" />
						{nodeCanvasDialog ? `dbt settings • ${buildTableLabel(nodeCanvasDialog.table)}` : 'dbt settings'}
					</DialogTitle>
					<DialogContent dividers>
						<Stack spacing={2}>
							{nodeCanvasDialogError ? (
								<FormHelperText error>{nodeCanvasDialogError}</FormHelperText>
							) : null}
							<Typography variant="body2" color="text.secondary">
								{nodeCanvasDialog?.columns.length ?? 0} column{(nodeCanvasDialog?.columns.length ?? 0) === 1 ? '' : 's'} mapped
							</Typography>
							<FormControl fullWidth size="small">
								<InputLabel id="node-resource-type-label">Resource type</InputLabel>
								<Select
									labelId="node-resource-type-label"
									label="Resource type"
									value={nodeCanvasDialog?.form.resourceType ?? resourceTypeOptions[0]}
									onChange={handleNodeCanvasResourceTypeChange}
								>
									{resourceTypeOptions.map((option) => (
										<MenuItem key={option} value={option}>
											{option}
										</MenuItem>
									))}
								</Select>
							</FormControl>
							<FormControl fullWidth size="small">
								<InputLabel id="node-layer-label">Layer</InputLabel>
								<Select
									labelId="node-layer-label"
									label="Layer"
									value={nodeCanvasDialog?.form.layer ?? ''}
									onChange={handleNodeCanvasLayerChange}
									displayEmpty
								>
									<MenuItem value="">
										<em>Unspecified</em>
									</MenuItem>
									{layerOptions.map((option) => (
										<MenuItem key={option} value={option}>
											{option}
										</MenuItem>
									))}
								</Select>
							</FormControl>
							<FormControl fullWidth size="small">
								<InputLabel id="node-materialization-label">Materialization</InputLabel>
								<Select
									labelId="node-materialization-label"
									label="Materialization"
									value={nodeCanvasDialog?.form.materialization ?? ''}
									onChange={handleNodeCanvasMaterializationChange}
									displayEmpty
								>
									<MenuItem value="">
										<em>Unspecified</em>
									</MenuItem>
									{materializationOptions.map((option) => (
										<MenuItem key={option} value={option}>
											{option}
										</MenuItem>
									))}
								</Select>
							</FormControl>
							<FormControl fullWidth size="small">
								<InputLabel id="node-access-label">Access</InputLabel>
								<Select
									labelId="node-access-label"
									label="Access"
									value={nodeCanvasDialog?.form.access ?? accessOptions[0]}
									onChange={handleNodeCanvasAccessChange}
								>
									{accessOptions.map((option) => (
										<MenuItem key={option} value={option}>
											{option}
										</MenuItem>
									))}
								</Select>
							</FormControl>
							<TextField
								label="Owner"
								placeholder="Team or person"
								value={nodeCanvasDialog?.form.owner ?? ''}
								onChange={handleNodeCanvasOwnerChange}
								size="small"
							/>
							<TextField
								label="Description"
								placeholder="dbt model description"
								value={nodeCanvasDialog?.form.description ?? ''}
								onChange={handleNodeCanvasDescriptionChange}
								size="small"
								multiline
								minRows={3}
							/>
							<Divider flexItem sx={{ my: 1 }} />
							<Stack spacing={1}>
								<Typography variant="subtitle2">Pre-hook SQL (runs before model)</Typography>
								<TextField
									placeholder="CALL pre_hook_procedure();"
									value={nodeCanvasDialog?.form.preHooks ?? ''}
									onChange={handleNodeCanvasPreHooksChange}
									size="small"
									multiline
									minRows={2}
								/>
							</Stack>
							<Stack spacing={1}>
								<Typography variant="subtitle2">Post-hook SQL (runs after model)</Typography>
								<TextField
									placeholder="CALL post_hook_procedure();"
									value={nodeCanvasDialog?.form.postHooks ?? ''}
									onChange={handleNodeCanvasPostHooksChange}
									size="small"
									multiline
									minRows={2}
								/>
							</Stack>
							<Divider flexItem sx={{ my: 1 }} />
							<Stack spacing={1.5}>
								<Stack direction="row" spacing={1} alignItems="center">
									<Typography variant="subtitle2">Incremental settings</Typography>
									{!isIncrementalMaterialization ? (
										<Typography variant="caption" color="text.secondary">
											Enable the incremental materialization to apply these options.
										</Typography>
									) : null}
								</Stack>
								<TextField
									label="Incremental strategy"
									placeholder="merge"
									value={nodeCanvasDialog?.form.incrementalStrategy ?? ''}
									onChange={handleNodeCanvasIncrementalStrategyChange}
									size="small"
									disabled={!isIncrementalMaterialization}
								/>
								<TextField
									label="Unique key (comma separated)"
									placeholder="order_id, updated_at"
									value={nodeCanvasDialog?.form.incrementalUniqueKey ?? ''}
									onChange={handleNodeCanvasIncrementalUniqueKeyChange}
									size="small"
									disabled={!isIncrementalMaterialization}
								/>
								<TextField
									label="Partition by (comma separated)"
									placeholder="DATE(updated_at)"
									value={nodeCanvasDialog?.form.incrementalPartitionBy ?? ''}
									onChange={handleNodeCanvasIncrementalPartitionByChange}
									size="small"
									disabled={!isIncrementalMaterialization}
								/>
								<TextField
									label="Cluster by (comma separated)"
									placeholder="country, state"
									value={nodeCanvasDialog?.form.incrementalClusterBy ?? ''}
									onChange={handleNodeCanvasIncrementalClusterByChange}
									size="small"
									disabled={!isIncrementalMaterialization}
								/>
							</Stack>
							<Stack spacing={1}>
								<Typography variant="subtitle2">Tags</Typography>
								<Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
									{nodeCanvasDialog?.form.tags.map((tag) => (
										<Chip key={tag} label={tag} onDelete={() => handleNodeCanvasRemoveTag(tag)} size="small" />
									))}
									<TextField
										label="Add tag"
										size="small"
										value={nodeCanvasDialog?.form.tagsInput ?? ''}
										onChange={handleNodeCanvasTagsInputChange}
										onKeyDown={handleNodeCanvasTagsKeyDown}
										onBlur={handleNodeCanvasTagsBlur}
										placeholder="Press Enter to add"
										InputProps={{ sx: { minWidth: 160 } }}
									/>
								</Stack>
							</Stack>
						</Stack>
					</DialogContent>
					<DialogActions sx={{ px: 3, py: 2 }}>
						<Button onClick={handleNodeCanvasClose}>Cancel</Button>
						<Button variant="contained" onClick={handleNodeCanvasSubmit}>
							Save settings
						</Button>
					</DialogActions>
				</Dialog>
		</Stack>
	</Paper>
	);
});

BuilderContent.displayName = 'DataDefinitionRelationshipBuilderContent';

const DataDefinitionRelationshipBuilder = forwardRef<
	DataDefinitionRelationshipBuilderHandle,
	DataDefinitionRelationshipBuilderProps
>((props, ref) => (
	<ReactFlowProvider>
		<BuilderContent ref={ref} {...props} />
	</ReactFlowProvider>
));

DataDefinitionRelationshipBuilder.displayName = 'DataDefinitionRelationshipBuilder';

export default DataDefinitionRelationshipBuilder;
