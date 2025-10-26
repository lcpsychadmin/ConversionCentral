import { ChangeEvent, MouseEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
	IconButton,
	Paper,
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

import ReportTableNode, { ReportTableNodeData } from '@components/reporting/ReportTableNode';
import { useToast } from '@hooks/useToast';
import type {
	DataDefinitionField,
	DataDefinitionJoinType,
	DataDefinitionRelationship,
	DataDefinitionRelationshipInput,
	DataDefinitionRelationshipUpdateInput,
	DataDefinitionTable
} from '../../types/data';

const nodeTypes = { dataDefinitionTable: ReportTableNode } as const;

const defaultNodeWidth = 260;
const defaultNodeHeight = 320;
const horizontalGap = 360;
const verticalGap = 360;

const joinTypeLabels: Record<DataDefinitionJoinType, string> = {
	inner: 'Inner Join',
	left: 'Left Join',
	right: 'Right Join'
};

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

const BuilderContent = ({
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
}: DataDefinitionRelationshipBuilderProps) => {
	const theme = useTheme();
	const toast = useToast();
	const [nodes, setNodes, onNodesChange] = useNodesState<ReportTableNodeData>([]);
	const [edges, setEdges, onEdgesChange] = useEdgesState<RelationshipEdgeData>([]);
	const [joinDialog, setJoinDialog] = useState<JoinDialogState | null>(null);
	const [dialogSubmitting, setDialogSubmitting] = useState(false);
	const [expanded, setExpanded] = useState(false);
	const draggedFieldRef = useRef<FieldDragPayload | null>(null);
	const reactFlowInstanceRef = useRef<ReactFlowInstance<ReportTableNodeData> | null>(null);
	const initialRelationshipHandledRef = useRef(false);

	const allowInteractions = canEdit && !busy && !dialogSubmitting;

	const fieldOptions = useMemo<FieldOption[]>(() => {
		const options: FieldOption[] = [];
		tables.forEach((table) => {
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
	}, [tables]);

	const fieldOptionsById = useMemo(() => {
		const map = new Map<string, FieldOption>();
		fieldOptions.forEach((option) => map.set(option.id, option));
		return map;
	}, [fieldOptions]);

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
		[canEdit, fieldOptionsById]
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

	useEffect(() => {
		const columnCount = Math.max(1, Math.ceil(Math.sqrt(Math.max(tables.length, 1))));

		const nextNodes: Node<ReportTableNodeData>[] = tables.map((table, index) => {
			const column = index % columnCount;
			const row = Math.floor(index / columnCount);
			const position = {
				x: column * horizontalGap,
				y: row * verticalGap
			};
			const selectedFieldIds = table.fields
				.filter((definitionField) => activeFieldIds.has(definitionField.id))
				.map((definitionField) => definitionField.id);

			return {
				id: table.id,
				type: 'dataDefinitionTable',
				position,
				data: {
					tableId: table.id,
					label: buildTableLabel(table),
					subtitle: buildTableSubtitle(table),
					fields: table.fields
						.filter((definitionField) => Boolean(definitionField.field))
						.map((definitionField) => ({
							id: definitionField.id,
							name: definitionField.field.name,
							type: definitionField.field.fieldType ?? null,
							description: definitionField.field.description ?? definitionField.notes ?? null
						})),
					allowSelection: false,
					onFieldDragStart: allowInteractions ? handleFieldDragStart : undefined,
					onFieldDragEnd: allowInteractions ? handleFieldDragEnd : undefined,
					onFieldJoin: allowInteractions ? handleFieldJoin : undefined,
					onRemoveTable: undefined,
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
		reactFlowInstanceRef,
		setNodes,
		tables
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

			nextEdges.push({
				id: relationship.id,
				source: foreignField.tableId,
				sourceHandle: `source:${foreignField.id}`,
				target: primaryField.tableId,
				targetHandle: `target:${primaryField.id}`,
				type: 'straight',
				style: {
					stroke: color,
					strokeWidth: 2
				},
				markerEnd: {
					type: MarkerType.ArrowClosed,
					color
				},
				data: {
					relationshipId: relationship.id,
					primaryFieldId: relationship.primaryFieldId,
					foreignFieldId: relationship.foreignFieldId,
					joinType: relationship.joinType,
					notes: relationship.notes ?? null
				}
			});
		});

		setEdges(nextEdges);
	}, [fieldOptionsById, joinColors, relationships, setEdges]);

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
	};

	const handleJoinTypeChange = (_event: MouseEvent<HTMLElement>, value: DataDefinitionJoinType | null) => {
		if (!value) {
			return;
		}
		setJoinDialog((previous) => (previous ? { ...previous, joinType: value } : previous));
	};

	const handleNotesChange = (event: ChangeEvent<HTMLInputElement>) => {
		const { value } = event.target;
		setJoinDialog((previous) => (previous ? { ...previous, notes: value } : previous));
	};

	const handleFieldSelection = (key: 'primary' | 'foreign', option: FieldOption | null) => {
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
				setJoinDialog(null);
			} else {
				toast.showError('The relationship could not be saved.');
			}
		} catch (error) {
			console.error(error);
			toast.showError('Failed to save the relationship.');
		} finally {
			setDialogSubmitting(false);
		}
	}, [joinDialog, onCreateRelationship, onUpdateRelationship, toast]);

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

	return (
		<Paper
			variant="outlined"
			sx={{
				p: 2,
				bgcolor: theme.palette.common.white,
				borderColor: alpha(theme.palette.info.main, 0.25),
				borderLeft: `4px solid ${theme.palette.info.main}`,
				boxShadow: `0 4px 12px ${alpha(theme.palette.common.black, 0.05)}`
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
						borderBottom: `2px solid ${alpha(theme.palette.warning.main, 0.15)}`
					}}
				>
					<Typography
						variant="h6"
						sx={{
							color: theme.palette.primary.main,
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
								border: `1px dashed ${alpha(theme.palette.primary.main, 0.35)}`,
								backgroundColor: alpha(theme.palette.primary.light, 0.06)
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
						panOnScroll
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
		</Stack>
	</Paper>
	);
	};

const DataDefinitionRelationshipBuilder = (props: DataDefinitionRelationshipBuilderProps) => (
	<ReactFlowProvider>
		<BuilderContent {...props} />
	</ReactFlowProvider>
);

export default DataDefinitionRelationshipBuilder;
