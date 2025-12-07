import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
	Alert,
	Box,
	Button,
	Checkbox,
	Chip,
	CircularProgress,
	Dialog,
	DialogActions,
	DialogContent,
	DialogTitle,
	FormControlLabel,
	InputAdornment,
	List,
	ListItem,
	ListItemIcon,
	ListItemText,
	Stack,
	TextField,
	Tooltip,
	Typography
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import { SimpleTreeView } from '@mui/x-tree-view/SimpleTreeView';
import { TreeItem } from '@mui/x-tree-view/TreeItem';

import {
	AvailableSourceTable,
	fetchSourceTableColumns,
	SourceTableColumn
} from '../../services/dataDefinitionService';
import { sourceKeyToSchemaKey, toSchemaTableKey, toSourceTableKey } from './sourceTableUtils';

export interface SelectedSourceTablePayload {
	catalogName: string | null;
	schemaName: string;
	tableName: string;
	tableType?: string | null;
	columnCount?: number | null;
	estimatedRows?: number | null;
	selectedColumns: SourceTableColumn[];
}

interface ColumnSelectionState {
	columns: SourceTableColumn[];
	selected: string[];
	loading: boolean;
	error: string | null;
	fetched: boolean;
}

interface CatalogTreeNode {
	id: string;
	key: string;
	label: string;
	children: SchemaTreeNode[];
	tableCount: number;
}

interface SchemaTreeNode {
	id: string;
	key: string;
	label: string;
	children: TableTreeNode[];
	tableCount: number;
}

interface TableTreeNode {
	id: string;
	key: string;
	label: string;
	catalogLabel: string;
	schemaLabel: string;
	table: AvailableSourceTable;
}

interface AddExistingSourceTableDialogProps {
	dataObjectId?: string;
	open: boolean;
	tables: AvailableSourceTable[];
	loading?: boolean;
	error?: string | null;
	excludedTableKeys?: string[];
	onClose: () => void;
	onSubmit: (payload: SelectedSourceTablePayload[]) => Promise<void> | void;
}

const DEFAULT_CATALOG_KEY = '__default_catalog__';
const DEFAULT_CATALOG_LABEL = 'Default Catalog';
const DEFAULT_SCHEMA_KEY = '__default_schema__';
const DEFAULT_SCHEMA_LABEL = '(Default Schema)';

const AddExistingSourceTableDialog = ({
	dataObjectId,
	open,
	tables,
	loading = false,
	error,
	excludedTableKeys = [],
	onClose,
	onSubmit
}: AddExistingSourceTableDialogProps) => {
	const [searchTerm, setSearchTerm] = useState('');
	const [selectedTableKeys, setSelectedTableKeys] = useState<string[]>([]);
	const [expandedNodes, setExpandedNodes] = useState<string[]>([]);
	const [activeItemId, setActiveItemId] = useState<string | null>(null);
	const [activeTableKey, setActiveTableKey] = useState<string | null>(null);
	const [columnState, setColumnState] = useState<Record<string, ColumnSelectionState>>({});
	const columnStateRef = useRef<Record<string, ColumnSelectionState>>({});
	const [localError, setLocalError] = useState<string | null>(null);
	const [pendingSelection, setPendingSelection] = useState<string | null>(null);
	const [submitting, setSubmitting] = useState(false);

	useEffect(() => {
		columnStateRef.current = columnState;
	}, [columnState]);

	const excludedSchemaKeySet = useMemo(() => new Set(excludedTableKeys), [excludedTableKeys]);
	const selectedKeySet = useMemo(() => new Set(selectedTableKeys), [selectedTableKeys]);

	const isSchemaKeyExcluded = useCallback(
		(key: string | null | undefined) => {
			const normalized = sourceKeyToSchemaKey(key);
			return normalized ? excludedSchemaKeySet.has(normalized) : false;
		},
		[excludedSchemaKeySet]
	);

	const isTableExcluded = useCallback(
		(table: AvailableSourceTable) => {
			const normalized = toSchemaTableKey(table.schemaName, table.tableName);
			return normalized ? excludedSchemaKeySet.has(normalized) : false;
		},
		[excludedSchemaKeySet]
	);

	const tableMap = useMemo(() => {
		const map = new Map<string, AvailableSourceTable>();
		tables.forEach((table) => {
			const key = toSourceTableKey(table.catalogName, table.schemaName, table.tableName);
			if (key) {
				map.set(key, table);
			}
		});
		return map;
	}, [tables]);

	// Build a catalog -> schema -> table hierarchy for the tree view.
	const catalogHierarchy = useMemo<CatalogTreeNode[]>(() => {
		const hierarchy = new Map<
			string,
			{ node: CatalogTreeNode; schemas: Map<string, SchemaTreeNode> }
		>();
		const term = searchTerm.trim().toLowerCase();

		const matchesSearch = (table: AvailableSourceTable) => {
			if (!term) {
				return true;
			}
			const catalog = (table.catalogName ?? '').toLowerCase();
			const schema = (table.schemaName ?? '').toLowerCase();
			const tableName = (table.tableName ?? '').toLowerCase();
			return catalog.includes(term) || schema.includes(term) || tableName.includes(term);
		};

		tables.forEach((table) => {
			if (!table.tableName || !matchesSearch(table)) {
				return;
			}

			const key = toSourceTableKey(table.catalogName, table.schemaName, table.tableName);
			if (!key) {
				return;
			}

			const catalogKey = (table.catalogName ?? '').trim().toLowerCase() || DEFAULT_CATALOG_KEY;
			const catalogLabel = table.catalogName?.trim() || DEFAULT_CATALOG_LABEL;
			let catalogEntry = hierarchy.get(catalogKey);
			if (!catalogEntry) {
				const catalogNode: CatalogTreeNode = {
					id: `catalog::${catalogKey}`,
					key: catalogKey,
					label: catalogLabel,
					children: [],
					tableCount: 0
				};
				catalogEntry = { node: catalogNode, schemas: new Map<string, SchemaTreeNode>() };
				hierarchy.set(catalogKey, catalogEntry);
			}

			const schemaKey = (table.schemaName ?? '').trim().toLowerCase() || DEFAULT_SCHEMA_KEY;
			const schemaLabel = table.schemaName?.trim() || DEFAULT_SCHEMA_LABEL;
			let schemaNode = catalogEntry.schemas.get(schemaKey);
			if (!schemaNode) {
				schemaNode = {
					id: `schema::${catalogKey}::${schemaKey}`,
					key: schemaKey,
					label: schemaLabel,
					children: [],
					tableCount: 0
				};
				catalogEntry.schemas.set(schemaKey, schemaNode);
				catalogEntry.node.children.push(schemaNode);
			}

			const tableNode: TableTreeNode = {
				id: `table::${key}`,
				key,
				label: table.tableName,
				catalogLabel,
				schemaLabel,
				table
			};

			schemaNode.children.push(tableNode);
			schemaNode.tableCount += 1;
			catalogEntry.node.tableCount += 1;
		});

		const sortedCatalogs = Array.from(hierarchy.values())
			.map(({ node, schemas }) => {
				const sortedSchemas = Array.from(schemas.values())
					.map((schema) => ({
						...schema,
						children: schema.children
							.slice()
							.sort((a, b) => a.label.localeCompare(b.label))
					}))
					.filter((schema) => schema.children.length > 0)
					.sort((a, b) => a.label.localeCompare(b.label));

				const totalTables = sortedSchemas.reduce(
					(sum, schema) => sum + schema.children.length,
					0
				);

				return {
					...node,
					children: sortedSchemas,
					tableCount: totalTables
				};
			})
			.filter((catalog) => catalog.children.length > 0)
			.sort((a, b) => a.label.localeCompare(b.label));

		return sortedCatalogs;
	}, [tables, searchTerm]);

	useEffect(() => {
		if (!open) {
			return;
		}
		setSearchTerm('');
		setSelectedTableKeys([]);
		setLocalError(null);
		setPendingSelection(null);
		setSubmitting(false);
		setExpandedNodes([]);
		setActiveItemId(null);
		setActiveTableKey(null);
		setColumnState({});
		columnStateRef.current = {};
	}, [open]);

	useEffect(() => {
		if (!open) {
			return;
		}
		setSelectedTableKeys((prev) => {
			const filtered = prev.filter(
				(key) => tableMap.has(key) && !isSchemaKeyExcluded(key)
			);
			return filtered.length === prev.length ? prev : filtered;
		});
		}, [open, tableMap, isSchemaKeyExcluded]);

	useEffect(() => {
		setColumnState((prev) => {
			const entries = Object.entries(prev).filter(([key]) => tableMap.has(key));
			if (entries.length === Object.keys(prev).length) {
				return prev;
			}
			const next: Record<string, ColumnSelectionState> = {};
			entries.forEach(([key, state]) => {
				next[key] = state;
			});
			columnStateRef.current = next;
			return next;
		});
	}, [tableMap]);

	useEffect(() => {
		if (!activeTableKey) {
			return;
		}
		if (!tableMap.has(activeTableKey)) {
			setActiveTableKey(null);
			setActiveItemId(null);
		}
	}, [activeTableKey, tableMap]);

	const waitForColumnLoad = useCallback(async (tableKey: string) => {
		const maxAttempts = 20;
		for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
			const current = columnStateRef.current[tableKey];
			if (!current?.loading) {
				return current ?? null;
			}
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		return columnStateRef.current[tableKey] ?? null;
	}, []);

	const ensureColumnsLoaded = useCallback(
		async (tableKey: string): Promise<ColumnSelectionState | null> => {
			const existing = columnStateRef.current[tableKey];
			if (existing?.loading) {
				return waitForColumnLoad(tableKey);
			}
			if (existing?.fetched && !existing.error) {
				return existing;
			}

			const metadata = tableMap.get(tableKey);
			if (!metadata || !dataObjectId) {
				const errorMessage = !dataObjectId
					? 'A data object is required to browse columns.'
					: 'Table metadata was not found.';
				const nextState: ColumnSelectionState = {
					columns: [],
					selected: [],
					loading: false,
					error: errorMessage,
					fetched: false
				};
				setColumnState((prev) => {
					const next = { ...prev, [tableKey]: nextState };
					columnStateRef.current = next;
					return next;
				});
				return nextState;
			}

			setColumnState((prev) => {
				const current = prev[tableKey];
				if (current?.loading) {
					columnStateRef.current = prev;
					return prev;
				}
				const nextState: ColumnSelectionState = {
					columns: current?.columns ?? [],
					selected: current?.selected ?? [],
					loading: true,
					error: null,
					fetched: current?.fetched ?? false
				};
				const next = { ...prev, [tableKey]: nextState };
				columnStateRef.current = next;
				return next;
			});

			try {
				const columns = await fetchSourceTableColumns(
					dataObjectId,
					metadata.schemaName,
					metadata.tableName
				);

				setColumnState((prev) => {
					const previous = prev[tableKey];
					const selected =
						previous?.selected?.length ?? 0
							? previous!.selected
							: columns.map((column) => column.name);
					const nextState: ColumnSelectionState = {
						columns,
						selected,
						loading: false,
						error: null,
						fetched: true
					};
					const next = { ...prev, [tableKey]: nextState };
					columnStateRef.current = next;
					return next;
				});

				return columnStateRef.current[tableKey] ?? null;
			} catch (fetchError) {
				const message =
					fetchError instanceof Error ? fetchError.message : 'Failed to load columns.';
				setColumnState((prev) => {
					const nextState: ColumnSelectionState = {
						columns: [],
						selected: [],
						loading: false,
						error: message,
						fetched: false
					};
					const next = { ...prev, [tableKey]: nextState };
					columnStateRef.current = next;
					return next;
				});
				return columnStateRef.current[tableKey] ?? null;
			}
		},
		[dataObjectId, tableMap, waitForColumnLoad]
	);

	const handleToggleTable = useCallback(
		async (node: TableTreeNode, shouldSelect: boolean) => {
			if (shouldSelect) {
				if (isSchemaKeyExcluded(node.key) || isTableExcluded(node.table)) {
					return;
				}
				setPendingSelection(node.key);
				setLocalError(null);
				const result = await ensureColumnsLoaded(node.key);
				setPendingSelection(null);
				if (!result || result.error) {
					setLocalError(result?.error ?? 'Unable to load columns for the selected table.');
					return;
				}
				setSelectedTableKeys((prev) =>
					prev.includes(node.key) ? prev : [...prev, node.key]
				);
				setActiveItemId(node.id);
				setActiveTableKey(node.key);
			} else {
				setSelectedTableKeys((prev) => prev.filter((key) => key !== node.key));
			}
		},
		[ensureColumnsLoaded, isSchemaKeyExcluded, isTableExcluded]
	);

	const handleColumnToggle = useCallback((tableKey: string, columnName: string) => {
		setColumnState((prev) => {
			const state = prev[tableKey];
			if (!state) {
				return prev;
			}
			const selectedSet = new Set(state.selected);
			if (selectedSet.has(columnName)) {
				selectedSet.delete(columnName);
			} else {
				selectedSet.add(columnName);
			}
			const nextState: ColumnSelectionState = {
				...state,
				selected: Array.from(selectedSet)
			};
			const next = { ...prev, [tableKey]: nextState };
			columnStateRef.current = next;
			return next;
		});
	}, []);

	const handleToggleAllColumns = useCallback((tableKey: string) => {
		setColumnState((prev) => {
			const state = prev[tableKey];
			if (!state) {
				return prev;
			}
			const allNames = state.columns.map((column) => column.name);
			const nextSelected =
				state.selected.length === allNames.length ? [] : allNames;
			const nextState: ColumnSelectionState = {
				...state,
				selected: nextSelected
			};
			const next = { ...prev, [tableKey]: nextState };
			columnStateRef.current = next;
			return next;
		});
	}, []);

	const handleCloseDialog = () => {
		if (loading || submitting || pendingSelection || anyColumnsLoading) {
			return;
		}
		onClose();
	};

	const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
		setSearchTerm(event.target.value);
	};

	const handleTreeSelectionChange = useCallback(
		async (itemId: string | null) => {
			setActiveItemId(itemId);
			if (itemId && itemId.startsWith('table::')) {
				const key = itemId.slice('table::'.length);
				setActiveTableKey(key);
				await ensureColumnsLoaded(key);
			} else {
				setActiveTableKey(null);
			}
		},
		[ensureColumnsLoaded]
	);

	const handleSubmit = async () => {
		setLocalError(null);
		if (!selectedTableKeys.length) {
			setLocalError('Select at least one table to add.');
			return;
		}

		setSubmitting(true);
		try {
			const payload: SelectedSourceTablePayload[] = [];
			for (const key of selectedTableKeys) {
				const metadata = tableMap.get(key);
				if (!metadata) {
					continue;
				}
				const state = await ensureColumnsLoaded(key);
				if (!state || state.error) {
					throw new Error(state?.error ?? 'Unable to load columns for the selection.');
				}
				const selectedNames = new Set(state.selected);
				if (!selectedNames.size) {
					const schemaLabel = metadata.schemaName ?? DEFAULT_SCHEMA_LABEL;
					throw new Error(
						`Select at least one column for ${schemaLabel}.${metadata.tableName}.`
					);
				}
				const selectedColumns = state.columns.filter((column) =>
					selectedNames.has(column.name)
				);
				payload.push({
					catalogName: metadata.catalogName ?? null,
					schemaName: metadata.schemaName,
					tableName: metadata.tableName,
					tableType: metadata.tableType ?? null,
					columnCount: metadata.columnCount ?? null,
					estimatedRows: metadata.estimatedRows ?? null,
					selectedColumns
				});
			}

			if (!payload.length) {
				setLocalError('Select at least one table to add.');
				return;
			}

			await onSubmit(payload);
			setSelectedTableKeys([]);
		} catch (submitError) {
			const message =
				submitError instanceof Error
					? submitError.message
					: 'Unable to add selected tables.';
			setLocalError(message);
			return;
		} finally {
			setSubmitting(false);
		}
	};

	const anyColumnsLoading = useMemo(
		() => Object.values(columnState).some((state) => state.loading),
		[columnState]
	);

	const activeColumnState = activeTableKey ? columnState[activeTableKey] : undefined;
	const activeMetadata = activeTableKey ? tableMap.get(activeTableKey) ?? null : null;
	const activeSelectedSet = useMemo(
		() => new Set(activeColumnState?.selected ?? []),
		[activeColumnState]
	);
	const allColumnsCount = activeColumnState?.columns.length ?? 0;
	const selectedColumnsCount = activeColumnState?.selected.length ?? 0;

	const renderTableNode = (node: TableTreeNode) => {
		const isSelected = selectedKeySet.has(node.key);
		const isExcluded = isSchemaKeyExcluded(node.key) || isTableExcluded(node.table);
		const selectionInProgress = pendingSelection === node.key;
		const disableCheckbox =
			loading ||
			submitting ||
			Boolean(pendingSelection && pendingSelection !== node.key) ||
			isExcluded;

		const detailChips = [];
		if (isSelected) {
			detailChips.push(
				<Chip key="selected" label="Selected" size="small" color="primary" variant="outlined" />
			);
		}
		if (isExcluded) {
			detailChips.push(
				<Chip key="excluded" label="Added" size="small" variant="outlined" />
			);
		}
		if (typeof node.table.columnCount === 'number') {
			detailChips.push(
				<Chip
					key="columns"
					label={`${node.table.columnCount} cols`}
					size="small"
					variant="outlined"
				/>
			);
		}
		if (typeof node.table.estimatedRows === 'number') {
			detailChips.push(
				<Chip
					key="rows"
					label={`${new Intl.NumberFormat().format(node.table.estimatedRows)} rows`}
					size="small"
					variant="outlined"
				/>
			);
		}

		return (
			<TreeItem
				key={node.id}
				itemId={node.id}
				label={
					<Stack
						direction="row"
						spacing={1}
						alignItems="center"
						sx={{ py: 0.5, pr: 0.5 }}
						onClick={(event) => {
							event.stopPropagation();
							setActiveItemId(node.id);
							setActiveTableKey(node.key);
							void ensureColumnsLoaded(node.key);
						}}
					>
						<Tooltip
							title={
								isExcluded ? 'This table is already part of the data definition.' : ''
							}
						>
							<span>
								<Checkbox
									size="small"
									checked={isSelected}
									disabled={disableCheckbox}
									onChange={(event) => {
										event.stopPropagation();
										void handleToggleTable(node, event.target.checked);
									}}
								/>
							</span>
						</Tooltip>
						<Box sx={{ flex: 1, minWidth: 0 }}>
							<Typography
								variant="body2"
								sx={{
									fontWeight: isSelected ? 600 : 500,
									overflow: 'hidden',
									textOverflow: 'ellipsis'
								}}
							>
								{node.label}
							</Typography>
							<Typography
								variant="caption"
								color="text.secondary"
								sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis' }}
							>
								{node.schemaLabel}.{node.table.tableName}
							</Typography>
						</Box>
						{detailChips.length > 0 && (
							<Stack direction="row" spacing={0.5}>
								{detailChips}
							</Stack>
						)}
						{selectionInProgress && <CircularProgress size={16} />}
					</Stack>
				}
			/>
		);
	};

	const renderSchemaNode = (node: SchemaTreeNode) => (
		<TreeItem
			key={node.id}
			itemId={node.id}
			label={
				<Stack direction="row" spacing={1} alignItems="center" sx={{ py: 0.5, pr: 0.5 }}>
					<Box sx={{ flex: 1, minWidth: 0 }}>
						<Typography
							variant="body2"
							sx={{
								fontWeight: 500,
								overflow: 'hidden',
								textOverflow: 'ellipsis'
							}}
						>
							{node.label}
						</Typography>
					</Box>
					<Chip label={`${node.tableCount} tables`} size="small" variant="outlined" />
				</Stack>
			}
		>
			{node.children.map(renderTableNode)}
		</TreeItem>
	);

	const renderCatalogNode = (node: CatalogTreeNode) => (
		<TreeItem
			key={node.id}
			itemId={node.id}
			label={
				<Stack direction="row" spacing={1} alignItems="center" sx={{ py: 0.5, pr: 0.5 }}>
					<Box sx={{ flex: 1, minWidth: 0 }}>
						<Typography
							variant="body2"
							sx={{
								fontWeight: 600,
								overflow: 'hidden',
								textOverflow: 'ellipsis'
							}}
						>
							{node.label}
						</Typography>
					</Box>
					<Chip label={`${node.tableCount} tables`} size="small" variant="outlined" />
				</Stack>
			}
		>
			{node.children.map(renderSchemaNode)}
		</TreeItem>
	);

	return (
		<Dialog open={open} onClose={handleCloseDialog} maxWidth="lg" fullWidth>
			<DialogTitle>Select Source Tables</DialogTitle>
			<DialogContent dividers>
				<Stack spacing={2} mt={1}>
					{error && <Alert severity="error">{error}</Alert>}
					{localError && <Alert severity="error">{localError}</Alert>}
					<Typography variant="body2" color="text.secondary">
						Choose tables from the catalog hierarchy and pick the columns to import into this
						data definition.
					</Typography>
					<Box
						sx={{
							display: 'flex',
							flexDirection: { xs: 'column', md: 'row' },
							gap: 2,
							alignItems: { xs: 'stretch', md: 'flex-start' }
						}}
					>
						<Box
							sx={{
								flex: { xs: '0 0 auto', md: '0 0 360px' },
								minWidth: { xs: '100%', md: 320 }
							}}
						>
							<TextField
								value={searchTerm}
								onChange={handleSearchChange}
								placeholder="Search tables..."
								size="small"
								fullWidth
								disabled={loading || submitting}
								InputProps={{
									startAdornment: (
										<InputAdornment position="start">
											<SearchIcon fontSize="small" />
										</InputAdornment>
									)
								}}
							/>
							<Box
								sx={{
									mt: 2,
									border: '1px solid',
									borderColor: 'divider',
									borderRadius: 1,
									p: 1,
									maxHeight: 420,
									overflow: 'auto'
								}}
							>
								{catalogHierarchy.length ? (
									<SimpleTreeView
										aria-label="Available source tables"
										expandedItems={expandedNodes}
										onExpandedItemsChange={(_, nodeIds) => {
											if (Array.isArray(nodeIds)) {
												setExpandedNodes(nodeIds);
												return;
											}
											setExpandedNodes(nodeIds ? [nodeIds] : []);
										}}
										selectedItems={activeItemId ?? null}
										onSelectedItemsChange={(_, nodeIds) => {
											const nextActive = Array.isArray(nodeIds)
												? nodeIds[0] ?? null
												: nodeIds ?? null;
											void handleTreeSelectionChange(nextActive);
										}}
									>
										{catalogHierarchy.map(renderCatalogNode)}
									</SimpleTreeView>
								) : (
									<Typography variant="body2" color="text.secondary" sx={{ p: 1 }}>
										{searchTerm
											? 'No tables match the current search.'
											: 'No source tables are available.'}
									</Typography>
								)}
							</Box>
						</Box>

						<Box sx={{ flex: 1, minWidth: 0 }}>
							{activeMetadata ? (
								<Stack spacing={2}>
									<Stack spacing={0.5}>
										<Typography variant="subtitle2">{activeMetadata.tableName}</Typography>
										<Typography variant="body2" color="text.secondary">
											{`${activeMetadata.schemaName ?? DEFAULT_SCHEMA_LABEL} · ${
												activeMetadata.catalogName ?? DEFAULT_CATALOG_LABEL
											}`}
										</Typography>
									</Stack>

									{activeColumnState?.error && (
										<Alert severity="error">{activeColumnState.error}</Alert>
									)}

									{activeColumnState?.loading || pendingSelection === activeTableKey ? (
										<Box display="flex" justifyContent="center" py={2}>
											<CircularProgress size={24} />
										</Box>
									) : activeColumnState?.columns?.length ? (
										<>
											<FormControlLabel
												control={
													<Checkbox
														checked={
															allColumnsCount > 0 &&
															selectedColumnsCount === allColumnsCount
														}
														indeterminate={
															selectedColumnsCount > 0 &&
															selectedColumnsCount < allColumnsCount
														}
														onChange={() =>
															activeTableKey && handleToggleAllColumns(activeTableKey)
														}
													/>
												}
												label={`Select All (${selectedColumnsCount}/${allColumnsCount})`}
											/>
											<List
												dense
												sx={{
													maxHeight: 320,
													overflow: 'auto',
													border: '1px solid',
													borderColor: 'divider',
													borderRadius: 1
												}}
											>
												{activeColumnState.columns.map((column) => {
													const checked = activeSelectedSet.has(column.name);
													return (
														<ListItem
															key={column.name}
															dense
															sx={{ cursor: 'pointer' }}
															onClick={() =>
																activeTableKey && handleColumnToggle(activeTableKey, column.name)
															}
														>
															<ListItemIcon>
																<Checkbox
																	edge="start"
																	checked={checked}
																	tabIndex={-1}
																	disableRipple
																	onChange={(event) => {
																		event.stopPropagation();
																		if (activeTableKey) {
																			handleColumnToggle(activeTableKey, column.name);
																		}
																	}}
																/>
															</ListItemIcon>
															<ListItemText
																primary={column.name}
																secondary={column.typeName}
																primaryTypographyProps={{ variant: 'body2' }}
																secondaryTypographyProps={{ variant: 'caption' }}
															/>
														</ListItem>
													);
												})}
											</List>
										</>
									) : (
										<Typography variant="body2" color="text.secondary">
											{pendingSelection === activeTableKey
												? 'Loading columns...'
												: 'No columns are available for this table.'}
										</Typography>
									)}
								</Stack>
							) : (
								<Typography variant="body2" color="text.secondary">
									Select a table from the catalog to review its columns.
								</Typography>
							)}
						</Box>
					</Box>
				</Stack>
			</DialogContent>
			<DialogActions>
				<Button
					onClick={handleCloseDialog}
					disabled={loading || submitting || pendingSelection !== null || anyColumnsLoading}
				>
					Cancel
				</Button>
				<LoadingButton
					onClick={handleSubmit}
					variant="contained"
					loading={loading || submitting}
					disabled={
						loading ||
						submitting ||
						pendingSelection !== null ||
						anyColumnsLoading ||
						selectedTableKeys.length === 0
					}
				>
					Add Selected Tables
				</LoadingButton>
			</DialogActions>
		</Dialog>
	);
};

export default AddExistingSourceTableDialog;
