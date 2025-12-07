import { useCallback, useEffect, useMemo, useState } from 'react';
import {
	Alert,
	Box,
	Button,
	Checkbox,
	Chip,
	MenuItem,
	Paper,
	Stack,
	TextField,
	Typography
} from '@mui/material';
import { useTheme } from '@mui/material/styles';
import {
	DataGrid,
	GridActionsCellItem,
	GridColDef
} from '@mui/x-data-grid';
import VisibilityIcon from '@mui/icons-material/Visibility';
import RefreshIcon from '@mui/icons-material/Refresh';
import InsightsIcon from '@mui/icons-material/Insights';

import PageHeader from '../components/common/PageHeader';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { useSystemConnections } from '../hooks/useSystemConnections';
import {
	ConnectionCatalogSelectionInput,
	ConnectionCatalogTable as CatalogRow,
	ConnectionTablePreview,
	System,
	SystemConnection,
	TableObservabilityMetric
} from '../types/data';
import {
	fetchConnectionTablePreview,
	fetchSystemConnectionCatalog,
	updateSystemConnectionCatalogSelection
} from '../services/systemConnectionService';
import ConnectionDataPreviewDialog from '../components/system-connection/ConnectionDataPreviewDialog';
import TableObservabilityMetricsDialog from '../components/table-observability/TableObservabilityMetricsDialog';
import { getSectionSurface } from '../theme/surfaceStyles';
import { fetchTableObservabilityMetrics } from '../services/tableObservabilityService';

const getErrorMessage = (error: unknown, fallback: string): string => {
	if (!error) return fallback;
	if (error instanceof Error) return error.message;
	if (typeof error === 'string') return error;
	if (typeof error === 'object') {
		const candidate = (error as { detail?: unknown; message?: unknown }).detail ??
			(error as { detail?: unknown; message?: unknown }).message;
		if (typeof candidate === 'string') {
			return candidate;
		}
	}

	return fallback;
};

const buildConnectionDescriptor = (
	connection: SystemConnection,
	systemLookup: Map<string, System>
) => {
	const connectionName = connection.name?.trim() || 'Connection';
	const systemRecord = connection.systemId ? systemLookup.get(connection.systemId) ?? null : null;
	const systemName = systemRecord?.name?.trim() || systemRecord?.physicalName?.trim() || 'Global connection';
	return {
		connectionName,
		systemName,
	};
};

const describeTableLabel = (row: { rawSchemaName: string | null; tableName: string }) => (
	row.rawSchemaName ? `${row.rawSchemaName}.${row.tableName}` : row.tableName
);

interface CatalogRowDisplay {
	id: string;
	connectionId: string;
	connectionName: string;
	schemaName: string;
	tableName: string;
	tableType?: string | null;
	columnCount?: number | null;
	estimatedRows?: number | null;
	available: boolean;
	selected: boolean;
	rawSchemaName: string | null;
	selectionId?: string | null;
}

interface PreviewRequest {
	connectionId: string;
	schemaName: string | null;
	tableName: string;
}

const TablesPage = () => {
	const theme = useTheme();
	const isDarkMode = theme.palette.mode === 'dark';
	const sectionSurface = useMemo(
		() => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }),
		[isDarkMode, theme]
	);

	const {
		systemsQuery: {
			data: systems = [],
			isLoading: systemsLoading,
			isError: systemsError,
			error: systemsErrorObj
		}
	} = useSystems();

	const {
		connectionsQuery: {
			data: connections = [],
			isLoading: connectionsLoading,
			isError: connectionsError,
			error: connectionsErrorObj
		}
	} = useSystemConnections();

	const [catalogMap, setCatalogMap] = useState<Record<string, CatalogRow[]>>({});
	const [catalogLoading, setCatalogLoading] = useState<Record<string, boolean>>({});
	const [catalogSaving, setCatalogSaving] = useState<Record<string, boolean>>({});
	const [catalogError, setCatalogError] = useState<Record<string, string | null>>({});
	const [catalogConfirmOpen, setCatalogConfirmOpen] = useState(false);
	const [catalogPendingChange, setCatalogPendingChange] = useState<{
		connectionId: string;
		nextSelectionIds: string[];
		removedRows: CatalogRow[];
	} | null>(null);
	const [selectedConnectionFilter, setSelectedConnectionFilter] = useState<string>('all');
	const [searchQuery, setSearchQuery] = useState('');
	const [schemaFilter, setSchemaFilter] = useState<string>('all');

	const [previewRequest, setPreviewRequest] = useState<PreviewRequest | null>(null);
	const [previewOpen, setPreviewOpen] = useState(false);
	const [previewData, setPreviewData] = useState<ConnectionTablePreview | null>(null);
	const [previewLoading, setPreviewLoading] = useState(false);
	const [previewError, setPreviewError] = useState<string | null>(null);
	const [metricsDialogOpen, setMetricsDialogOpen] = useState(false);
	const [metricsTarget, setMetricsTarget] = useState<CatalogRowDisplay | null>(null);
	const [metricsRecords, setMetricsRecords] = useState<TableObservabilityMetric[]>([]);
	const [metricsLoading, setMetricsLoading] = useState(false);
	const [metricsError, setMetricsError] = useState<string | null>(null);

	const systemLookup = useMemo(
		() => new Map<string, System>(systems.map((system) => [system.id, system])),
		[systems]
	);

	const catalogableConnections = useMemo(
		() => connections.filter((connection) => connection.connectionType === 'jdbc'),
		[connections]
	);

	const connectionDescriptors = useMemo(() => {
		const map = new Map<string, ReturnType<typeof buildConnectionDescriptor>>();
		catalogableConnections.forEach((connection) => {
			map.set(connection.id, buildConnectionDescriptor(connection, systemLookup));
		});
		return map;
	}, [catalogableConnections, systemLookup]);

	useEffect(() => {
		if (selectedConnectionFilter === 'all') {
			return;
		}
		const stillExists = catalogableConnections.some(
			(connection) => connection.id === selectedConnectionFilter
		);
		if (!stillExists) {
			setSelectedConnectionFilter('all');
		}
	}, [catalogableConnections, selectedConnectionFilter]);

	const loadCatalog = useCallback(async (connectionId: string) => {
		setCatalogLoading((prev) => ({ ...prev, [connectionId]: true }));
		setCatalogError((prev) => ({ ...prev, [connectionId]: null }));
		try {
			const rows = await fetchSystemConnectionCatalog(connectionId);
			setCatalogMap((prev) => ({ ...prev, [connectionId]: rows }));
		} catch (error) {
			setCatalogError((prev) => ({
				...prev,
				[connectionId]: getErrorMessage(error, 'Unable to load catalog for this connection.')
			}));
		} finally {
			setCatalogLoading((prev) => ({ ...prev, [connectionId]: false }));
		}
	}, []);

	useEffect(() => {
		catalogableConnections.forEach((connection) => {
			if (catalogMap[connection.id] || catalogLoading[connection.id]) {
				return;
			}
			void loadCatalog(connection.id);
		});
	}, [catalogableConnections, catalogLoading, catalogMap, loadCatalog]);

	const persistCatalogSelection = useCallback(
		async (connectionId: string, selectionIds: string[]) => {
			const rows = catalogMap[connectionId] ?? [];
			const selectionSet = new Set(selectionIds);
			const payload: ConnectionCatalogSelectionInput[] = rows
				.filter((row) => selectionSet.has(`${row.schemaName}.${row.tableName}`))
				.map((row) => ({
					schemaName: row.schemaName,
					tableName: row.tableName,
					tableType: row.tableType ?? undefined,
					columnCount: row.columnCount ?? undefined,
					estimatedRows: row.estimatedRows ?? undefined
				}));

			setCatalogSaving((prev) => ({ ...prev, [connectionId]: true }));
			setCatalogError((prev) => ({ ...prev, [connectionId]: null }));

			try {
				await updateSystemConnectionCatalogSelection(connectionId, payload);
				await loadCatalog(connectionId);
			} catch (error) {
				setCatalogError((prev) => ({
					...prev,
					[connectionId]: getErrorMessage(error, 'Unable to save selection.')
				}));
			} finally {
				setCatalogSaving((prev) => ({ ...prev, [connectionId]: false }));
			}
		},
		[catalogMap, loadCatalog]
	);

	const handleConnectionSelectionChange = useCallback(
		(connectionId: string, nextSelection: string[]) => {
			const rows = catalogMap[connectionId] ?? [];
			const previousSelection = rows
				.filter((row) => row.selected)
				.map((row) => `${row.schemaName}.${row.tableName}`);
			const nextSet = new Set(nextSelection);

			if (
				previousSelection.length === nextSelection.length &&
				previousSelection.every((id) => nextSet.has(id))
			) {
				return;
			}

			const normalizedNextSelection = Array.from(nextSet).sort();
			const removedIds = previousSelection.filter((id) => !nextSet.has(id));

			if (removedIds.length > 0) {
				const removedSet = new Set(removedIds);
				const removedRows = rows.filter((row) => removedSet.has(`${row.schemaName}.${row.tableName}`));
				setCatalogPendingChange({
					connectionId,
					nextSelectionIds: normalizedNextSelection,
					removedRows
				});
				setCatalogConfirmOpen(true);
				return;
			}

			void persistCatalogSelection(connectionId, normalizedNextSelection);
		},
		[catalogMap, persistCatalogSelection]
	);

	const handleInlineSelectionAction = useCallback(
		(row: CatalogRowDisplay, nextSelected: boolean) => {
			if (selectedConnectionFilter === 'all' || row.connectionId !== selectedConnectionFilter) {
				return;
			}
			const rows = catalogMap[row.connectionId] ?? [];
			const buildSelectionKey = (schemaName: string | null, tableName: string) => `${schemaName ?? ''}.${tableName}`;
			const currentSelection = rows
				.filter((entry) => entry.selected)
				.map((entry) => buildSelectionKey(entry.schemaName ?? null, entry.tableName));
			const updated = new Set(currentSelection);
			const targetKey = buildSelectionKey(row.rawSchemaName ?? null, row.tableName);
			if (nextSelected) {
				updated.add(targetKey);
			} else {
				updated.delete(targetKey);
			}
			handleConnectionSelectionChange(row.connectionId, Array.from(updated));
		},
		[catalogMap, handleConnectionSelectionChange, selectedConnectionFilter]
	);

	const catalogConfirmDescription = useMemo(() => {
		if (!catalogPendingChange || catalogPendingChange.removedRows.length === 0) {
			return '';
		}
		const descriptor = connectionDescriptors.get(catalogPendingChange.connectionId);
		const connectionLabel = descriptor?.connectionName ?? 'this connection';
		const targets = catalogPendingChange.removedRows
			.map((row) => {
				const schema = row.schemaName?.trim();
				return schema ? `${schema}.${row.tableName}` : row.tableName;
			})
			.join(', ');
		return `Unselecting these tables for ${connectionLabel} will remove their constructed tables, data definitions, and related reports. This action cannot be undone. Impacted tables: ${targets}.`;
	}, [catalogPendingChange, connectionDescriptors]);

	const handleCatalogConfirmCascade = useCallback(async () => {
		if (!catalogPendingChange) return;
		await persistCatalogSelection(catalogPendingChange.connectionId, catalogPendingChange.nextSelectionIds);
		setCatalogPendingChange(null);
		setCatalogConfirmOpen(false);
	}, [catalogPendingChange, persistCatalogSelection]);

	const handleCatalogCancelCascade = useCallback(() => {
		if (catalogPendingChange) {
			void loadCatalog(catalogPendingChange.connectionId);
		}
		setCatalogPendingChange(null);
		setCatalogConfirmOpen(false);
	}, [catalogPendingChange, loadCatalog]);

	const aggregatedRows = useMemo<CatalogRowDisplay[]>(() => {
		const rows: CatalogRowDisplay[] = [];
		catalogableConnections.forEach((connection) => {
			const catalog = catalogMap[connection.id] ?? [];
			if (!catalog) {
				return;
			}
			const descriptor = connectionDescriptors.get(connection.id);
			catalog.forEach((row) => {
				const normalizedSchema = row.schemaName?.trim() ?? '';
				const schemaLabel = normalizedSchema.length > 0 ? normalizedSchema : '(No schema)';
				rows.push({
					id: `${connection.id}:${row.schemaName ?? 'default'}:${row.tableName}`,
					connectionId: connection.id,
					connectionName: descriptor?.connectionName ?? 'Connection',
					schemaName: schemaLabel,
					tableName: row.tableName,
					tableType: row.tableType ?? null,
					columnCount: row.columnCount ?? null,
					estimatedRows: row.estimatedRows ?? null,
					available: row.available,
					selected: row.selected,
					rawSchemaName: normalizedSchema.length > 0 ? normalizedSchema : null,
					selectionId: row.selectionId ?? null
				});
			});
		});

		return rows.sort((a, b) => {
			if (a.connectionName !== b.connectionName) {
				return a.connectionName.localeCompare(b.connectionName);
			}
			if (a.schemaName !== b.schemaName) {
				return a.schemaName.localeCompare(b.schemaName);
			}
			return a.tableName.localeCompare(b.tableName);
		});
	}, [catalogMap, catalogableConnections, connectionDescriptors]);

	const availableSchemas = useMemo<string[]>(() => {
		const schemas = new Set<string>();
		aggregatedRows.forEach((row) => {
			if (selectedConnectionFilter !== 'all' && row.connectionId !== selectedConnectionFilter) {
				return;
			}
			schemas.add(row.rawSchemaName ?? '');
		});
		return Array.from(schemas).sort((a, b) => {
			const labelA = a || '(No schema)';
			const labelB = b || '(No schema)';
			return labelA.localeCompare(labelB);
		});
	}, [aggregatedRows, selectedConnectionFilter]);

	const filteredRows = useMemo(() => {
		const normalizedQuery = searchQuery.trim().toLowerCase();
		return aggregatedRows.filter((row) => {
			const matchesConnection =
				selectedConnectionFilter === 'all' || row.connectionId === selectedConnectionFilter;
			const matchesSchema = schemaFilter === 'all' || row.rawSchemaName === schemaFilter;
			if (!matchesConnection) {
				return false;
			}
			if (!matchesSchema) {
				return false;
			}
			if (!normalizedQuery) {
				return true;
			}
			return (
				row.schemaName.toLowerCase().includes(normalizedQuery) ||
				row.tableName.toLowerCase().includes(normalizedQuery) ||
				row.connectionName.toLowerCase().includes(normalizedQuery)
			);
		});
	}, [aggregatedRows, searchQuery, selectedConnectionFilter, schemaFilter]);

	const selectedConnectionLoading = selectedConnectionFilter === 'all'
		? false
		: catalogLoading[selectedConnectionFilter] ?? false;
	const selectedConnectionSaving = selectedConnectionFilter === 'all'
		? false
		: catalogSaving[selectedConnectionFilter] ?? false;

	const connectionsWithErrors = useMemo(
		() => catalogableConnections.filter((connection) => Boolean(catalogError[connection.id])),
		[catalogError, catalogableConnections]
	);

	const totalTablesLoaded = aggregatedRows.length;
	const loadedConnections = catalogableConnections.filter(
		(connection) => (catalogMap[connection.id] ?? []).length > 0
	).length;
	const anyCatalogLoading = catalogableConnections.some((connection) => catalogLoading[connection.id]);
	const anyCatalogSaving = catalogableConnections.some((connection) => catalogSaving[connection.id]);

	const overallLoading = systemsLoading || connectionsLoading || anyCatalogLoading;

	const handleRefreshAll = () => {
		catalogableConnections.forEach((connection) => {
			void loadCatalog(connection.id);
		});
	};

	const executePreview = useCallback(async (request: PreviewRequest) => {
		setPreviewLoading(true);
		setPreviewError(null);
		try {
			const data = await fetchConnectionTablePreview(
				request.connectionId,
				request.schemaName,
				request.tableName
			);
			setPreviewData(data);
		} catch (error) {
			setPreviewError(getErrorMessage(error, 'Unable to load preview.'));
		} finally {
			setPreviewLoading(false);
		}
	}, []);

	const openPreviewTarget = useCallback(
		(connectionId: string, schemaName: string | null, tableName: string) => {
			const request: PreviewRequest = {
				connectionId,
				schemaName,
				tableName
			};
			setPreviewRequest(request);
			setPreviewOpen(true);
			void executePreview(request);
		},
		[executePreview]
	);

	const handlePreview = useCallback((row: CatalogRowDisplay) => {
		openPreviewTarget(row.connectionId, row.rawSchemaName, row.tableName);
	}, [openPreviewTarget]);


	const handlePreviewRefresh = () => {
		if (previewRequest) {
			void executePreview(previewRequest);
		}
	};

	const handlePreviewClose = () => {
		setPreviewOpen(false);
		setPreviewRequest(null);
		setPreviewData(null);
		setPreviewError(null);
	};

	const loadObservabilityMetrics = useCallback(async (row: CatalogRowDisplay) => {
		if (!row.selectionId) {
			setMetricsRecords([]);
			setMetricsError('Select this table before observability metrics are captured.');
			return;
		}
		setMetricsLoading(true);
		setMetricsError(null);
		try {
			const data = await fetchTableObservabilityMetrics({
				selectionId: row.selectionId,
				limit: 100
			});
			setMetricsRecords(data);
		} catch (error) {
			setMetricsError(getErrorMessage(error, 'Unable to load observability metrics.'));
		} finally {
			setMetricsLoading(false);
		}
	}, []);

	const handleMetrics = useCallback((row: CatalogRowDisplay) => {
		setMetricsTarget(row);
		setMetricsDialogOpen(true);
		void loadObservabilityMetrics(row);
	}, [loadObservabilityMetrics]);

	const handleMetricsRefresh = useCallback(() => {
		if (metricsTarget) {
			void loadObservabilityMetrics(metricsTarget);
		}
	}, [loadObservabilityMetrics, metricsTarget]);

	const handleMetricsClose = useCallback(() => {
		setMetricsDialogOpen(false);
		setMetricsTarget(null);
		setMetricsRecords([]);
		setMetricsError(null);
	}, []);

	const columns = useMemo<GridColDef<CatalogRowDisplay>[]>(() => [
		{
			field: 'connectionName',
			headerName: 'Connection',
			flex: 1.3,
			renderCell: ({ row }) => (
				<Typography variant="body2" sx={{ fontWeight: 600 }}>
					{row.connectionName}
				</Typography>
			)
		},
		{
			field: 'schemaName',
			headerName: 'Schema',
			flex: 0.8
		},
		{
			field: 'tableName',
			headerName: 'Table',
			flex: 1
		},
		{
			field: 'tableType',
			headerName: 'Type',
			width: 130,
			valueFormatter: ({ value }) => (value ? String(value).replace('_', ' ') : '—')
		},
		{
			field: 'columnCount',
			headerName: 'Columns',
			width: 120,
			align: 'center'
		},
		{
			field: 'estimatedRows',
			headerName: 'Rows (est.)',
			width: 150,
			valueFormatter: ({ value }) =>
				value === null || value === undefined
					? '—'
					: new Intl.NumberFormat().format(value as number)
		},
		{
			field: 'selected',
			headerName: 'Selected',
			width: 140,
			renderCell: ({ row }) => {
				const isEditable = selectedConnectionFilter !== 'all' && row.connectionId === selectedConnectionFilter;
				return (
					<Checkbox
						checked={row.selected}
						disabled={!isEditable || selectedConnectionLoading || selectedConnectionSaving}
						onChange={(event) => handleInlineSelectionAction(row, event.target.checked)}
						inputProps={{ 'aria-label': 'Toggle table selection' }}
					/>
				);
			}
		},
		{
			field: 'available',
			headerName: 'Status',
			width: 120,
			renderCell: ({ value }) => (
				<Chip
					label={value ? 'Available' : 'Missing'}
					size="small"
					color={value ? 'success' : 'warning'}
					variant={value ? 'outlined' : 'filled'}
				/>
			)
		},
		{
			field: 'actions',
			type: 'actions',
			headerName: 'Actions',
			width: 140,
			getActions: ({ row }) => [
				<GridActionsCellItem
					key="metrics"
					icon={<InsightsIcon fontSize="small" />}
					label="Observability metrics"
					onClick={() => handleMetrics(row)}
					disabled={!row.selectionId}
				/>,
				<GridActionsCellItem
					key="preview"
					icon={<VisibilityIcon fontSize="small" />}
					label="Preview"
					onClick={() => handlePreview(row)}
					disabled={!row.available}
				/>
			]
		}
	], [handleInlineSelectionAction, handleMetrics, handlePreview, selectedConnectionFilter, selectedConnectionLoading, selectedConnectionSaving]);

	const connectionsErrorMessage = connectionsError
		? getErrorMessage(connectionsErrorObj, 'Unable to load connections.')
		: null;
	const systemsErrorMessage = systemsError
		? getErrorMessage(systemsErrorObj, 'Unable to load applications.')
		: null;

	return (
		<Box>
			<PageHeader
				title="Tables"
				subtitle="Review every table surfaced by your JDBC connections and quickly preview sample rows."
			/>

			{connectionsErrorMessage && (
				<Alert severity="error" sx={{ mb: 2 }}>
					{connectionsErrorMessage}
				</Alert>
			)}

			{systemsErrorMessage && (
				<Alert severity="error" sx={{ mb: 2 }}>
					{systemsErrorMessage}
				</Alert>
			)}

			{catalogableConnections.length === 0 && !connectionsLoading && (
				<Alert severity="info">
					No JDBC connections are registered yet. Create a JDBC connection on the Connections page to view its catalog here.
				</Alert>
			)}

			{catalogableConnections.length > 0 && (
				<Stack spacing={3}>
					<Paper
						elevation={0}
						sx={{
							p: 3,
							borderRadius: 3,
							...sectionSurface
						}}
					>
						<Stack spacing={2}>
							<Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
								<TextField
									select
									fullWidth
									label="Connection"
									value={selectedConnectionFilter}
									onChange={(event) => setSelectedConnectionFilter(event.target.value)}
								>
									<MenuItem value="all">All connections</MenuItem>
									{catalogableConnections.map((connection) => {
										const descriptor = connectionDescriptors.get(connection.id);
										return (
											<MenuItem key={connection.id} value={connection.id}>
												{descriptor?.connectionName ?? 'Connection'}
											</MenuItem>
										);
									})}
								</TextField>
								<TextField
									select
									fullWidth
									label="Schema"
									value={schemaFilter}
									onChange={(event) => setSchemaFilter(event.target.value)}
								>
									<MenuItem value="all">All schemas</MenuItem>
									{availableSchemas.map((schemaValue) => (
										<MenuItem key={schemaValue || '(empty)'} value={schemaValue}>
											{schemaValue || '(No schema)'}
										</MenuItem>
									))}
								</TextField>
								<TextField
									fullWidth
									label="Search"
									placeholder="Filter by schema, table, or connection"
									value={searchQuery}
									onChange={(event) => setSearchQuery(event.target.value)}
								/>
							</Stack>

							<Stack direction="row" spacing={1} flexWrap="wrap">
								<Chip label={`Connections loaded: ${loadedConnections}/${catalogableConnections.length}`} />
								<Chip label={`Tables loaded: ${totalTablesLoaded}`} />
							</Stack>

							<Stack direction="row" spacing={2} alignItems="center" justifyContent="space-between">
								<Typography variant="body2" color="text.secondary">
									Catalog data refreshes on demand. Use the controls below to reload individual connections or all catalogs at once.
								</Typography>
								<Button
									variant="contained"
									startIcon={<RefreshIcon />}
									onClick={handleRefreshAll}
									disabled={anyCatalogLoading || anyCatalogSaving}
								>
									Refresh Catalogs
								</Button>
							</Stack>

							{connectionsWithErrors.length > 0 && (
								<Alert severity="warning">
									<Stack spacing={1}>
										{connectionsWithErrors.map((connection) => {
											const descriptor = connectionDescriptors.get(connection.id);
											return (
												<Stack key={connection.id} direction="row" spacing={1} alignItems="center" justifyContent="space-between">
													<Box>
														<Typography variant="body2" sx={{ fontWeight: 600 }}>
															{descriptor?.connectionName ?? 'Connection'}
														</Typography>
														<Typography variant="body2">
															  {catalogError[connection.id] ?? 'Unable to load catalog.'}
														</Typography>
													</Box>
													<Button size="small" onClick={() => loadCatalog(connection.id)}>
														Retry
													</Button>
												</Stack>
											);
										})}
									</Stack>
								</Alert>
							)}
						</Stack>
					</Paper>

					<Paper
						elevation={0}
						sx={{
							p: 3,
							borderRadius: 3,
							...sectionSurface
						}}
					>
						<Box sx={{ height: 640, width: '100%' }}>
							<DataGrid
								rows={filteredRows}
								columns={columns}
								loading={overallLoading}
								getRowId={(row) => row.id}
								disableRowSelectionOnClick
								sx={{
									border: 'none',
									'& .MuiDataGrid-columnHeaders': {
										fontWeight: 600
									}
								}}
								initialState={{
									pagination: {
										paginationModel: { pageSize: 25 }
									}
								}}
								pageSizeOptions={[25, 50, 100]}
							/>
						</Box>
					</Paper>
				</Stack>
			)}

			<TableObservabilityMetricsDialog
				open={metricsDialogOpen}
				tableLabel={metricsTarget ? describeTableLabel(metricsTarget) : 'Table'}
				metrics={metricsRecords}
				loading={metricsLoading}
				error={metricsError}
				onClose={handleMetricsClose}
				onRefresh={handleMetricsRefresh}
			/>

			<ConfirmDialog
				open={catalogConfirmOpen}
				title="Remove Selected Tables"
				description={catalogConfirmDescription}
				confirmLabel="Remove"
				onClose={handleCatalogCancelCascade}
				onConfirm={handleCatalogConfirmCascade}
				loading={catalogPendingChange ? catalogSaving[catalogPendingChange.connectionId] ?? false : false}
			/>

			<ConnectionDataPreviewDialog
				open={previewOpen}
				schemaName={previewRequest?.schemaName ?? null}
				tableName={previewRequest?.tableName ?? 'Table'}
				loading={previewLoading}
				error={previewError}
				preview={previewData}
				onClose={handlePreviewClose}
				onRefresh={handlePreviewRefresh}
			/>
		</Box>
	);
};

export default TablesPage;
