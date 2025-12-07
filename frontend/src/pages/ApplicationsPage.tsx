import { useEffect, useMemo, useState } from 'react';
import { Alert, Box, Button, Grid, Paper, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';

import PageHeader from '../components/common/PageHeader';
import SystemTable from '../components/system/SystemTable';
import SystemForm from '../components/system/SystemForm';
import ConfirmDialog from '../components/common/ConfirmDialog';
import { useSystems } from '../hooks/useSystems';
import { useAuth } from '../context/AuthContext';
import { System, SystemFormValues } from '../types/data';
import { getSectionSurface } from '../theme/surfaceStyles';

const getErrorMessage = (error: unknown, fallback: string) => {
	if (!error) return fallback;
	if (error instanceof Error) return error.message;
	if (typeof error === 'string') return error;
	if (typeof error === 'object') {
		const detail = (error as { detail?: unknown; message?: unknown }).detail;
		if (typeof detail === 'string') return detail;
		const message = (error as { detail?: unknown; message?: unknown }).message;
		if (typeof message === 'string') return message;
	}
	return fallback;
};

const ApplicationsPage = () => {
	const theme = useTheme();
	const { hasRole } = useAuth();
	const canManage = hasRole('admin');
	const isDarkMode = theme.palette.mode === 'dark';
	const sectionSurface = useMemo(
		() => getSectionSurface(theme, { shadow: isDarkMode ? 'raised' : 'subtle' }),
		[isDarkMode, theme]
	);

	const {
		systemsQuery,
		createSystem,
		updateSystem,
		deleteSystem,
		creating,
		updating,
		deleting
	} = useSystems();

	const {
		data: systems = [],
		isLoading,
		isError,
		error
	} = systemsQuery;

	const [selectedSystem, setSelectedSystem] = useState<System | null>(null);
	const [formOpen, setFormOpen] = useState(false);
	const [formMode, setFormMode] = useState<'create' | 'edit'>('create');
	const [confirmOpen, setConfirmOpen] = useState(false);

	const sortedSystems = useMemo(() => {
		return systems.slice().sort((a, b) => a.name.localeCompare(b.name));
	}, [systems]);

	useEffect(() => {
		if (selectedSystem && !systems.some((system) => system.id === selectedSystem.id)) {
			setSelectedSystem(null);
		}
	}, [systems, selectedSystem]);

	const handleSelect = (system: System | null) => {
		setSelectedSystem(system);
	};

	const handleCreateClick = () => {
		setFormMode('create');
		setSelectedSystem(null);
		setFormOpen(true);
	};

	const handleEdit = (system: System) => {
		setFormMode('edit');
		setSelectedSystem(system);
		setFormOpen(true);
	};

	const handleDeleteRequest = (system: System) => {
		setSelectedSystem(system);
		setConfirmOpen(true);
	};

	const handleFormClose = () => {
		setFormOpen(false);
	};

	const handleFormSubmit = async (values: SystemFormValues) => {
		try {
			if (formMode === 'create') {
				await createSystem(values);
				setSelectedSystem(null);
			} else if (selectedSystem) {
				await updateSystem({
					id: selectedSystem.id,
					input: values
				});
			}
			setFormOpen(false);
		} catch (_submissionError) {
			// Toast handled from the systems hook
		}
	};

	const handleConfirmDelete = async () => {
		if (!selectedSystem) return;
		try {
			await deleteSystem(selectedSystem.id);
			setConfirmOpen(false);
			setSelectedSystem(null);
		} catch (_deleteError) {
			// Toast handled from the systems hook
		}
	};

	const errorMessage = isError
		? getErrorMessage(error, 'Unable to load applications.')
		: null;

	const saving = creating || updating;

	return (
		<Box>
			<PageHeader
				title="Applications"
				subtitle="Register and maintain every application feeding Conversion Central."
			/>

			{errorMessage && (
				<Alert severity="error" sx={{ mb: 3 }}>
					{errorMessage}
				</Alert>
			)}

			<Grid container spacing={3} alignItems="flex-start">
				<Grid item xs={12}>
					<Paper
						elevation={0}
						sx={{
							p: 3,
							borderRadius: 3,
							...sectionSurface
						}}
					>
						<Box display="flex" alignItems="center" justifyContent="space-between" mb={3}>
							<Typography
								variant="h5"
								sx={{
									color: isDarkMode ? theme.palette.common.white : theme.palette.primary.dark,
									fontWeight: 700
								}}
							>
								Applications
							</Typography>
							{canManage && (
								<Button variant="contained" onClick={handleCreateClick} disabled={saving}>
									New Application
								</Button>
							)}
						</Box>

						<Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
							Applications map physical source systems to business data objects. Keep this list current so downstream table catalogs and data quality workflows stay aligned.
						</Typography>

						<SystemTable
							data={sortedSystems}
							loading={isLoading}
							selectedId={selectedSystem?.id ?? null}
							canManage={canManage}
							onSelect={handleSelect}
							onEdit={canManage ? handleEdit : undefined}
							onDelete={canManage ? handleDeleteRequest : undefined}
						/>
					</Paper>
				</Grid>
			</Grid>

			{canManage && (
				<>
					<SystemForm
						open={formOpen}
						title={formMode === 'create' ? 'Create Application' : 'Edit Application'}
						initialValues={formMode === 'edit' ? selectedSystem : null}
						loading={saving}
						onClose={handleFormClose}
						onSubmit={handleFormSubmit}
					/>

					<ConfirmDialog
						open={confirmOpen}
						title="Delete Application"
						description={
							selectedSystem
								? `Are you sure you want to delete ${selectedSystem.name}? This cannot be undone.`
								: undefined
						}
						confirmLabel="Delete"
						onClose={() => setConfirmOpen(false)}
						onConfirm={handleConfirmDelete}
						loading={deleting}
					/>
				</>
			)}
		</Box>
	);
};

export default ApplicationsPage;
