import { Navigate, Outlet, RouterProvider, createBrowserRouter } from 'react-router-dom';
import { CircularProgress, Box } from '@mui/material';
import { Suspense } from 'react';

import MainLayout from '../layout/MainLayout';
import OverviewPage from '@pages/OverviewPage';
import DataDefinitionsPage from '@pages/DataDefinitionPage';
import DataConstructionPage from '@pages/DataConstructionPage';
import ProcessAreasPage from '@pages/ProcessAreasPage';
import DataObjectsPage from '@pages/DataObjectsPage';
import SourceSystemsPage from '@pages/SourceSystemsPage';
import IngestionSchedulesPage from '@pages/IngestionSchedulesPage';
import DataWarehouseSettingsPage from '@pages/DatabricksSettingsPage';
import ProjectsPage from '@pages/ProjectsPage';
import ReleasesPage from '@pages/ReleasesPage';
import LoginPage from '@pages/LoginPage';
import ReportingDesignerPage from '@pages/ReportingDesignerPage';
import ReportingCatalogPage from '@pages/ReportingCatalogPage';
import CompanySettingsPage from '@pages/CompanySettingsPage';
import LegalRequirementsPage from '@pages/LegalRequirementsPage';
import SecurityClassificationsPage from '@pages/SecurityClassificationsPage';
import UploadDataPage from '@pages/UploadDataPage';
import DataQualityOverviewPage from '@pages/DataQualityOverviewPage';
import DataQualityDatasetsPage from '@pages/DataQualityDatasetsPage';
import DataQualityRunHistoryPage from '@pages/DataQualityRunHistoryPage';
import DataQualityProfilingRunsPage from '@pages/DataQualityProfilingRunsPage';
import DataQualityProfileRunResultsPage from '@pages/DataQualityProfileRunResultsPage';
import DataQualityTestLibraryPage from '@pages/DataQualityTestLibraryPage';
import DataQualityTestSuitesPage from '@pages/DataQualityTestSuitesPage';
import DataQualityTestSuiteDefinitionPage from '@pages/DataQualityTestSuiteDefinitionPage';
import DataQualitySettingsPage from '@pages/DataQualitySettingsPage';
import { ProtectedRoute } from '@routes/guards/ProtectedRoute';
import { AuthProvider } from '@context/AuthContext';

const Loader = () => (
  <Box display="flex" justifyContent="center" alignItems="center" height="100vh">
    <CircularProgress />
  </Box>
);

const AuthBoundary = () => (
  <AuthProvider>
    <Outlet />
  </AuthProvider>
);

const router = createBrowserRouter([
  {
    element: <AuthBoundary />,
    children: [
      {
        path: '/login',
        element: <LoginPage />
      },
      {
        path: '/',
        element: (
          <ProtectedRoute>
            <MainLayout />
          </ProtectedRoute>
        ),
        children: [
          { index: true, element: <OverviewPage /> },
          { path: 'data-definitions', element: <DataDefinitionsPage /> },
          { path: 'data-definition', element: <Navigate to="/data-definitions" replace /> },
          { path: 'data-construction', element: <DataConstructionPage /> },
          { path: 'process-areas', element: <ProcessAreasPage /> },
          { path: 'applications', element: <Navigate to="/data-configuration/source-systems" replace /> },
          { path: 'systems', element: <Navigate to="/data-configuration/source-systems" replace /> },
          { path: 'data-configuration/source-systems', element: <SourceSystemsPage /> },
          { path: 'data-configuration/source-catalog', element: <Navigate to="/data-configuration/source-systems" replace /> },
          { path: 'data-configuration/ingestion-schedules', element: <IngestionSchedulesPage /> },
          { path: 'data-configuration/data-warehouse', element: <DataWarehouseSettingsPage /> },
          { path: 'data-configuration/upload-data', element: <UploadDataPage /> },
          { path: 'data-configuration/databricks', element: <Navigate to="/data-configuration/data-warehouse" replace /> },
          { path: 'application-settings/connections', element: <Navigate to="/data-configuration/source-systems" replace /> },
          { path: 'application-settings/ingestion-schedules', element: <Navigate to="/data-configuration/ingestion-schedules" replace /> },
          { path: 'application-settings/databricks', element: <Navigate to="/data-configuration/data-warehouse" replace /> },
          { path: 'application-settings/company', element: <CompanySettingsPage /> },
          { path: 'application-settings/legal-requirements', element: <LegalRequirementsPage /> },
          { path: 'application-settings/security-classifications', element: <SecurityClassificationsPage /> },
          { path: 'data-objects', element: <DataObjectsPage /> },
          { path: 'project-settings/projects', element: <ProjectsPage /> },
          { path: 'project-settings/releases', element: <ReleasesPage /> },
          { path: 'reporting/designer', element: <ReportingDesignerPage /> },
          { path: 'reporting/catalog', element: <ReportingCatalogPage /> },
          { path: 'data-quality', element: <DataQualityOverviewPage /> },
          { path: 'data-quality/datasets', element: <DataQualityDatasetsPage /> },
          { path: 'data-quality/profiling-runs', element: <DataQualityProfilingRunsPage /> },
          { path: 'data-quality/profiling-runs/:profileRunId/results', element: <DataQualityProfileRunResultsPage /> },
          { path: 'data-quality/test-suites', element: <DataQualityTestSuitesPage /> },
          { path: 'data-quality/test-suites/:testSuiteKey', element: <DataQualityTestSuiteDefinitionPage /> },
          { path: 'data-quality/test-library', element: <DataQualityTestLibraryPage /> },
          { path: 'data-quality/run-history', element: <DataQualityRunHistoryPage /> },
          { path: 'data-quality/settings', element: <DataQualitySettingsPage /> }
        ]
      },
      {
        path: '*',
        element: <Navigate to="/" replace />
      }
    ]
  }
]);

const AppRouter = () => (
  <Suspense fallback={<Loader />}>
    <RouterProvider
      router={router}
      fallbackElement={<Loader />}
      future={{ v7_startTransition: true, v7_relativeSplatPath: true } as Record<string, boolean>}
    />
  </Suspense>
);

export default AppRouter;
