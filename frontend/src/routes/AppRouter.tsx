import { Navigate, Outlet, RouterProvider, createBrowserRouter } from 'react-router-dom';
import { CircularProgress, Box } from '@mui/material';
import { Suspense } from 'react';

import MainLayout from '../layout/MainLayout';
import OverviewPage from '@pages/OverviewPage';
import DataDefinitionsPage from '@pages/DataDefinitionPage';
import DataConstructionPage from '@pages/DataConstructionPage';
import ProcessAreasPage from '@pages/ProcessAreasPage';
import DataObjectsPage from '@pages/DataObjectsPage';
import ApplicationsPage from '@pages/ApplicationsPage';
import SourceCatalogPage from '@pages/SourceCatalogPage';
import IngestionSchedulesPage from '@pages/IngestionSchedulesPage';
import DataWarehouseSettingsPage from '@pages/DatabricksSettingsPage';
import ProjectsPage from '@pages/ProjectsPage';
import ReleasesPage from '@pages/ReleasesPage';
import LoginPage from '@pages/LoginPage';
import ReportingDesignerPage from '@pages/ReportingDesignerPage';
import ReportingCatalogPage from '@pages/ReportingCatalogPage';
import ApplicationDatabaseSetupPage from '@pages/ApplicationDatabaseSetupPage';
import CompanySettingsPage from '@pages/CompanySettingsPage';
import UploadDataPage from '@pages/UploadDataPage';
import { ProtectedRoute } from '@routes/guards/ProtectedRoute';
import { AuthProvider } from '@context/AuthContext';
import { ApplicationDatabaseGuard } from '@routes/guards/ApplicationDatabaseGuard';

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
            <ApplicationDatabaseGuard>
              <MainLayout />
            </ApplicationDatabaseGuard>
          </ProtectedRoute>
        ),
        children: [
          { index: true, element: <OverviewPage /> },
          { path: 'data-definitions', element: <DataDefinitionsPage /> },
          { path: 'data-definition', element: <Navigate to="/data-definitions" replace /> },
          { path: 'data-construction', element: <DataConstructionPage /> },
          { path: 'process-areas', element: <ProcessAreasPage /> },
          { path: 'applications', element: <ApplicationsPage /> },
          { path: 'systems', element: <Navigate to="/applications" replace /> },
          { path: 'data-configuration/source-catalog', element: <SourceCatalogPage /> },
          { path: 'data-configuration/ingestion-schedules', element: <IngestionSchedulesPage /> },
          { path: 'data-configuration/application-database', element: <ApplicationDatabaseSetupPage /> },
          { path: 'data-configuration/data-warehouse', element: <DataWarehouseSettingsPage /> },
          { path: 'data-configuration/upload-data', element: <UploadDataPage /> },
          { path: 'data-configuration/databricks', element: <Navigate to="/data-configuration/data-warehouse" replace /> },
          { path: 'application-settings/connections', element: <Navigate to="/data-configuration/source-catalog" replace /> },
          { path: 'application-settings/ingestion-schedules', element: <Navigate to="/data-configuration/ingestion-schedules" replace /> },
          { path: 'application-settings/application-database', element: <Navigate to="/data-configuration/application-database" replace /> },
          { path: 'application-settings/databricks', element: <Navigate to="/data-configuration/data-warehouse" replace /> },
          { path: 'application-settings/company', element: <CompanySettingsPage /> },
          { path: 'data-objects', element: <DataObjectsPage /> },
          { path: 'project-settings/projects', element: <ProjectsPage /> },
          { path: 'project-settings/releases', element: <ReleasesPage /> },
          { path: 'reporting/designer', element: <ReportingDesignerPage /> },
          { path: 'reporting/catalog', element: <ReportingCatalogPage /> }
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
