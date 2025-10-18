import { Navigate, Outlet, RouterProvider, createBrowserRouter } from 'react-router-dom';
import { CircularProgress, Box } from '@mui/material';
import { Suspense } from 'react';

import MainLayout from '../layout/MainLayout';
import OverviewPage from '@pages/OverviewPage';
import DataDefinitionsPage from '@pages/DataDefinitionPage';
import ProcessAreasPage from '@pages/ProcessAreasPage';
import InventoryPage from '@pages/DataObjectsPage';
import SystemsPage from '@pages/SystemsPage';
import SystemConnectionsPage from '@pages/SystemConnectionsPage';
import ProjectsPage from '@pages/ProjectsPage';
import ReleasesPage from '@pages/ReleasesPage';
import LoginPage from '@pages/LoginPage';
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
          { path: 'process-areas', element: <ProcessAreasPage /> },
          { path: 'systems', element: <SystemsPage /> },
          { path: 'application-settings/connections', element: <SystemConnectionsPage /> },
          { path: 'data-objects', element: <InventoryPage /> },
          { path: 'project-settings/projects', element: <ProjectsPage /> },
          { path: 'project-settings/releases', element: <ReleasesPage /> }
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
