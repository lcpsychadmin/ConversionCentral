import { jsx as _jsx } from "react/jsx-runtime";
import { Navigate, Outlet, RouterProvider, createBrowserRouter } from 'react-router-dom';
import { CircularProgress, Box } from '@mui/material';
import { Suspense } from 'react';
import MainLayout from '../layout/MainLayout';
import OverviewPage from '@pages/OverviewPage';
import DataDefinitionsPage from '@pages/DataDefinitionPage';
import DataConstructionPage from '@pages/DataConstructionPage';
import ProcessAreasPage from '@pages/ProcessAreasPage';
import InventoryPage from '@pages/DataObjectsPage';
import SystemsPage from '@pages/SystemsPage';
import SystemConnectionsPage from '@pages/SystemConnectionsPage';
import ProjectsPage from '@pages/ProjectsPage';
import ReleasesPage from '@pages/ReleasesPage';
import LoginPage from '@pages/LoginPage';
import ReportingDesignerPage from '@pages/ReportingDesignerPage';
import ReportingCatalogPage from '@pages/ReportingCatalogPage';
import { ProtectedRoute } from '@routes/guards/ProtectedRoute';
import { AuthProvider } from '@context/AuthContext';
const Loader = () => (_jsx(Box, { display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", children: _jsx(CircularProgress, {}) }));
const AuthBoundary = () => (_jsx(AuthProvider, { children: _jsx(Outlet, {}) }));
const router = createBrowserRouter([
    {
        element: _jsx(AuthBoundary, {}),
        children: [
            {
                path: '/login',
                element: _jsx(LoginPage, {})
            },
            {
                path: '/',
                element: (_jsx(ProtectedRoute, { children: _jsx(MainLayout, {}) })),
                children: [
                    { index: true, element: _jsx(OverviewPage, {}) },
                    { path: 'data-definitions', element: _jsx(DataDefinitionsPage, {}) },
                    { path: 'data-definition', element: _jsx(Navigate, { to: "/data-definitions", replace: true }) },
                    { path: 'data-construction', element: _jsx(DataConstructionPage, {}) },
                    { path: 'process-areas', element: _jsx(ProcessAreasPage, {}) },
                    { path: 'systems', element: _jsx(SystemsPage, {}) },
                    { path: 'application-settings/connections', element: _jsx(SystemConnectionsPage, {}) },
                    { path: 'data-objects', element: _jsx(InventoryPage, {}) },
                    { path: 'project-settings/projects', element: _jsx(ProjectsPage, {}) },
                    { path: 'project-settings/releases', element: _jsx(ReleasesPage, {}) },
                    { path: 'reporting/designer', element: _jsx(ReportingDesignerPage, {}) },
                    { path: 'reporting/catalog', element: _jsx(ReportingCatalogPage, {}) }
                ]
            },
            {
                path: '*',
                element: _jsx(Navigate, { to: "/", replace: true })
            }
        ]
    }
]);
const AppRouter = () => (_jsx(Suspense, { fallback: _jsx(Loader, {}), children: _jsx(RouterProvider, { router: router, fallbackElement: _jsx(Loader, {}), future: { v7_startTransition: true, v7_relativeSplatPath: true } }) }));
export default AppRouter;
