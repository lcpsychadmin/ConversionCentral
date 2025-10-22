import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { AppBar, Box, Collapse, Drawer, IconButton, List, ListItem, ListItemButton, ListItemText, Toolbar, Typography, ListItemIcon } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import MenuIcon from '@mui/icons-material/Menu';
import { useState } from 'react';
import { Link, Outlet } from 'react-router-dom';
import LogoutIcon from '@mui/icons-material/Logout';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DashboardIcon from '@mui/icons-material/Dashboard';
import SettingsIcon from '@mui/icons-material/Settings';
import ProjectsIcon from '@mui/icons-material/FolderOpen';
import DataObjectIcon from '@mui/icons-material/Storage';
import { useAuth } from '../context/AuthContext';
const drawerWidth = 280;
const navItems = [
    { label: 'Overview', path: '/', icon: _jsx(DashboardIcon, {}) },
    {
        label: 'Application Settings',
        collapsible: true,
        icon: _jsx(SettingsIcon, {}),
        children: [
            { label: 'Systems', path: '/systems' },
            { label: 'Connections', path: '/application-settings/connections' },
            { label: 'Process Areas', path: '/process-areas' }
        ]
    },
    {
        label: 'Project Settings',
        collapsible: true,
        icon: _jsx(ProjectsIcon, {}),
        children: [
            { label: 'Projects', path: '/project-settings/projects' },
            { label: 'Releases', path: '/project-settings/releases' }
        ]
    },
    {
        label: 'Data Objects',
        collapsible: true,
        icon: _jsx(DataObjectIcon, {}),
        children: [
            { label: 'Inventory', path: '/data-objects' },
            { label: 'Data Definitions', path: '/data-definitions' },
            { label: 'Data Construction', path: '/data-construction' }
        ]
    }
];
const MainLayout = () => {
    const theme = useTheme();
    const [mobileOpen, setMobileOpen] = useState(false);
    const [openSections, setOpenSections] = useState(() => navItems
        .filter((item) => item.collapsible)
        .reduce((acc, item) => {
        acc[item.label] = false;
        return acc;
    }, {}));
    const { user, logout } = useAuth();
    const handleDrawerToggle = () => {
        setMobileOpen((prev) => !prev);
    };
    const handleSectionToggle = (label) => {
        setOpenSections((prev) => ({
            ...prev,
            [label]: !prev[label]
        }));
    };
    const renderChildItems = (children) => (_jsx(List, { component: "div", disablePadding: true, children: children.map((child) => (_jsx(ListItem, { disablePadding: true, children: _jsx(ListItemButton, { component: Link, to: child.path ?? '#', sx: { pl: 4 }, disabled: !child.path, onClick: () => {
                    if (child.path) {
                        setMobileOpen(false);
                    }
                }, children: _jsx(ListItemText, { primary: child.label }) }) }, child.label))) }));
    const drawer = (_jsxs("div", { style: { backgroundColor: alpha(theme.palette.primary.main, 0.08), height: '100%' }, children: [_jsx(Toolbar, {}), _jsx(List, { sx: { px: 1 }, children: navItems.map((item) => {
                    const hasChildren = Boolean(item.children?.length);
                    const isCollapsible = hasChildren && item.collapsible;
                    const isOpen = isCollapsible ? openSections[item.label] : true;
                    return (_jsxs(Box, { sx: { width: '100%' }, children: [_jsx(ListItem, { disablePadding: true, children: isCollapsible ? (_jsxs(ListItemButton, { onClick: () => handleSectionToggle(item.label), sx: {
                                        borderRadius: 1,
                                        mb: 0.5,
                                        color: theme.palette.primary.dark,
                                        fontWeight: 600,
                                        '&:hover': {
                                            backgroundColor: alpha(theme.palette.primary.main, 0.12)
                                        }
                                    }, children: [item.icon && (_jsx(ListItemIcon, { sx: {
                                                color: 'inherit',
                                                minWidth: 40,
                                                '& svg': { fontSize: 24 }
                                            }, children: item.icon })), _jsx(ListItemText, { primary: item.label, primaryTypographyProps: {
                                                fontSize: 16,
                                                fontWeight: 600,
                                                letterSpacing: 0.3
                                            } }), isOpen ? _jsx(ExpandLessIcon, { sx: { fontSize: 24 } }) : _jsx(ExpandMoreIcon, { sx: { fontSize: 24 } })] })) : item.path ? (_jsxs(ListItemButton, { component: Link, to: item.path, onClick: () => setMobileOpen(false), sx: {
                                        borderRadius: 1,
                                        mb: 0.5,
                                        color: theme.palette.text.primary,
                                        '&:hover': {
                                            backgroundColor: alpha(theme.palette.primary.main, 0.1)
                                        }
                                    }, children: [item.icon && (_jsx(ListItemIcon, { sx: {
                                                color: 'inherit',
                                                minWidth: 40,
                                                '& svg': { fontSize: 24 }
                                            }, children: item.icon })), _jsx(ListItemText, { primary: item.label, primaryTypographyProps: {
                                                fontSize: 16,
                                                fontWeight: 600
                                            } })] })) : (_jsxs(ListItemButton, { disabled: true, sx: {
                                        borderRadius: 1,
                                        mb: 0.5,
                                        color: theme.palette.primary.dark,
                                        fontWeight: 600
                                    }, children: [item.icon && (_jsx(ListItemIcon, { sx: {
                                                color: 'inherit',
                                                minWidth: 40,
                                                '& svg': { fontSize: 24 }
                                            }, children: item.icon })), _jsx(ListItemText, { primary: item.label, primaryTypographyProps: {
                                                fontSize: 16,
                                                fontWeight: 600
                                            } })] })) }), hasChildren && (isCollapsible ? (_jsx(Collapse, { in: isOpen, timeout: "auto", unmountOnExit: true, children: renderChildItems(item.children) })) : (renderChildItems(item.children)))] }, item.label));
                }) })] }));
    return (_jsxs(Box, { sx: { display: 'flex' }, children: [_jsx(AppBar, { position: "fixed", sx: {
                    background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
                    boxShadow: `0 4px 20px ${alpha(theme.palette.primary.main, 0.3)}`,
                    borderBottom: `3px solid ${alpha(theme.palette.primary.light, 0.4)}`,
                    color: theme.palette.primary.contrastText
                }, children: _jsxs(Toolbar, { sx: { py: 1.5 }, children: [_jsx(IconButton, { color: "inherit", "aria-label": "open drawer", edge: "start", onClick: handleDrawerToggle, sx: {
                                mr: 2,
                                [theme.breakpoints.up('lg')]: {
                                    display: 'none'
                                }
                            }, children: _jsx(MenuIcon, {}) }), _jsx(Typography, { variant: "h6", noWrap: true, component: "div", sx: {
                                flexGrow: 1,
                                fontWeight: 800,
                                fontSize: '1.4rem',
                                letterSpacing: 0.5,
                                color: theme.palette.common.white,
                                textShadow: `0 2px 4px ${alpha(theme.palette.common.black, 0.2)}`
                            }, children: "Conversion Central" }), user && (_jsx(IconButton, { color: "inherit", onClick: logout, sx: { ml: 1 }, children: _jsx(LogoutIcon, {}) }))] }) }), _jsxs(Box, { component: "nav", sx: { flexShrink: 0 }, "aria-label": "navigation", children: [_jsx(Drawer, { variant: "temporary", open: mobileOpen, onClose: handleDrawerToggle, ModalProps: {
                            keepMounted: true,
                            BackdropProps: {
                                sx: {
                                    backdropFilter: 'blur(4px)',
                                    backgroundColor: alpha(theme.palette.common.black, 0.75)
                                }
                            }
                        }, sx: {
                            display: 'block',
                            [theme.breakpoints.up('lg')]: {
                                display: 'none'
                            },
                            '& .MuiDrawer-paper': {
                                boxSizing: 'border-box',
                                width: drawerWidth,
                                backgroundColor: alpha(theme.palette.background.default, 0.96),
                                borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                                boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`
                            }
                        }, children: drawer }), _jsx(Drawer, { variant: "permanent", sx: {
                            display: 'none',
                            [theme.breakpoints.up('lg')]: {
                                display: 'block'
                            },
                            width: drawerWidth,
                            flexShrink: 0,
                            '& .MuiDrawer-paper': {
                                boxSizing: 'border-box',
                                width: drawerWidth,
                                backgroundColor: alpha(theme.palette.background.default, 0.96),
                                borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                                boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`,
                                position: 'fixed',
                                height: '100vh',
                                top: 0,
                                left: 0
                            }
                        }, children: drawer })] }), _jsxs(Box, { component: "main", sx: {
                    flexGrow: 1,
                    p: 3,
                    width: '100%',
                    minWidth: 0
                }, children: [_jsx(Toolbar, {}), _jsx(Outlet, {})] })] }));
};
export default MainLayout;
