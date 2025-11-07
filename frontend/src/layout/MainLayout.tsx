import { AppBar, Box, Collapse, Drawer, IconButton, List, ListItem, ListItemButton, ListItemText, Toolbar, Typography, ListItemIcon } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import MenuIcon from '@mui/icons-material/Menu';
import { useState } from 'react';
import { useQuery } from 'react-query';
import { Link, Outlet } from 'react-router-dom';
import LogoutIcon from '@mui/icons-material/Logout';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DashboardIcon from '@mui/icons-material/Dashboard';
import SettingsIcon from '@mui/icons-material/Settings';
import ProjectsIcon from '@mui/icons-material/FolderOpen';
import DataObjectIcon from '@mui/icons-material/Storage';
import AssessmentIcon from '@mui/icons-material/Assessment';
import TuneIcon from '@mui/icons-material/Tune';
import ManageAccountsIcon from '@mui/icons-material/ManageAccounts';

import { useAuth } from '../context/AuthContext';
import {
  COMPANY_SETTINGS_QUERY_KEY,
  fetchCompanySettings
} from '../services/applicationSettingsService';

import { ReactNode } from 'react';

const drawerWidth = 280;

interface NavItem {
  label: string;
  path?: string;
  collapsible?: boolean;
  children?: NavItem[];
  icon?: ReactNode;
}

const navItems: NavItem[] = [
  { label: 'Overview', path: '/', icon: <DashboardIcon /> },
  {
    label: 'Application Settings',
    collapsible: true,
    icon: <SettingsIcon />,
    children: [
      { label: 'Applications', path: '/applications' },
      { label: 'Company Settings', path: '/application-settings/company' },
      { label: 'Product Teams', path: '/process-areas' }
    ]
  },
  {
    label: 'Data Connections',
    collapsible: true,
    icon: <TuneIcon />,
    children: [
      { label: 'Source Catalog', path: '/data-configuration/source-catalog' },
      { label: 'Ingestion Schedules', path: '/data-configuration/ingestion-schedules' },
      { label: 'Application Database', path: '/data-configuration/application-database' },
      { label: 'Data Warehouse', path: '/data-configuration/data-warehouse' }
    ]
  },
  {
    label: 'Data Configuration',
    collapsible: true,
    icon: <DataObjectIcon />,
    children: [
      { label: 'Data Objects', path: '/data-objects' },
      { label: 'Data Object Definition', path: '/data-definitions' }
    ]
  },
  {
    label: 'Project Settings',
    collapsible: true,
    icon: <ProjectsIcon />,
    children: [
      { label: 'Projects', path: '/project-settings/projects' },
      { label: 'Releases', path: '/project-settings/releases' }
    ]
  },
  {
    label: 'Data Management',
    collapsible: true,
    icon: <ManageAccountsIcon />,
    children: [
      { label: 'Upload Data', path: '/data-configuration/upload-data' },
      { label: 'Manage Data', path: '/data-construction' }
    ]
  },
  {
    label: 'Reporting',
    collapsible: true,
    icon: <AssessmentIcon />,
    children: [
      { label: 'Report Designer', path: '/reporting/designer' },
      { label: 'Reports & Outputs', path: '/reporting/catalog' }
    ]
  }
];

const MainLayout = () => {
  const theme = useTheme();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [openSections, setOpenSections] = useState<Record<string, boolean>>(() =>
    navItems
      .filter((item) => item.collapsible)
      .reduce<Record<string, boolean>>((acc, item) => {
        acc[item.label] = false;
        return acc;
      }, {})
  );
  const { user, logout } = useAuth();

  const { data: companySettings } = useQuery(
    COMPANY_SETTINGS_QUERY_KEY,
    fetchCompanySettings,
    {
      staleTime: 5 * 60 * 1000
    }
  );

  const siteTitle = companySettings?.siteTitle?.trim() || 'Conversion Central';
  const logoDataUrl = companySettings?.logoDataUrl ?? null;

  const handleDrawerToggle = () => {
    setMobileOpen((prev) => !prev);
  };

  const handleSectionToggle = (label: string) => {
    setOpenSections((prev) => ({
      ...prev,
      [label]: !prev[label]
    }));
  };

  const renderChildItems = (children: NavItem[]) => (
    <List component="div" disablePadding>
      {children.map((child) => (
        <ListItem key={child.label} disablePadding>
          <ListItemButton
            component={Link}
            to={child.path ?? '#'}
            sx={{ pl: 4 }}
            disabled={!child.path}
            onClick={() => {
              if (child.path) {
                setMobileOpen(false);
              }
            }}
          >
            <ListItemText primary={child.label} />
          </ListItemButton>
        </ListItem>
      ))}
    </List>
  );

  const drawer = (
    <div style={{ backgroundColor: alpha(theme.palette.primary.main, 0.08), height: '100%' }}>
      <Toolbar />
      <List sx={{ px: 1 }}>
        {navItems.map((item) => {
          const hasChildren = Boolean(item.children?.length);
          const isCollapsible = hasChildren && item.collapsible;
          const isOpen = isCollapsible ? openSections[item.label] : true;

          return (
            <Box key={item.label} sx={{ width: '100%' }}>
              <ListItem disablePadding>
                {isCollapsible ? (
                  <ListItemButton
                    onClick={() => handleSectionToggle(item.label)}
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      color: theme.palette.primary.dark,
                      fontWeight: 600,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.12)
                      }
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: 'inherit',
                          minWidth: 40,
                          '& svg': { fontSize: 24 }
                        }}
                      >
                        {item.icon}
                      </ListItemIcon>
                    )}
                    <ListItemText
                      primary={item.label}
                      primaryTypographyProps={{
                        fontSize: 16,
                        fontWeight: 600,
                        letterSpacing: 0.3
                      }}
                    />
                    {isOpen ? <ExpandLessIcon sx={{ fontSize: 24 }} /> : <ExpandMoreIcon sx={{ fontSize: 24 }} />}
                  </ListItemButton>
                ) : item.path ? (
                  <ListItemButton
                    component={Link}
                    to={item.path}
                    onClick={() => setMobileOpen(false)}
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      color: theme.palette.text.primary,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.1)
                      }
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: 'inherit',
                          minWidth: 40,
                          '& svg': { fontSize: 24 }
                        }}
                      >
                        {item.icon}
                      </ListItemIcon>
                    )}
                    <ListItemText
                      primary={item.label}
                      primaryTypographyProps={{
                        fontSize: 16,
                        fontWeight: 600
                      }}
                    />
                  </ListItemButton>
                ) : (
                  <ListItemButton
                    disabled
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      color: theme.palette.primary.dark,
                      fontWeight: 600
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: 'inherit',
                          minWidth: 40,
                          '& svg': { fontSize: 24 }
                        }}
                      >
                        {item.icon}
                      </ListItemIcon>
                    )}
                    <ListItemText
                      primary={item.label}
                      primaryTypographyProps={{
                        fontSize: 16,
                        fontWeight: 600
                      }}
                    />
                  </ListItemButton>
                )}
              </ListItem>
              {hasChildren && (
                isCollapsible ? (
                  <Collapse in={isOpen} timeout="auto" unmountOnExit>
                    {renderChildItems(item.children!)}
                  </Collapse>
                ) : (
                  renderChildItems(item.children!)
                )
              )}
            </Box>
          );
        })}
      </List>
    </div>
  );

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar
        position="fixed"
        sx={{
          background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
          boxShadow: `0 4px 20px ${alpha(theme.palette.primary.main, 0.3)}`,
          borderBottom: `3px solid ${alpha(theme.palette.primary.light, 0.4)}`,
          color: theme.palette.primary.contrastText,
          width: { lg: `calc(100% - ${drawerWidth}px)` },
          ml: { lg: `${drawerWidth}px` }
        }}
      >
        <Toolbar sx={{ py: 1.5 }}>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            edge="start"
            onClick={handleDrawerToggle}
            sx={{
              mr: 2,
              [theme.breakpoints.up('lg')]: {
                display: 'none'
              }
            }}
          >
            <MenuIcon />
          </IconButton>
          <Box
            sx={{
              flexGrow: 1,
              display: 'flex',
              alignItems: 'center',
              gap: 1.5,
              minWidth: 0
            }}
          >
            {logoDataUrl && (
              <Box
                component="img"
                src={logoDataUrl}
                alt={`${siteTitle} logo`}
                sx={{
                  maxHeight: 40,
                  width: 'auto',
                  objectFit: 'contain'
                }}
              />
            )}
            <Typography
              variant="h6"
              noWrap
              component="div"
              sx={{
                fontWeight: 800,
                fontSize: '1.4rem',
                letterSpacing: 0.5,
                color: theme.palette.common.white,
                textShadow: `0 2px 4px ${alpha(theme.palette.common.black, 0.2)}`,
                minWidth: 0,
                whiteSpace: 'nowrap',
                overflow: 'hidden',
                textOverflow: 'ellipsis'
              }}
            >
              {siteTitle}
            </Typography>
          </Box>
          {user && (
            <IconButton color="inherit" onClick={logout} sx={{ ml: 1 }}>
              <LogoutIcon />
            </IconButton>
          )}
        </Toolbar>
      </AppBar>
      <Box component="nav" sx={{ flexShrink: 0 }} aria-label="navigation">
        {/* Mobile drawer - only on small screens */}
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{
            keepMounted: true,
            BackdropProps: {
              sx: {
                backdropFilter: 'blur(4px)',
                backgroundColor: alpha(theme.palette.common.black, 0.75)
              }
            }
          }}
          sx={{
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
          }}
        >
          {drawer}
        </Drawer>
        {/* Permanent drawer - only on large screens */}
        <Drawer
          variant="permanent"
          sx={{
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
              position: 'sticky',
              height: '100vh',
              top: 0
            }
          }}
        >
          {drawer}
        </Drawer>
      </Box>
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          pl: { xs: 1.5, sm: 2, lg: 2.5, xl: 3 },
          pr: { xs: 1.5, sm: 2, lg: 2.5, xl: 3 },
          pt: { xs: 2, sm: 3 },
          pb: { xs: 3, sm: 4 },
          width: '100%',
          minWidth: 0
        }}
      >
        <Toolbar />
        <Outlet />
      </Box>
    </Box>
  );
};

export default MainLayout;
