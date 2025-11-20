import { AppBar, Box, Collapse, Drawer, IconButton, List, ListItem, ListItemButton, ListItemText, Toolbar, Typography, ListItemIcon, Dialog, DialogTitle, DialogContent, DialogActions, Button } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import MenuIcon from '@mui/icons-material/Menu';
import { useState, useEffect, useCallback } from 'react';
import { useQuery } from 'react-query';
import { Link, Outlet, useNavigate } from 'react-router-dom';
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
import FactCheckIcon from '@mui/icons-material/FactCheck';

import { useAuth } from '../context/AuthContext';
import {
  COMPANY_SETTINGS_QUERY_KEY,
  fetchCompanySettings
} from '../services/applicationSettingsService';

import { ReactNode } from 'react';
import useDesignerStore from '../stores/designerStore';

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
      { label: 'Company Settings', path: '/application-settings/company' }
    ]
  },
  {
    label: 'Data Connections',
    collapsible: true,
    icon: <TuneIcon />,
    children: [
      { label: 'Application Database', path: '/data-configuration/application-database' },
      { label: 'Data Warehouse', path: '/data-configuration/data-warehouse' },
      { label: 'Source Systems', path: '/data-configuration/source-systems' },
      { label: 'Ingestion Schedules', path: '/data-configuration/ingestion-schedules' }
    ]
  },
  {
    label: 'Data Configuration',
    collapsible: true,
    icon: <DataObjectIcon />,
    children: [
      {
        label: 'Data Definition Settings',
        collapsible: true,
        children: [
          { label: 'Legal Requirements', path: '/application-settings/legal-requirements' },
          { label: 'Security Classifications', path: '/application-settings/security-classifications' }
        ]
      },
      { label: 'Product Teams', path: '/process-areas' },
      { label: 'Data Objects', path: '/data-objects' },
      { label: 'Data Object Definition', path: '/data-definitions' }
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
    label: 'Data Quality',
    collapsible: true,
    icon: <FactCheckIcon />,
    children: [
      { label: 'Overview', path: '/data-quality' },
      { label: 'Datasets', path: '/data-quality/datasets' },
      { label: 'Profiling Runs', path: '/data-quality/profiling-runs' },
      { label: 'Test Suites', path: '/data-quality/test-suites' },
      { label: 'Test Library', path: '/data-quality/test-library' },
      { label: 'Run History & Alerts', path: '/data-quality/run-history' },
      { label: 'Settings', path: '/data-quality/settings' }
    ]
  },
  // Project settings will be moved under Data Migration (see below)
  {
    label: 'Reporting',
    collapsible: true,
    icon: <AssessmentIcon />,
    children: [
      { label: 'Report Designer', path: '/reporting/designer' },
      { label: 'Report Catalog', path: '/reporting/catalog' }
    ]
  }
  ,
  {
    label: 'Data Migration',
    collapsible: true,
    icon: <ProjectsIcon />,
    children: [
      {
        label: 'Project Settings',
        collapsible: true,
        children: [
          { label: 'Projects', path: '/project-settings/projects' },
          { label: 'Releases', path: '/project-settings/releases' }
        ]
      }
    ]
  }
];

type NavigationSentinelWindow = Window & {
  __CC_SUPPRESS_BEFOREUNLOAD?: boolean;
};

const MainLayout = () => {
  const theme = useTheme();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [pendingHref, setPendingHref] = useState<string | null>(null);
  const hasUnsaved = useDesignerStore((s) => s.hasUnsaved);
  const setHasUnsaved = useDesignerStore((s) => s.setHasUnsaved);

  // warn on full unload if there are unsaved changes
  useEffect(() => {
    const onBeforeUnload = (event: BeforeUnloadEvent) => {
      // If we've intentionally suppressed beforeunload (e.g., user confirmed
      // navigation via the in-app modal), allow the unload to proceed without
      // showing the browser default popup.
      try {
        const sentinelWindow = window as NavigationSentinelWindow;
        if (sentinelWindow.__CC_SUPPRESS_BEFOREUNLOAD) {
          return;
        }
      } catch {
        // ignore
      }
      if (!hasUnsaved) {
        return;
      }
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', onBeforeUnload);
    return () => window.removeEventListener('beforeunload', onBeforeUnload);
  }, [hasUnsaved]);
  // Create expand/collapse state for any collapsible section at any depth.
  const buildCollapseMap = (items: NavItem[], parentKey?: string) =>
    items.reduce<Record<string, boolean>>((acc, it) => {
      const key = parentKey ? `${parentKey}::${it.label}` : it.label;
      if (it.collapsible) acc[key] = false;
      if (it.children) {
        Object.assign(acc, buildCollapseMap(it.children, key));
      }
      return acc;
    }, {});

  const [openSections, setOpenSections] = useState<Record<string, boolean>>(() =>
    buildCollapseMap(navItems)
  );
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const isDarkMode = theme.palette.mode === 'dark';
  const navTextColor = isDarkMode ? theme.palette.common.white : theme.palette.text.primary;
  const navHeadingColor = isDarkMode ? theme.palette.common.white : theme.palette.primary.dark;
  const navHoverBackground = alpha(theme.palette.primary.main, isDarkMode ? 0.18 : 0.12);
  const navIconColor = theme.palette.primary.main;

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

  const handleSectionToggle = (key: string) => {
    setOpenSections((prev) => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  const renderNestedChildren = (items: NavItem[], parentKey?: string) => (
    <List component="div" disablePadding>
      {items.map((item) => {
        const key = parentKey ? `${parentKey}::${item.label}` : item.label;
        if (item.collapsible) {
          const isOpen = Boolean(openSections[key]);
          return (
            <Box key={key}>
              <ListItem disablePadding>
                  <ListItemButton
                    onClick={() => handleSectionToggle(key)}
                  sx={{
                    pl: 4,
                    color: navHeadingColor,
                    '&:hover': {
                      backgroundColor: navHoverBackground
                    }
                  }}
                >
                  <ListItemText
                    primary={item.label}
                    primaryTypographyProps={{ sx: { color: navHeadingColor } }}
                  />
                  {isOpen ? (
                    <ExpandLessIcon sx={{ fontSize: 22, color: navIconColor }} />
                  ) : (
                    <ExpandMoreIcon sx={{ fontSize: 22, color: navIconColor }} />
                  )}
                </ListItemButton>
              </ListItem>
              <Collapse in={isOpen} timeout="auto" unmountOnExit>
                {item.children ? renderNestedChildren(item.children, key) : null}
              </Collapse>
            </Box>
          );
        }

        return (
          <ListItem key={key} disablePadding>
            <ListItemButton
              component={Link}
              to={item.path ?? '#'}
              sx={{
                pl: 6,
                color: navTextColor,
                '&:hover': {
                  backgroundColor: navHoverBackground
                }
              }}
              disabled={!item.path}
                onClick={(ev) => {
                  if (!item.path) return;
                  if (ev.button !== 0 || ev.shiftKey || ev.ctrlKey || ev.metaKey || ev.altKey) return;
                  // DEBUG: log whether the event was already defaultPrevented before we act
                  // eslint-disable-next-line no-console
                  console.debug('[nav] pre-handler defaultPrevented', { before: !!ev.defaultPrevented, href: item.path });
                  if (hasUnsaved) {
                    ev.preventDefault();
                    setPendingHref(item.path);
                    setConfirmOpen(true);
                    return;
                  }
                  setMobileOpen(false);
                }}
            >
              <ListItemText
                primary={item.label}
                primaryTypographyProps={{
                  sx: {
                    color: navTextColor,
                    fontWeight: 500
                  }
                }}
              />
            </ListItemButton>
          </ListItem>
        );
      })}
    </List>
  );

  // Confirmation dialog shown when user attempts to navigate away with unsaved changes
  const handleConfirmLeave = useCallback(() => {
    const href = pendingHref;
    if (href) {
      try {
        // Clear flag and close dialog synchronously to avoid re-triggering
        setHasUnsaved(false);
        setConfirmOpen(false);
        setPendingHref(null);
        setMobileOpen(false);
        // Log and navigate on next tick so any current event handlers finish.
        // eslint-disable-next-line no-console
        console.debug('[nav][confirm] navigating to', href);
        // Aggressive navigation: set a sentinel to suppress our beforeunload
        // handler, patch Event behavior so other beforeunload listeners cannot
        // block the unload, perform SPA navigate and then force a full reload.
        // Restore patched behavior afterwards.
        try {
          // set sentinel so beforeunload handlers respect the in-app confirm
          // while we perform SPA navigation.
          (window as NavigationSentinelWindow).__CC_SUPPRESS_BEFOREUNLOAD = true;
        } catch {
          // ignore
        }
        setTimeout(() => {
          try {
            navigate(href);
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error('[nav][confirm] navigate failed', e);
          } finally {
            try {
              // clear sentinel after navigation attempt
              delete (window as NavigationSentinelWindow).__CC_SUPPRESS_BEFOREUNLOAD;
            } catch {
              // ignore
            }
          }
        }, 0);
      } catch (e) {
        // ignore
      }
    } else {
      setConfirmOpen(false);
    }
  }, [navigate, pendingHref, setHasUnsaved]);

  const handleCancelLeave = useCallback(() => {
    setConfirmOpen(false);
    setPendingHref(null);
  }, []);


  const drawer = (
    <Box
      sx={{
        backgroundColor: alpha(theme.palette.primary.main, 0.08),
        flexGrow: 1,
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        minHeight: '100vh'
      }}
    >
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
                      color: navHeadingColor,
                      fontWeight: 600,
                      '&:hover': {
                        backgroundColor: navHoverBackground
                      }
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: navIconColor,
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
                        letterSpacing: 0.3,
                        sx: { color: navHeadingColor }
                      }}
                    />
                    {isOpen ? (
                      <ExpandLessIcon sx={{ fontSize: 24, color: navIconColor }} />
                    ) : (
                      <ExpandMoreIcon sx={{ fontSize: 24, color: navIconColor }} />
                    )}
                  </ListItemButton>
                ) : item.path ? (
                  <ListItemButton
                    component={Link}
                    to={item.path}
                    onClick={() => setMobileOpen(false)}
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      color: navTextColor,
                      '&:hover': {
                        backgroundColor: navHoverBackground
                      }
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: navIconColor,
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
                        sx: { color: navTextColor }
                      }}
                    />
                  </ListItemButton>
                ) : (
                  <ListItemButton
                    disabled
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      color: navHeadingColor,
                      fontWeight: 600
                    }}
                  >
                    {item.icon && (
                      <ListItemIcon
                        sx={{
                          color: navIconColor,
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
                        sx: { color: navHeadingColor }
                      }}
                    />
                  </ListItemButton>
                )}
              </ListItem>
              {hasChildren && (
                isCollapsible ? (
                  <Collapse in={isOpen} timeout="auto" unmountOnExit>
                    {renderNestedChildren(item.children!, item.label)}
                  </Collapse>
                ) : (
                  renderNestedChildren(item.children!, item.label)
                )
              )}
            </Box>
          );
        })}
      </List>
    </Box>
  );

  // Confirmation dialog UI
  const confirmDialog = (
    <Dialog open={confirmOpen} onClose={handleCancelLeave} aria-labelledby="unsaved-dialog-title">
      <DialogTitle id="unsaved-dialog-title">Unsaved changes</DialogTitle>
      <DialogContent>
        You have unsaved changes in the Report Designer. Leaving this page will discard those changes. Continue?
      </DialogContent>
      <DialogActions>
  <Button onClick={handleCancelLeave}>Cancel</Button>
  <Button onClick={handleConfirmLeave} color="error">Leave</Button>
      </DialogActions>
    </Dialog>
  );

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      {confirmDialog}
      <AppBar
        position="fixed"
        sx={{
          background: `linear-gradient(180deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
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
                  objectFit: 'contain',
                  display: 'block',
                  backgroundColor: 'transparent'
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
              backgroundColor: 'transparent',
              borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
              boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`,
              display: 'flex'
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
              backgroundColor: 'transparent',
              borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
              boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`,
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              minHeight: '100%'
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
