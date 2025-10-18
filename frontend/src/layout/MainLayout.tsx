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
      { label: 'Systems', path: '/systems' },
      { label: 'Connections', path: '/application-settings/connections' },
      { label: 'Process Areas', path: '/process-areas' }
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
    label: 'Data Objects',
    collapsible: true,
    icon: <DataObjectIcon />,
    children: [
      { label: 'Inventory', path: '/data-objects' },
      { label: 'Data Definitions', path: '/data-definitions' }
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
          width: { sm: `calc(100% - ${drawerWidth}px)` },
          ml: { sm: `${drawerWidth}px` },
          background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${alpha(theme.palette.primary.main, 0.95)} 100%)`,
          boxShadow: `0 4px 20px ${alpha(theme.palette.primary.main, 0.3)}`,
          borderBottom: `3px solid ${alpha(theme.palette.primary.light, 0.4)}`
        }}
      >
        <Toolbar sx={{ py: 1.5 }}>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            edge="start"
            onClick={handleDrawerToggle}
            sx={{ mr: 2, display: { sm: 'none' } }}
          >
            <MenuIcon />
          </IconButton>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{
              flexGrow: 1,
              fontWeight: 800,
              fontSize: '1.4rem',
              letterSpacing: 0.5,
              textShadow: `0 2px 4px ${alpha(theme.palette.common.black, 0.2)}`
            }}
          >
            Conversion Central
          </Typography>
          {user && (
            <IconButton color="inherit" onClick={logout} sx={{ ml: 1 }}>
              <LogoutIcon />
            </IconButton>
          )}
        </Toolbar>
      </AppBar>
      <Box
        component="nav"
        sx={{ width: { sm: drawerWidth }, flexShrink: { sm: 0 } }}
        aria-label="navigation"
      >
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{ keepMounted: true }}
          sx={{
            display: { xs: 'block', sm: 'none' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: drawerWidth,
              backgroundColor: alpha(theme.palette.primary.main, 0.08),
              borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
              boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`
            }
          }}
        >
          {drawer}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: 'none', sm: 'block' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: drawerWidth,
              backgroundColor: alpha(theme.palette.primary.main, 0.08),
              borderRight: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
              boxShadow: `2px 0 12px ${alpha(theme.palette.primary.main, 0.1)}`
            }
          }}
          open
        >
          {drawer}
        </Drawer>
      </Box>
      <Box
        component="main"
        sx={{ flexGrow: 1, p: 3, width: { sm: `calc(100% - ${drawerWidth}px)` } }}
      >
        <Toolbar />
        <Outlet />
      </Box>
    </Box>
  );
};

export default MainLayout;
