import { alpha } from '@mui/material/styles';
/**
 * Returns consistent DataGrid styling matching the Data Definition page design
 */
export const getDataGridStyles = (theme) => ({
    '--DataGrid-rowBorderColor': alpha(theme.palette.divider, 0.22),
    '--DataGrid-columnSeparatorColor': alpha(theme.palette.divider, 0.22),
    border: `1px solid ${alpha(theme.palette.divider, 0.45)}`,
    borderRadius: 2,
    bgcolor: alpha(theme.palette.background.paper, 0.98),
    boxShadow: `0 6px 18px ${alpha(theme.palette.common.black, 0.05)}`,
    '& .MuiDataGrid-columnHeaders': {
        background: `linear-gradient(180deg, ${theme.palette.primary.main} 0%, ${alpha(theme.palette.primary.dark, 0.8)} 100%)`,
        borderBottom: `1px solid ${theme.palette.primary.dark}`,
        boxShadow: `inset 0 -2px 4px ${alpha(theme.palette.primary.dark, 0.35)}`,
        textTransform: 'uppercase',
        letterSpacing: 0.35,
        color: theme.palette.primary.contrastText
    },
    '& .MuiDataGrid-columnHeader, & .MuiDataGrid-cell': {
        outline: 'none'
    },
    '& .MuiDataGrid-columnHeaderTitle': {
        fontWeight: 700,
        fontSize: 12.5,
        color: theme.palette.primary.contrastText,
        textShadow: `0 1px 2px ${alpha(theme.palette.primary.dark, 0.4)}`
    },
    '& .MuiDataGrid-columnHeader .MuiSvgIcon-root': {
        color: theme.palette.primary.contrastText
    },
    '& .MuiDataGrid-cell': {
        display: 'flex',
        alignItems: 'center',
        borderBottom: `1px solid ${alpha(theme.palette.divider, 0.18)}`,
        borderRight: `1px solid ${alpha(theme.palette.divider, 0.16)}`,
        fontSize: 13.5,
        color: theme.palette.text.primary,
        padding: theme.spacing(0.75, 1.25),
        lineHeight: 1.45,
        transition: theme.transitions.create(['background-color', 'box-shadow'], {
            duration: theme.transitions.duration.shortest
        })
    },
    '& .MuiDataGrid-row': {
        transition: theme.transitions.create(['background-color', 'box-shadow'], {
            duration: theme.transitions.duration.shortest
        }),
        '&:hover': {
            backgroundColor: alpha(theme.palette.primary.main, 0.06),
            boxShadow: `0 0 0 1px ${alpha(theme.palette.primary.main, 0.15)}`
        },
        '&.Mui-selected': {
            backgroundColor: alpha(theme.palette.primary.main, 0.1),
            boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
            '&:hover': {
                backgroundColor: alpha(theme.palette.primary.main, 0.14),
                boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.25)}`
            }
        }
    },
    '& .MuiDataGrid-row:nth-of-type(even) .MuiDataGrid-cell': {
        backgroundColor: alpha(theme.palette.action.hover, 0.12)
    },
    '& .MuiDataGrid-row:focus': {
        boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.primary.main, 0.45)}`,
        transition: theme.transitions.create(['box-shadow', 'background-color'], { duration: 200 })
    },
    '& .MuiDataGrid-row:focus-visible': {
        boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.25)}, inset 0 0 0 2px ${theme.palette.primary.main}`,
        backgroundColor: alpha(theme.palette.primary.main, 0.02)
    },
    '& .MuiDataGrid-cell.MuiDataGrid-cell--editing': {
        backgroundColor: alpha(theme.palette.common.white, 0.98),
        boxShadow: `inset 0 0 0 2px ${alpha(theme.palette.primary.main, 0.45)}`
    }
});
