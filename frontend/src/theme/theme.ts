import { Theme, createTheme } from '@mui/material/styles';

export type ThemeMode = 'light' | 'dark';

export const DEFAULT_THEME_MODE: ThemeMode = 'light';
export const DEFAULT_ACCENT_COLOR = '#1e88e5';

interface BuildThemeOptions {
  mode?: ThemeMode;
  accentColor?: string;
}

export const buildTheme = (options?: BuildThemeOptions): Theme => {
  const mode = options?.mode ?? DEFAULT_THEME_MODE;
  const accentColor = options?.accentColor ?? DEFAULT_ACCENT_COLOR;

  const isDark = mode === 'dark';

  return createTheme({
    palette: {
      mode,
      primary: {
        main: accentColor
      },
      secondary: {
        main: isDark ? '#ce93d8' : '#8e24aa'
      },
      background: isDark
        ? {
            default: '#111927',
            paper: '#1f2937'
          }
        : {
            default: '#f4f6f8',
            paper: '#ffffff'
          }
    },
    typography: {
      fontFamily: 'Roboto, Helvetica, Arial, sans-serif'
    },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          html: {
            textSizeAdjust: '100%'
          },
          body: {
            printColorAdjust: 'exact'
          }
        }
      },
      MuiButton: {
        styleOverrides: {
          root: {
            borderRadius: 8
          }
        }
      },
      MuiDialogTitle: {
        styleOverrides: {
          root: ({ theme }) => ({
            backgroundColor: theme.palette.primary.dark,
            color: theme.palette.common.white,
            padding: theme.spacing(2, 3),
            fontWeight: 600,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: theme.spacing(1.5),
            '& .MuiTypography-root': {
              fontWeight: 600
            },
            '& .MuiIconButton-root': {
              color: theme.palette.common.white
            }
          })
        }
      },
      MuiDialogContent: {
        styleOverrides: {
          root: ({ theme }) => ({
            padding: theme.spacing(3),
            '&:first-of-type': {
              paddingTop: theme.spacing(4)
            },
            '&.MuiDialogContent-dividers': {
              padding: theme.spacing(3),
              paddingTop: theme.spacing(4)
            }
          })
        }
      }
    }
  });
};

const theme = buildTheme();

export default theme;
