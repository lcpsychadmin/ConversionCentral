import { createTheme } from '@mui/material/styles';
const theme = createTheme({
    palette: {
        mode: 'light',
        primary: {
            main: '#1e88e5'
        },
        secondary: {
            main: '#8e24aa'
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
        }
    }
});
export default theme;
