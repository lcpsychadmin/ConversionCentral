// Force rebuild - new version with grid fixes
import React from 'react';
import ReactDOM from 'react-dom/client';
import { CssBaseline, GlobalStyles, ThemeProvider } from '@mui/material';
import 'reactflow/dist/style.css';
import { QueryClient, QueryClientProvider } from 'react-query';

import App from './routes/AppRouter';
import theme from './theme/theme';
import { ToastProvider } from './components/common/ToastProvider';

const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <GlobalStyles
          styles={{
            "input[class^='ag-'][type='number']:not(.ag-number-field-input-stepper)": {
              appearance: 'textfield',
              MozAppearance: 'textfield'
            }
          }}
        />
        <CssBaseline />
        <ToastProvider>
          <App />
        </ToastProvider>
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
// force rebuild 2025-10-22 10:26:27
