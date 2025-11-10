// Force rebuild - new version with grid fixes
import React from 'react';
import ReactDOM from 'react-dom/client';
import 'reactflow/dist/style.css';
import { QueryClient, QueryClientProvider } from 'react-query';

import App from './routes/AppRouter';
import { ToastProvider } from './components/common/ToastProvider';
import { BrandingThemeProvider } from './context/BrandingThemeProvider';

const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrandingThemeProvider>
        <ToastProvider>
          <App />
        </ToastProvider>
      </BrandingThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
// force rebuild 2025-10-22 10:26:27
