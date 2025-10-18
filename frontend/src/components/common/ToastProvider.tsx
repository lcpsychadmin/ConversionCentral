import { Alert, AlertColor, Snackbar } from '@mui/material';
import { createContext, ReactNode, useCallback, useMemo, useState } from 'react';

interface ToastContextValue {
  showToast: (message: string, severity?: AlertColor) => void;
  showSuccess: (message: string) => void;
  showError: (message: string) => void;
  showInfo: (message: string) => void;
}

interface ToastState {
  open: boolean;
  message: string;
  severity: AlertColor;
}

export const ToastContext = createContext<ToastContextValue | undefined>(undefined);

const DEFAULT_STATE: ToastState = {
  open: false,
  message: '',
  severity: 'info'
};

interface ToastProviderProps {
  children: ReactNode;
}

export const ToastProvider = ({ children }: ToastProviderProps) => {
  const [toast, setToast] = useState<ToastState>(DEFAULT_STATE);

  const handleClose = useCallback(() => {
    setToast((prev) => ({ ...prev, open: false }));
  }, []);

  const showToast = useCallback((message: string, severity: AlertColor = 'info') => {
    setToast({ open: true, message, severity });
  }, []);

  const value = useMemo<ToastContextValue>(
    () => ({
      showToast,
      showSuccess: (message: string) => showToast(message, 'success'),
      showError: (message: string) => showToast(message, 'error'),
      showInfo: (message: string) => showToast(message, 'info')
    }),
    [showToast]
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      <Snackbar
        open={toast.open}
        autoHideDuration={4000}
        onClose={handleClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={handleClose} severity={toast.severity} sx={{ width: '100%' }}>
          {toast.message}
        </Alert>
      </Snackbar>
    </ToastContext.Provider>
  );
};
