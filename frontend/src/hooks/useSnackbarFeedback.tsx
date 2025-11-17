import { Alert, Snackbar } from '@mui/material';
import type { AlertColor } from '@mui/material';
import { useCallback, useMemo, useState } from 'react';

interface SnackbarFeedbackOptions {
  autoHideDuration?: number;
}

interface SnackbarFeedbackControls {
  show: (message: string, severity?: AlertColor) => void;
  showSuccess: (message: string) => void;
  showError: (message: string) => void;
  close: () => void;
  snackbar: JSX.Element | null;
}

const useSnackbarFeedback = (options: SnackbarFeedbackOptions = {}): SnackbarFeedbackControls => {
  const { autoHideDuration = 4000 } = options;
  const [open, setOpen] = useState(false);
  const [message, setMessage] = useState('');
  const [severity, setSeverity] = useState<AlertColor>('success');

  const show = useCallback((nextMessage: string, nextSeverity: AlertColor = 'success') => {
    setMessage(nextMessage);
    setSeverity(nextSeverity);
    setOpen(true);
  }, []);

  const showSuccess = useCallback((nextMessage: string) => show(nextMessage, 'success'), [show]);
  const showError = useCallback((nextMessage: string) => show(nextMessage, 'error'), [show]);

  const close = useCallback(() => setOpen(false), []);

  const snackbar = useMemo(
    () => (
      <Snackbar
        open={open}
        autoHideDuration={autoHideDuration}
        onClose={(_, reason) => {
          if (reason === 'clickaway') {
            return;
          }
          close();
        }}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity={severity} onClose={close} sx={{ width: '100%' }}>
          {message}
        </Alert>
      </Snackbar>
    ),
    [autoHideDuration, close, message, open, severity]
  );

  return {
    show,
    showSuccess,
    showError,
    close,
    snackbar
  };
};

export default useSnackbarFeedback;
