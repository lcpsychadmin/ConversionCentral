import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Alert, Snackbar } from '@mui/material';
import { createContext, useCallback, useMemo, useState } from 'react';
export const ToastContext = createContext(undefined);
const DEFAULT_STATE = {
    open: false,
    message: '',
    severity: 'info'
};
export const ToastProvider = ({ children }) => {
    const [toast, setToast] = useState(DEFAULT_STATE);
    const handleClose = useCallback(() => {
        setToast((prev) => ({ ...prev, open: false }));
    }, []);
    const showToast = useCallback((message, severity = 'info') => {
        setToast({ open: true, message, severity });
    }, []);
    const value = useMemo(() => ({
        showToast,
        showSuccess: (message) => showToast(message, 'success'),
        showError: (message) => showToast(message, 'error'),
        showInfo: (message) => showToast(message, 'info')
    }), [showToast]);
    return (_jsxs(ToastContext.Provider, { value: value, children: [children, _jsx(Snackbar, { open: toast.open, autoHideDuration: 4000, onClose: handleClose, anchorOrigin: { vertical: 'bottom', horizontal: 'right' }, children: _jsx(Alert, { onClose: handleClose, severity: toast.severity, sx: { width: '100%' }, children: toast.message }) })] }));
};
