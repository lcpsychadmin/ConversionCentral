import { jsx as _jsx, Fragment as _Fragment } from "react/jsx-runtime";
import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
export const ProtectedRoute = ({ children, roles }) => {
    const { user, loading, hasRole } = useAuth();
    const location = useLocation();
    if (loading) {
        return null;
    }
    if (!user) {
        return _jsx(Navigate, { to: "/login", state: { from: location }, replace: true });
    }
    if (roles && !hasRole(...roles)) {
        return _jsx(Navigate, { to: "/", replace: true });
    }
    return _jsx(_Fragment, { children: children });
};
